"""Offline migration into production LightRAG storages (no LLM API required).

Run:
    uv run python src/init_storage.py

What it does:
    1. Initializes production storages (PGKV + PGDocStatus + Neo4J + Qdrant)
    2. Imports existing artifacts from lightrag_index/ directly
    3. Skips LLM extraction and OpenAI calls entirely
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
INDEX_DIR = ROOT / "lightrag_index"
GRAPHML_PATH = INDEX_DIR / "graph_chunk_entity_relation.graphml"
KV_FULL_DOCS_PATH = INDEX_DIR / "kv_store_full_docs.json"
KV_TEXT_CHUNKS_PATH = INDEX_DIR / "kv_store_text_chunks.json"
KV_DOC_STATUS_PATH = INDEX_DIR / "kv_store_doc_status.json"
KV_OLD_FULL_ENTITIES_PATH = INDEX_DIR / "kv_store_full_entities.json"
KV_OLD_FULL_RELATIONS_PATH = INDEX_DIR / "kv_store_full_relations.json"
KV_OLD_ENTITY_CHUNKS_PATH = INDEX_DIR / "kv_store_entity_chunks.json"
KV_OLD_RELATION_CHUNKS_PATH = INDEX_DIR / "kv_store_relation_chunks.json"
VDB_ENTITIES_PATH = INDEX_DIR / "vdb_entities.json"
VDB_RELATIONSHIPS_PATH = INDEX_DIR / "vdb_relationships.json"
VDB_CHUNKS_PATH = INDEX_DIR / "vdb_chunks.json"

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"

BATCH_SIZE_KV = int(os.getenv("OFFLINE_BATCH_SIZE_KV", "200"))
BATCH_SIZE_GRAPH = int(os.getenv("OFFLINE_BATCH_SIZE_GRAPH", "200"))
BATCH_SIZE_VECTOR = int(os.getenv("OFFLINE_BATCH_SIZE_VECTOR", "100"))


def _require_env(var_name: str) -> str:
    value = os.getenv(var_name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {var_name}")
    return value


def _load_env_file(env_path: Path) -> None:
    """Load KEY=VALUE pairs from .env without overriding existing vars."""
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as exc:
        raise RuntimeError(f"Missing required file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON file: {path}") from exc


def _chunked_items(data: dict[str, Any], batch_size: int):
    items = list(data.items())
    for i in range(0, len(items), batch_size):
        yield dict(items[i : i + batch_size])


def _ensure_required_artifacts() -> None:
    required = [
        GRAPHML_PATH,
        KV_FULL_DOCS_PATH,
        KV_TEXT_CHUNKS_PATH,
        KV_DOC_STATUS_PATH,
        KV_OLD_FULL_ENTITIES_PATH,
        KV_OLD_FULL_RELATIONS_PATH,
        KV_OLD_ENTITY_CHUNKS_PATH,
        KV_OLD_RELATION_CHUNKS_PATH,
        VDB_ENTITIES_PATH,
        VDB_RELATIONSHIPS_PATH,
        VDB_CHUNKS_PATH,
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise RuntimeError(f"Missing migration artifacts: {missing}")


async def _dummy_llm(*_args, **_kwargs) -> str:
    # Offline migration mode never calls generation; this is a safe placeholder.
    return ""


def _build_dummy_embedding_func(embedding_dim: int) -> Any:
    lightrag_utils = importlib.import_module("lightrag.utils")
    EmbeddingFunc = getattr(lightrag_utils, "EmbeddingFunc")

    async def _embed(texts: list[str], **_kwargs) -> np.ndarray:
        return np.zeros((len(texts), embedding_dim), dtype=np.float32)

    return EmbeddingFunc(
        embedding_dim=embedding_dim,
        func=_embed,
        max_token_size=8192,
        model_name="offline-import-embedding",
    )


def _normalize_doc_status(status: str) -> str:
    mapping = {
        "done": "processed",
        "processed": "processed",
        "pending": "pending",
        "processing": "processing",
        "failed": "failed",
    }
    return mapping.get(status.lower(), "processed")


def _build_doc_status_payload(
    full_docs: dict[str, dict[str, Any]],
    text_chunks: dict[str, dict[str, Any]],
    doc_status_raw: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    now_iso = datetime.now(timezone.utc).isoformat()
    doc_to_chunks: dict[str, list[str]] = {}
    for chunk_id, chunk in text_chunks.items():
        doc_id = chunk.get("full_doc_id")
        if isinstance(doc_id, str) and doc_id:
            doc_to_chunks.setdefault(doc_id, []).append(chunk_id)

    payload: dict[str, dict[str, Any]] = {}
    for doc_id, doc in full_docs.items():
        status_entry = doc_status_raw.get(doc_id, {})
        content = str(doc.get("content", ""))
        summary = str(status_entry.get("content") or content[:200])
        file_path = str(doc.get("file_path", "unknown_source") or "unknown_source")
        chunks_list = doc_to_chunks.get(doc_id, [])

        raw_status = str(status_entry.get("status", "done"))
        payload[doc_id] = {
            "status": _normalize_doc_status(raw_status),
            "content_summary": summary,
            "content_length": len(content),
            "chunks_count": len(chunks_list),
            "chunks_list": chunks_list,
            "file_path": file_path,
            "track_id": None,
            "metadata": {},
            "error_msg": None,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
    return payload


def _build_doc_entity_relation_payloads(
    text_chunks: dict[str, dict[str, Any]],
    old_full_entities: dict[str, dict[str, Any]],
    old_full_relations: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    chunk_to_doc: dict[str, str] = {}
    for chunk_id, chunk in text_chunks.items():
        doc_id = chunk.get("full_doc_id")
        if isinstance(doc_id, str) and doc_id:
            chunk_to_doc[chunk_id] = doc_id

    doc_entities: dict[str, set[str]] = {}
    for entity_name, entity_data in old_full_entities.items():
        source_id = str(entity_data.get("source_id", ""))
        for chunk_id in [s for s in source_id.split("<SEP>") if s]:
            doc_id = chunk_to_doc.get(chunk_id)
            if doc_id:
                doc_entities.setdefault(doc_id, set()).add(entity_name)

    doc_relations: dict[str, set[tuple[str, str]]] = {}
    for relation_data in old_full_relations.values():
        src = str(relation_data.get("src_id", "")).strip()
        tgt = str(relation_data.get("tgt_id", "")).strip()
        if not src or not tgt:
            continue
        relation_pair = tuple(sorted((src, tgt)))
        source_id = str(relation_data.get("source_id", ""))
        for chunk_id in [s for s in source_id.split("<SEP>") if s]:
            doc_id = chunk_to_doc.get(chunk_id)
            if doc_id:
                doc_relations.setdefault(doc_id, set()).add(relation_pair)

    full_entities_payload = {
        doc_id: {
            "entity_names": sorted(list(names)),
            "count": len(names),
        }
        for doc_id, names in doc_entities.items()
    }
    full_relations_payload = {
        doc_id: {
            "relation_pairs": [list(pair) for pair in sorted(list(pairs))],
            "count": len(pairs),
        }
        for doc_id, pairs in doc_relations.items()
    }

    return full_entities_payload, full_relations_payload


def _build_entity_chunks_payload(old_entity_chunks: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payload = {}
    for entity_name, data in old_entity_chunks.items():
        chunks = data.get("chunks", [])
        if not isinstance(chunks, list):
            chunks = []
        payload[entity_name] = {
            "chunk_ids": chunks,
            "count": len(chunks),
        }
    return payload


def _build_relation_chunks_payload(
    old_full_relations: dict[str, dict[str, Any]],
    old_relation_chunks: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    make_relation_chunk_key = getattr(importlib.import_module("lightrag.utils"), "make_relation_chunk_key")
    payload: dict[str, dict[str, Any]] = {}
    for rel_id, rel_data in old_relation_chunks.items():
        relation = old_full_relations.get(rel_id)
        if not relation:
            continue
        src = str(relation.get("src_id", "")).strip()
        tgt = str(relation.get("tgt_id", "")).strip()
        if not src or not tgt:
            continue
        chunks = rel_data.get("chunks", [])
        if not isinstance(chunks, list):
            chunks = []
        key = make_relation_chunk_key(src, tgt)
        payload[key] = {
            "chunk_ids": chunks,
            "count": len(chunks),
        }
    return payload


async def _build_rag() -> Any:
    # Validate required env vars based on selected storage adapters.
    _require_env("NEO4J_URI")
    _require_env("NEO4J_USERNAME")
    _require_env("NEO4J_PASSWORD")
    _require_env("QDRANT_URL")
    _require_env("POSTGRES_USER")
    _require_env("POSTGRES_PASSWORD")
    _require_env("POSTGRES_DATABASE")

    try:
        lightrag_module = importlib.import_module("lightrag")
    except ImportError as exc:
        raise RuntimeError(
            "Missing LightRAG dependencies. Install with: uv add lightrag-hku"
        ) from exc

    LightRAG = getattr(lightrag_module, "LightRAG")

    vdb_chunks = _load_json(VDB_CHUNKS_PATH)
    embedding_dim = int(vdb_chunks.get("embedding_dim", 384))
    embedding_func = _build_dummy_embedding_func(embedding_dim)

    rag = LightRAG(
        working_dir=os.getenv("LIGHTRAG_WORKING_DIR", "./lightrag_index"),
        workspace=os.getenv("WORKSPACE", "viet_contract_prod"),
        kv_storage="PGKVStorage",
        doc_status_storage="PGDocStatusStorage",
        graph_storage="Neo4JStorage",
        vector_storage="QdrantVectorDBStorage",
        llm_model_func=_dummy_llm,
        embedding_func=embedding_func,
    )

    await rag.initialize_storages()
    return rag


async def _upsert_kv_store(storage: Any, payload: dict[str, dict[str, Any]], batch_size: int, label: str) -> None:
    logger = logging.getLogger(__name__)
    total = len(payload)
    if total == 0:
        logger.warning("Skip %s: no records", label)
        return

    processed = 0
    for batch in _chunked_items(payload, batch_size):
        await storage.upsert(batch)
        await storage.index_done_callback()
        processed += len(batch)
        logger.info("%s: %s/%s", label, processed, total)


async def _import_graph(rag: Any) -> None:
    logger = logging.getLogger(__name__)
    nx = importlib.import_module("networkx")
    graph = nx.read_graphml(GRAPHML_PATH)

    node_count = graph.number_of_nodes()
    edge_count = graph.number_of_edges()
    logger.info("Graph import start: %s nodes, %s edges", node_count, edge_count)

    processed_nodes = 0
    for node_id, node_data in graph.nodes(data=True):
        await rag.chunk_entity_relation_graph.upsert_node(str(node_id), dict(node_data))
        processed_nodes += 1
        if processed_nodes % BATCH_SIZE_GRAPH == 0:
            await rag.chunk_entity_relation_graph.index_done_callback()
            logger.info("Graph nodes: %s/%s", processed_nodes, node_count)

    processed_edges = 0
    for src, tgt, edge_data in graph.edges(data=True):
        await rag.chunk_entity_relation_graph.upsert_edge(str(src), str(tgt), dict(edge_data))
        processed_edges += 1
        if processed_edges % BATCH_SIZE_GRAPH == 0:
            await rag.chunk_entity_relation_graph.index_done_callback()
            logger.info("Graph edges: %s/%s", processed_edges, edge_count)

    await rag.chunk_entity_relation_graph.index_done_callback()
    logger.info("Graph import done: %s nodes, %s edges", processed_nodes, processed_edges)


def _build_vector_points(storage: Any, vdb_file: Path) -> list[Any]:
    qdrant_impl = importlib.import_module("lightrag.kg.qdrant_impl")
    compute_mdhash_id_for_qdrant = getattr(qdrant_impl, "compute_mdhash_id_for_qdrant")
    ID_FIELD = getattr(qdrant_impl, "ID_FIELD")
    WORKSPACE_ID_FIELD = getattr(qdrant_impl, "WORKSPACE_ID_FIELD")
    CREATED_AT_FIELD = getattr(qdrant_impl, "CREATED_AT_FIELD")

    nano_vectordb = importlib.import_module("nano_vectordb")
    NanoVectorDB = getattr(nano_vectordb, "NanoVectorDB")

    raw_json = _load_json(vdb_file)
    embedding_dim = int(raw_json.get("embedding_dim", storage.embedding_func.embedding_dim))
    db = NanoVectorDB(embedding_dim=embedding_dim, storage_file=str(vdb_file))
    storage_data = getattr(db, "_NanoVectorDB__storage")
    records = storage_data.get("data", [])
    matrix = storage_data.get("matrix")

    qdrant_models = importlib.import_module("qdrant_client.models")
    PointStruct = getattr(qdrant_models, "PointStruct")

    points = []
    for idx, item in enumerate(records):
        item_id = str(item.get("__id__", "")).strip()
        if not item_id:
            continue
        if matrix is None or idx >= len(matrix):
            continue
        vector = matrix[idx]

        payload = {
            ID_FIELD: item_id,
            WORKSPACE_ID_FIELD: storage.effective_workspace,
            CREATED_AT_FIELD: int(item.get("__created_at__", 0) or 0),
        }
        for field in storage.meta_fields:
            if field in item:
                payload[field] = item[field]

        points.append(
            PointStruct(
                id=compute_mdhash_id_for_qdrant(item_id, prefix=storage.effective_workspace),
                vector=vector.tolist(),
                payload=payload,
            )
        )
    return points


async def _import_vectors(rag: Any) -> None:
    logger = logging.getLogger(__name__)
    vector_jobs = [
        (rag.entities_vdb, VDB_ENTITIES_PATH, "vectors.entities"),
        (rag.relationships_vdb, VDB_RELATIONSHIPS_PATH, "vectors.relationships"),
        (rag.chunks_vdb, VDB_CHUNKS_PATH, "vectors.chunks"),
    ]

    for storage, vdb_file, label in vector_jobs:
        points = _build_vector_points(storage, vdb_file)
        total = len(points)
        if total == 0:
            logger.warning("Skip %s: no points", label)
            continue

        processed = 0
        for i in range(0, total, BATCH_SIZE_VECTOR):
            batch = points[i : i + BATCH_SIZE_VECTOR]
            storage._client.upsert(
                collection_name=storage.final_namespace,
                points=batch,
                wait=True,
            )
            processed += len(batch)
            logger.info("%s: %s/%s", label, processed, total)
        await storage.index_done_callback()


async def amain() -> None:
    _load_env_file(ROOT / ".env")
    logger = logging.getLogger(__name__)

    _ensure_required_artifacts()

    full_docs = _load_json(KV_FULL_DOCS_PATH)
    text_chunks = _load_json(KV_TEXT_CHUNKS_PATH)
    doc_status_raw = _load_json(KV_DOC_STATUS_PATH)
    old_full_entities = _load_json(KV_OLD_FULL_ENTITIES_PATH)
    old_full_relations = _load_json(KV_OLD_FULL_RELATIONS_PATH)
    old_entity_chunks = _load_json(KV_OLD_ENTITY_CHUNKS_PATH)
    old_relation_chunks = _load_json(KV_OLD_RELATION_CHUNKS_PATH)

    doc_status_payload = _build_doc_status_payload(full_docs, text_chunks, doc_status_raw)
    full_entities_payload, full_relations_payload = _build_doc_entity_relation_payloads(
        text_chunks,
        old_full_entities,
        old_full_relations,
    )
    entity_chunks_payload = _build_entity_chunks_payload(old_entity_chunks)
    relation_chunks_payload = _build_relation_chunks_payload(old_full_relations, old_relation_chunks)

    rag: Any | None = None
    try:
        rag = await _build_rag()

        # Stage 1: Core KV + doc status
        await _upsert_kv_store(rag.full_docs, full_docs, BATCH_SIZE_KV, "kv.full_docs")
        await _upsert_kv_store(rag.text_chunks, text_chunks, BATCH_SIZE_KV, "kv.text_chunks")
        await _upsert_kv_store(rag.doc_status, doc_status_payload, BATCH_SIZE_KV, "kv.doc_status")

        # Stage 2: Derived doc-level entity/relation stores + chunk tracking
        await _upsert_kv_store(rag.full_entities, full_entities_payload, BATCH_SIZE_KV, "kv.full_entities")
        await _upsert_kv_store(rag.full_relations, full_relations_payload, BATCH_SIZE_KV, "kv.full_relations")
        await _upsert_kv_store(rag.entity_chunks, entity_chunks_payload, BATCH_SIZE_KV, "kv.entity_chunks")
        await _upsert_kv_store(rag.relation_chunks, relation_chunks_payload, BATCH_SIZE_KV, "kv.relation_chunks")

        # Stage 3: Graph import
        await _import_graph(rag)

        # Stage 4: Vector import using precomputed vectors
        await _import_vectors(rag)

        logger.info("Offline storage migration completed successfully (no LLM API calls).")
    finally:
        if rag is not None:
            await rag.finalize_storages()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
    asyncio.run(amain())
