from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))


logger = logging.getLogger(__name__)


REQUIRED_ARTIFACTS = (
    "graph_chunk_entity_relation.graphml",
    "kv_store_full_docs.json",
    "kv_store_text_chunks.json",
    "kv_store_doc_status.json",
    "kv_store_full_entities.json",
    "kv_store_full_relations.json",
    "kv_store_entity_chunks.json",
    "kv_store_relation_chunks.json",
    "vdb_chunks.json",
    "vdb_entities.json",
    "vdb_relationships.json",
)


@dataclass(frozen=True, slots=True)
class PublishStorageConfig:
    project_root: Path
    index_dir: Path
    embedding_model_name: str
    verify_after_publish: bool = True
    batch_size_kv: int = 200
    batch_size_graph: int = 200
    batch_size_vector: int = 100


def load_env_file(env_path: Path) -> None:
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


def build_publish_config(
    *,
    project_root: Path,
    index_dir: Path,
    embedding_model_name: str,
    verify_after_publish: bool = True,
    batch_size_kv: int = 200,
    batch_size_graph: int = 200,
    batch_size_vector: int = 100,
) -> PublishStorageConfig:
    return PublishStorageConfig(
        project_root=project_root,
        index_dir=index_dir,
        embedding_model_name=embedding_model_name,
        verify_after_publish=verify_after_publish,
        batch_size_kv=batch_size_kv,
        batch_size_graph=batch_size_graph,
        batch_size_vector=batch_size_vector,
    )


def build_publish_config_from_runtime(runtime: Any) -> PublishStorageConfig:
    return build_publish_config(
        project_root=Path(runtime.project_root),
        index_dir=Path(runtime.output_dir),
        embedding_model_name=str(runtime.embedding_model_name),
        verify_after_publish=bool(runtime.publish_verify_after_publish),
        batch_size_kv=int(runtime.publish_batch_size_kv),
        batch_size_graph=int(runtime.publish_batch_size_graph),
        batch_size_vector=int(runtime.publish_batch_size_vector),
    )


def _require_env(var_name: str) -> str:
    value = os.getenv(var_name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {var_name}")
    return value


def _artifact_path(config: PublishStorageConfig, file_name: str) -> Path:
    return config.index_dir / file_name


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RuntimeError(f"Missing required file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON file: {path}") from exc


def _chunked_items(data: dict[str, Any], batch_size: int):
    items = list(data.items())
    for index in range(0, len(items), batch_size):
        yield dict(items[index : index + batch_size])


def ensure_required_artifacts(config: PublishStorageConfig) -> None:
    missing = [
        str(_artifact_path(config, file_name))
        for file_name in REQUIRED_ARTIFACTS
        if not _artifact_path(config, file_name).exists()
    ]
    if missing:
        raise RuntimeError(f"Missing publish artifacts: {missing}")


async def _dummy_llm(*_args, **_kwargs) -> str:
    return ""


def _build_dummy_embedding_func(embedding_dim: int, model_name: str) -> Any:
    lightrag_utils = importlib.import_module("lightrag.utils")
    EmbeddingFunc = getattr(lightrag_utils, "EmbeddingFunc")

    async def _embed(texts: list[str], **_kwargs) -> np.ndarray:
        return np.zeros((len(texts), embedding_dim), dtype=np.float32)

    return EmbeddingFunc(
        embedding_dim=embedding_dim,
        func=_embed,
        max_token_size=8192,
        model_name=model_name,
    )


async def _build_rag(config: PublishStorageConfig) -> Any:
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

    vdb_chunks = _load_json(_artifact_path(config, "vdb_chunks.json"))
    embedding_dim = int(vdb_chunks.get("embedding_dim", 0) or 0)
    if embedding_dim < 1:
        raise RuntimeError("Invalid embedding_dim in vdb_chunks.json")

    rag = LightRAG(
        working_dir=str(config.index_dir),
        workspace=os.getenv("WORKSPACE", "viet_contract_prod"),
        kv_storage="PGKVStorage",
        doc_status_storage="PGDocStatusStorage",
        graph_storage="Neo4JStorage",
        vector_storage="QdrantVectorDBStorage",
        llm_model_func=_dummy_llm,
        embedding_func=_build_dummy_embedding_func(embedding_dim, config.embedding_model_name),
    )
    await rag.initialize_storages()
    return rag


async def _upsert_kv_store(
    storage: Any,
    payload: dict[str, dict[str, Any]],
    *,
    batch_size: int,
    label: str,
) -> None:
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


async def _import_graph(rag: Any, config: PublishStorageConfig) -> dict[str, int]:
    nx = importlib.import_module("networkx")
    graph = nx.read_graphml(_artifact_path(config, "graph_chunk_entity_relation.graphml"))

    node_count = graph.number_of_nodes()
    edge_count = graph.number_of_edges()
    logger.info("Graph import start: %s nodes, %s edges", node_count, edge_count)

    processed_nodes = 0
    for node_id, node_data in graph.nodes(data=True):
        await rag.chunk_entity_relation_graph.upsert_node(str(node_id), dict(node_data))
        processed_nodes += 1
        if processed_nodes % config.batch_size_graph == 0:
            await rag.chunk_entity_relation_graph.index_done_callback()
            logger.info("graph.nodes: %s/%s", processed_nodes, node_count)

    processed_edges = 0
    for src, tgt, edge_data in graph.edges(data=True):
        await rag.chunk_entity_relation_graph.upsert_edge(str(src), str(tgt), dict(edge_data))
        processed_edges += 1
        if processed_edges % config.batch_size_graph == 0:
            await rag.chunk_entity_relation_graph.index_done_callback()
            logger.info("graph.edges: %s/%s", processed_edges, edge_count)

    await rag.chunk_entity_relation_graph.index_done_callback()
    logger.info("Graph import done: %s nodes, %s edges", processed_nodes, processed_edges)
    return {"nodes": processed_nodes, "edges": processed_edges}


def _build_vector_points(storage: Any, vdb_file: Path) -> list[Any]:
    qdrant_impl = importlib.import_module("lightrag.kg.qdrant_impl")
    compute_mdhash_id_for_qdrant = getattr(qdrant_impl, "compute_mdhash_id_for_qdrant")
    id_field = getattr(qdrant_impl, "ID_FIELD")
    workspace_id_field = getattr(qdrant_impl, "WORKSPACE_ID_FIELD")
    created_at_field = getattr(qdrant_impl, "CREATED_AT_FIELD")

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

    points: list[Any] = []
    for index, item in enumerate(records):
        item_id = str(item.get("__id__", "")).strip()
        if not item_id or matrix is None or index >= len(matrix):
            continue

        payload = {
            id_field: item_id,
            workspace_id_field: storage.effective_workspace,
            created_at_field: int(item.get("__created_at__", 0) or 0),
        }
        for field in storage.meta_fields:
            if field in item:
                payload[field] = item[field]

        points.append(
            PointStruct(
                id=compute_mdhash_id_for_qdrant(item_id, prefix=storage.effective_workspace),
                vector=matrix[index].tolist(),
                payload=payload,
            )
        )
    return points


async def _import_vectors(rag: Any, config: PublishStorageConfig) -> dict[str, int]:
    vector_jobs = [
        (rag.entities_vdb, _artifact_path(config, "vdb_entities.json"), "vectors.entities"),
        (rag.relationships_vdb, _artifact_path(config, "vdb_relationships.json"), "vectors.relationships"),
        (rag.chunks_vdb, _artifact_path(config, "vdb_chunks.json"), "vectors.chunks"),
    ]

    results: dict[str, int] = {}
    for storage, vdb_file, label in vector_jobs:
        points = _build_vector_points(storage, vdb_file)
        total = len(points)
        results[label] = total
        if total == 0:
            logger.warning("Skip %s: no points", label)
            continue

        processed = 0
        for index in range(0, total, config.batch_size_vector):
            batch = points[index : index + config.batch_size_vector]
            storage._client.upsert(
                collection_name=storage.final_namespace,
                points=batch,
                wait=True,
            )
            processed += len(batch)
            logger.info("%s: %s/%s", label, processed, total)
        await storage.index_done_callback()

    return results


def _load_publish_payloads(config: PublishStorageConfig) -> dict[str, dict[str, Any]]:
    return {
        "kv.full_docs": _load_json(_artifact_path(config, "kv_store_full_docs.json")),
        "kv.text_chunks": _load_json(_artifact_path(config, "kv_store_text_chunks.json")),
        "kv.doc_status": _load_json(_artifact_path(config, "kv_store_doc_status.json")),
        "kv.full_entities": _load_json(_artifact_path(config, "kv_store_full_entities.json")),
        "kv.full_relations": _load_json(_artifact_path(config, "kv_store_full_relations.json")),
        "kv.entity_chunks": _load_json(_artifact_path(config, "kv_store_entity_chunks.json")),
        "kv.relation_chunks": _load_json(_artifact_path(config, "kv_store_relation_chunks.json")),
    }


async def verify_storage_after_publish() -> dict[str, Any]:
    storage_check = importlib.import_module("src.check_storage")
    return {
        "postgresql": await storage_check.check_postgresql(),
        "neo4j": await storage_check.check_neo4j(),
        "qdrant": await storage_check.check_qdrant(),
    }


async def publish_index_to_storage(config: PublishStorageConfig) -> dict[str, Any]:
    load_env_file(config.project_root / ".env")
    ensure_required_artifacts(config)

    payloads = _load_publish_payloads(config)
    rag: Any | None = None
    graph_summary: dict[str, int] = {}
    vector_summary: dict[str, int] = {}

    try:
        rag = await _build_rag(config)

        await _upsert_kv_store(
            rag.full_docs,
            payloads["kv.full_docs"],
            batch_size=config.batch_size_kv,
            label="kv.full_docs",
        )
        await _upsert_kv_store(
            rag.text_chunks,
            payloads["kv.text_chunks"],
            batch_size=config.batch_size_kv,
            label="kv.text_chunks",
        )
        await _upsert_kv_store(
            rag.doc_status,
            payloads["kv.doc_status"],
            batch_size=config.batch_size_kv,
            label="kv.doc_status",
        )
        await _upsert_kv_store(
            rag.full_entities,
            payloads["kv.full_entities"],
            batch_size=config.batch_size_kv,
            label="kv.full_entities",
        )
        await _upsert_kv_store(
            rag.full_relations,
            payloads["kv.full_relations"],
            batch_size=config.batch_size_kv,
            label="kv.full_relations",
        )
        await _upsert_kv_store(
            rag.entity_chunks,
            payloads["kv.entity_chunks"],
            batch_size=config.batch_size_kv,
            label="kv.entity_chunks",
        )
        await _upsert_kv_store(
            rag.relation_chunks,
            payloads["kv.relation_chunks"],
            batch_size=config.batch_size_kv,
            label="kv.relation_chunks",
        )

        graph_summary = await _import_graph(rag, config)
        vector_summary = await _import_vectors(rag, config)
        logger.info("Offline storage publish completed successfully.")
    finally:
        if rag is not None:
            await rag.finalize_storages()

    verification = None
    if config.verify_after_publish:
        verification = await verify_storage_after_publish()

    return {
        "index_dir": str(config.index_dir),
        "kv_counts": {label: len(payload) for label, payload in payloads.items()},
        "graph": graph_summary,
        "vectors": vector_summary,
        "verification": verification,
    }


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    project_root = Path(__file__).resolve().parents[3]
    config = build_publish_config(
        project_root=project_root,
        index_dir=project_root / "data" / "index",
        embedding_model_name=os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"),
        verify_after_publish=True,
        batch_size_kv=int(os.getenv("OFFLINE_BATCH_SIZE_KV", "200")),
        batch_size_graph=int(os.getenv("OFFLINE_BATCH_SIZE_GRAPH", "200")),
        batch_size_vector=int(os.getenv("OFFLINE_BATCH_SIZE_VECTOR", "100")),
    )

    try:
        asyncio.run(publish_index_to_storage(config))
    except Exception as exc:
        logger.error("Offline storage publish failed: %s", exc, exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
