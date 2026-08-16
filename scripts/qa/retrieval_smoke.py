from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import textwrap
from pathlib import Path
from typing import Any

import asyncpg
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.http import models


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_QUERIES = [
    "quyen va nghia vu cua co dong pho thong trong cong ty co phan",
    "tai san chung cua vo chong duoc quy dinh nhu the nao",
    "truong hop nao thoa thuan trong tai bi vo hieu",
]


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


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def collection_suffix(model_name: str, embedding_dim: int) -> str:
    safe_model_name = re.sub(r"[^a-zA-Z0-9_]", "_", model_name.lower())
    return f"{safe_model_name}_{embedding_dim}d"


def resolve_chunks_collection(
    client: QdrantClient,
    *,
    model_name: str,
    embedding_dim: int,
    explicit_collection: str | None,
) -> str:
    if explicit_collection:
        return explicit_collection

    expected_name = f"lightrag_vdb_chunks_{collection_suffix(model_name, embedding_dim)}"
    existing_names = {collection.name for collection in client.get_collections().collections}
    if expected_name in existing_names:
        return expected_name

    candidates = sorted(name for name in existing_names if name.startswith("lightrag_vdb_chunks"))
    if not candidates:
        raise RuntimeError("No Qdrant chunks collection found")

    raise RuntimeError(
        "Expected Qdrant collection not found: "
        f"{expected_name}. Existing chunks collections: {', '.join(candidates)}"
    )


def embed_query(query: str, *, model_name: str, api_key: str) -> list[float]:
    client = OpenAI(api_key=api_key)
    response = client.embeddings.create(model=model_name, input=query)
    return response.data[0].embedding


def search_qdrant(
    client: QdrantClient,
    *,
    collection_name: str,
    query_vector: list[float],
    workspace: str,
    top_k: int,
) -> list[Any]:
    query_filter = models.Filter(
        must=[
            models.FieldCondition(
                key="workspace_id",
                match=models.MatchValue(value=workspace),
            )
        ]
    )
    response = client.query_points(
        collection_name=collection_name,
        query=query_vector,
        query_filter=query_filter,
        limit=top_k,
        with_payload=True,
        with_vectors=False,
    )
    return list(response.points)


async def fetch_chunk_texts(chunk_ids: list[str], workspace: str) -> dict[str, dict[str, Any]]:
    if not chunk_ids:
        return {}

    conn = await asyncpg.connect(
        host=require_env("POSTGRES_HOST"),
        port=int(require_env("POSTGRES_PORT")),
        user=require_env("POSTGRES_USER"),
        password=require_env("POSTGRES_PASSWORD"),
        database=require_env("POSTGRES_DATABASE"),
    )
    try:
        rows = await conn.fetch(
            """
            SELECT id, content, file_path, full_doc_id, tokens
            FROM LIGHTRAG_DOC_CHUNKS
            WHERE workspace = $1 AND id = ANY($2::text[])
            """,
            workspace,
            chunk_ids,
        )
    finally:
        await conn.close()

    return {row["id"]: dict(row) for row in rows}


def preview_text(text: str, max_chars: int) -> str:
    normalized = " ".join(text.split())
    return textwrap.shorten(normalized, width=max_chars, placeholder="...")


def chunk_id_from_point(point: Any) -> str:
    payload = point.payload or {}
    return str(payload.get("id") or "").strip()


async def run_smoke(args: argparse.Namespace) -> None:
    load_env_file(ROOT / ".env")

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    embedding_model = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small").strip()
    embedding_dim = int(os.getenv("OPENAI_EMBEDDING_DIM", "1536"))
    workspace = os.getenv("QDRANT_WORKSPACE", os.getenv("WORKSPACE", "viet_contract_prod")).strip()

    qdrant = QdrantClient(
        url=require_env("QDRANT_URL"),
        api_key=os.getenv("QDRANT_API_KEY") or None,
        timeout=20.0,
    )
    collection_name = resolve_chunks_collection(
        qdrant,
        model_name=embedding_model,
        embedding_dim=embedding_dim,
        explicit_collection=args.collection,
    )

    print(f"Collection: {collection_name}")
    print(f"Workspace: {workspace}")
    print(f"Embedding: {embedding_model} ({embedding_dim}d)")

    for query_index, query in enumerate(args.queries, start=1):
        print("\n" + "=" * 90)
        print(f"Query {query_index}: {query}")

        query_vector = embed_query(
            query,
            model_name=embedding_model,
            api_key=require_env("OPENAI_API_KEY"),
        )
        points = search_qdrant(
            qdrant,
            collection_name=collection_name,
            query_vector=query_vector,
            workspace=workspace,
            top_k=args.top_k,
        )
        chunk_ids = [chunk_id_from_point(point) for point in points]
        chunk_rows = await fetch_chunk_texts(chunk_ids, workspace)

        for rank, point in enumerate(points, start=1):
            chunk_id = chunk_id_from_point(point)
            payload = point.payload or {}
            row = chunk_rows.get(chunk_id, {})
            content = str(row.get("content") or payload.get("content") or "")
            file_path = str(row.get("file_path") or payload.get("file_path") or "unknown_source")
            full_doc_id = str(row.get("full_doc_id") or payload.get("full_doc_id") or "")

            print(f"\n#{rank} score={point.score:.4f} chunk_id={chunk_id}")
            print(f"source={file_path} full_doc_id={full_doc_id}")
            print(preview_text(content, args.preview_chars))

    qdrant.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke-test retrieval from Qdrant chunks and PostgreSQL text chunks."
    )
    parser.add_argument(
        "--query",
        dest="queries",
        action="append",
        help="Query to run. Can be passed multiple times. Defaults to 3 legal smoke queries.",
    )
    parser.add_argument("--top-k", type=int, default=3, help="Number of chunks to print per query.")
    parser.add_argument(
        "--preview-chars",
        type=int,
        default=520,
        help="Maximum characters to print for each chunk preview.",
    )
    parser.add_argument(
        "--collection",
        help="Override Qdrant collection name. Defaults to the published chunks collection.",
    )
    args = parser.parse_args()
    if not args.queries:
        args.queries = DEFAULT_QUERIES
    return args


def main() -> int:
    try:
        asyncio.run(run_smoke(parse_args()))
    except Exception as exc:
        print(f"Retrieval smoke failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
