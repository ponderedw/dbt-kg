"""GraphRAG: store embeddings directly on dbt_graph nodes and query via KNN."""
import os
import logging
from typing import Optional, List

from falkordb import FalkorDB
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.embeddings import Embeddings
from pydantic import Field

logger = logging.getLogger(__name__)

EMBEDDING_DIM = 1536  # titan-embed-text-v1 and text-embedding-3-small are both 1536
GRAPH_NAME = "dbt_graph"
EMBEDDABLE_TYPES = {
    "model": "Model",
    "source": "Source",
    "seed": "Seed",
    "snapshot": "Snapshot",
}

_SPLIT_EMBEDDINGS = os.getenv("SPLIT_EMBEDDINGS", "false").lower() == "true"
_CHUNK_SIZE = 6_000   # chars — safely under Titan's 8192 token limit
_CHUNK_OVERLAP = 200  # chars of overlap between consecutive chunks
CHUNK_GRAPH_NAME = "dbt_graph_chunks"


def _split_text(text: str) -> list[str]:
    from langchain_text_splitters import RecursiveCharacterTextSplitter
    splitter = RecursiveCharacterTextSplitter(chunk_size=_CHUNK_SIZE, chunk_overlap=_CHUNK_OVERLAP)
    return splitter.split_text(text)


def _embedder() -> Embeddings:
    """Return the embedding model that matches the configured LLM provider."""
    model_type, _ = os.environ["LLM_MODEL_ID"].split(":", 1)
    if model_type == "bedrock":
        from langchain_aws import BedrockEmbeddings
        return BedrockEmbeddings(model_id="amazon.titan-embed-text-v1")
    elif model_type == "openai":
        from langchain_openai import OpenAIEmbeddings
        return OpenAIEmbeddings(model="text-embedding-3-small")
    elif model_type == "antropic":
        # Anthropic has no embeddings API; use Bedrock Titan as fallback
        from langchain_aws import BedrockEmbeddings
        return BedrockEmbeddings(model_id="amazon.titan-embed-text-v1")
    else:
        raise ValueError(f"No embedding model available for provider: {model_type}")


def _node_text(node_data: dict, catalog_nodes: dict) -> str:
    """Build a single text blob to embed for one node."""
    name = node_data.get("name", "")
    resource_type = node_data.get("resource_type", "")
    description = (node_data.get("description") or "").strip()
    schema = node_data.get("schema", "")
    materialized = node_data.get("config", {}).get("materialized", "")

    lines = [f"{resource_type}: {name}"]
    if schema:
        lines.append(f"Schema: {schema}")
    if materialized:
        lines.append(f"Materialization: {materialized}")
    if description:
        lines.append(f"Description: {description}")

    # Merge manifest + catalog columns
    node_id = node_data.get("unique_id", "")
    columns = dict(node_data.get("columns") or {})
    for col_name, col_data in (catalog_nodes.get(node_id, {}).get("columns") or {}).items():
        columns.setdefault(col_name, col_data)

    col_lines = []
    for col_name, col_data in columns.items():
        col_desc = (col_data.get("description") or col_data.get("comment") or "").strip()
        entry = f"  - {col_name}"
        if col_desc:
            entry += f": {col_desc}"
        col_lines.append(entry)
    if col_lines:
        lines.append("Columns:\n" + "\n".join(col_lines))

    return "\n".join(lines)


def _get_changed_node_ids(old_manifest: dict, new_manifest: dict) -> set:
    """Return unique_ids of nodes that are new or whose embeddable text changed."""
    def _collect(manifest: dict) -> dict:
        nodes = {}
        for node_id, node_data in manifest.get("nodes", {}).items():
            if node_data.get("resource_type", "") in EMBEDDABLE_TYPES:
                nodes[node_id] = node_data
        for source_id, source_data in manifest.get("sources", {}).items():
            nodes[source_id] = source_data
        return nodes

    old_nodes = _collect(old_manifest)
    new_nodes = _collect(new_manifest)

    changed = set()
    for uid, node_data in new_nodes.items():
        if uid not in old_nodes or _node_text(node_data, {}) != _node_text(old_nodes[uid], {}):
            changed.add(uid)
    return changed


def build_node_embeddings(
    manifest_data: dict,
    catalog_data: dict,
    host: str = "falkordb",
    port: int = 6379,
    username: Optional[str] = None,
    password: Optional[str] = None,
    node_ids: Optional[set] = None,
) -> None:
    """Compute embeddings and store them as an `embedding` property on existing
    Model / Source / Seed / Snapshot nodes in dbt_graph.

    Also creates a vector index on each label so KNN queries are efficient.
    Safe to call repeatedly – index creation is idempotent.

    node_ids: if provided, only re-embed those specific unique_ids (incremental mode).
    """
    db = FalkorDB(host=host, port=port, username=username, password=password)
    graph = db.select_graph(GRAPH_NAME)
    catalog_nodes = catalog_data.get("nodes", {})

    # Collect nodes to embed: unique_id -> (node_data, label)
    to_embed: dict[str, tuple[dict, str]] = {}
    for node_id, node_data in manifest_data.get("nodes", {}).items():
        rt = node_data.get("resource_type", "")
        if rt in EMBEDDABLE_TYPES:
            to_embed[node_id] = (node_data, EMBEDDABLE_TYPES[rt])
    for source_id, source_data in manifest_data.get("sources", {}).items():
        to_embed[source_id] = (source_data, "Source")

    if node_ids is not None:
        to_embed = {uid: v for uid, v in to_embed.items() if uid in node_ids}
        logger.info("Incremental mode: %d/%d nodes to re-embed", len(to_embed), len(node_ids))

    if not to_embed:
        logger.warning("No embeddable nodes found in manifest")
        db.close()
        return

    embedder = _embedder()

    if _SPLIT_EMBEDDINGS:
        # ── Split mode: Chunk nodes stored in a separate graph ──
        chunk_graph = db.select_graph(CHUNK_GRAPH_NAME)
        try:
            chunk_graph.query(
                f"CREATE VECTOR INDEX FOR (n:Chunk) ON (n.embedding) "
                f"OPTIONS {{dimension: {EMBEDDING_DIM}, similarityFunction: 'cosine'}}"
            )
            logger.info("Created vector index on %s Chunk.embedding", CHUNK_GRAPH_NAME)
        except Exception as e:
            logger.debug("Chunk vector index already exists or failed: %s", e)

        updated = 0
        for uid, (node_data, label) in to_embed.items():
            text = _node_text(node_data, catalog_nodes)
            chunks = _split_text(text)

            try:
                chunk_graph.query(
                    "MATCH (c:Chunk {parent_id: $uid}) DETACH DELETE c",
                    {"uid": uid},
                )
            except Exception as e:
                logger.warning("Could not delete old chunks for %s: %s", uid, e)

            node_attrs = {
                "name": node_data.get("name", ""),
                "description": (node_data.get("description") or "").strip(),
                "resource_type": node_data.get("resource_type", ""),
                "schema": node_data.get("schema", ""),
                "materialized": node_data.get("config", {}).get("materialized", ""),
                "parent_label": label,
            }
            vectors = embedder.embed_documents(chunks)
            for i, (chunk_text, vec) in enumerate(zip(chunks, vectors)):
                chunk_id = f"{uid}__chunk_{i}"
                try:
                    chunk_graph.query(
                        "CREATE (:Chunk {unique_id: $chunk_id, parent_id: $uid, "
                        "text: $text, embedding: vecf32($vec), "
                        "name: $name, description: $description, resource_type: $resource_type, "
                        "schema: $schema, materialized: $materialized, parent_label: $parent_label})",
                        {"chunk_id": chunk_id, "uid": uid, "text": chunk_text, "vec": vec,
                         **node_attrs},
                    )
                    updated += 1
                except Exception as e:
                    logger.error("Error storing chunk %s: %s", chunk_id, e)

        logger.info("Stored %d chunks across %d nodes in %s", updated, len(to_embed), CHUNK_GRAPH_NAME)

    else:
        # ── Default mode: single embedding stored on the node itself ──
        for label in {label for _, label in to_embed.values()}:
            try:
                graph.query(
                    f"CREATE VECTOR INDEX FOR (n:{label}) ON (n.embedding) "
                    f"OPTIONS {{dimension: {EMBEDDING_DIM}, similarityFunction: 'cosine'}}"
                )
                logger.info("Created vector index on %s.embedding", label)
            except Exception as e:
                logger.debug("Vector index on %s already exists or failed: %s", label, e)

        ids = list(to_embed.keys())
        texts = [_node_text(to_embed[uid][0], catalog_nodes) for uid in ids]
        logger.info("Computing embeddings for %d nodes…", len(texts))
        vectors = embedder.embed_documents(texts)

        updated = 0
        for uid, vec in zip(ids, vectors):
            _, label = to_embed[uid]
            try:
                graph.query(
                    f"MATCH (n:{label}) WHERE n.unique_id = $uid SET n.embedding = vecf32($vec)",
                    {"uid": uid, "vec": vec},
                )
                updated += 1
            except Exception as e:
                logger.error("Error storing embedding for %s: %s", uid, e)

        logger.info("Stored embeddings on %d/%d nodes", updated, len(ids))


_FULLTEXT_LABELS = ["Model", "Source", "Seed", "Snapshot"]
_FULLTEXT_PROPERTIES = ["name", "description", "schema", "alias", "materialized", "resource_type"]


def build_fulltext_index(
    host: str = "falkordb",
    port: int = 6379,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> None:
    """Create full-text indexes on key string properties of every embeddable label.

    Indexes: name, description, schema, alias, materialized, resource_type.
    Idempotent — safe to call on every upload.
    """
    db = FalkorDB(host=host, port=port, username=username, password=password)
    graph = db.select_graph(GRAPH_NAME)

    props = ", ".join(f"n.{p}" for p in _FULLTEXT_PROPERTIES)
    for label in _FULLTEXT_LABELS:
        try:
            graph.query(f"CREATE FULLTEXT INDEX FOR (n:{label}) ON ({props})")
            logger.info("Created fulltext index on %s (%s)", label, ", ".join(_FULLTEXT_PROPERTIES))
        except Exception as e:
            logger.debug("Fulltext index on %s already exists or failed: %s", label, e)


class FalkorDBFulltextRetriever(BaseRetriever):
    """Retrieve dbt graph nodes by full-text search over their description property."""

    host: str = "falkordb"
    port: int = 6379
    username: Optional[str] = None
    password: Optional[str] = None
    k: int = 5
    labels: List[str] = Field(default=_FULLTEXT_LABELS)

    def _get_relevant_documents(
        self,
        query: str,
        *,
        run_manager: CallbackManagerForRetrieverRun,
    ) -> List[Document]:
        db = FalkorDB(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
        )
        graph = db.select_graph(GRAPH_NAME)

        docs: list[Document] = []
        seen: set[str] = set()

        # FalkorDB fulltext uses Redisearch syntax. Multi-word queries default to AND
        # which is too strict for short descriptions. Convert to OR so we get results
        # and let the score ranking surface the best matches.
        if ' ' in query and '|' not in query and '"' not in query:
            search_query = ' | '.join(query.split())
        else:
            search_query = query

        for label in self.labels:
            try:
                result = graph.query(
                    f"CALL db.idx.fulltext.queryNodes('{label}', $query) "
                    f"YIELD node, score "
                    f"RETURN node.name AS name, node.description AS description, "
                    f"node.unique_id AS unique_id, node.resource_type AS resource_type, score",
                    {"query": search_query},
                )
                for row in result.result_set:
                    uid = row[2]
                    if uid in seen:
                        continue
                    seen.add(uid)
                    name, description, _, resource_type, score = row
                    docs.append(Document(
                        page_content=f"[fulltext:{round(score, 4)}] {resource_type}: {name}\n{description or ''}",
                        metadata={
                            "unique_id": uid,
                            "name": name,
                            "resource_type": resource_type,
                            "score": score,
                        },
                    ))
            except Exception as e:
                logger.warning("Fulltext query failed for label %s: %s", label, e)

        docs.sort(key=lambda d: d.metadata.get("score", 0), reverse=True)
        return docs[: self.k]


def _rerank(query: str, docs: list[Document], top_n: int) -> tuple[list[Document], bool]:
    """Rerank documents with Amazon Bedrock Rerank 1.0.

    Returns (reranked_docs, was_reranked). Falls back to original order on any
    error or when the provider is not Bedrock.
    """
    model_type = os.environ.get("LLM_MODEL_ID", "").split(":")[0]
    if model_type != "bedrock" or not docs:
        return docs[:top_n], False

    try:
        import boto3
        session = boto3.session.Session()
        region = session.region_name or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        client = boto3.client("bedrock-agent-runtime", region_name=region)

        # Allow full ARN override; otherwise build from model ID
        model_arn = os.environ.get("BEDROCK_RERANKER_MODEL_ARN")
        if not model_arn:
            model_id = os.environ.get("BEDROCK_RERANKER_MODEL_ID", "cohere.rerank-v3-5:0")
            model_arn = f"arn:aws:bedrock:{region}::foundation-model/{model_id}"

        response = client.rerank(
            rerankingConfiguration={
                "type": "BEDROCK_RERANKING_MODEL",
                "bedrockRerankingConfiguration": {
                    "modelConfiguration": {
                        "modelArn": model_arn,
                    },
                    "numberOfResults": min(top_n, len(docs)),
                },
            },
            sources=[
                {
                    "type": "INLINE",
                    "inlineDocumentSource": {
                        "type": "TEXT",
                        "textDocument": {"text": doc.page_content},
                    },
                }
                for doc in docs
            ],
            queries=[{"type": "TEXT", "textQuery": {"text": query}}],
        )

        # Bedrock Rerank response key depends on the model:
        # Amazon models use "rerankingResults"; Cohere models use "results"
        reranking_results = response.get("results") or response.get("rerankingResults") or []
        logger.info("Rerank API returned %d results (model_arn=%s)", len(reranking_results), model_arn)
        if not reranking_results:
            logger.warning("Rerank API succeeded but returned no results — check model ARN or quota")
            return docs[:top_n], False

        reranked: list[Document] = []
        for result in reranking_results:
            idx = result.get("index")
            score = result.get("relevanceScore")
            logger.debug("  rerank result index=%s score=%s", idx, score)
            if idx is None or idx >= len(docs):
                logger.warning("Rerank result index %s out of range (docs=%d)", idx, len(docs))
                continue
            doc = docs[idx]
            doc.metadata["rerank_score"] = round(score, 4) if score is not None else 0.0
            reranked.append(doc)

        logger.info("Reranked %d/%d docs", len(reranked), len(docs))
        if not reranked:
            return docs[:top_n], False
        return reranked, True

    except Exception as e:
        logger.warning("Reranking failed, using KNN order: %s", e)
        return docs[:top_n], False


class FalkorDBNodeRetriever(BaseRetriever):
    """Retrieve dbt graph nodes by semantic similarity + optional Bedrock reranking.

    Queries the `embedding` property stored on Model / Source / Seed /
    Snapshot nodes directly in dbt_graph using FalkorDB's vector KNN index,
    then reranks with Amazon Rerank 1.0 when running on Bedrock.
    """

    host: str = "falkordb"
    port: int = 6379
    username: Optional[str] = None
    password: Optional[str] = None
    k: int = 5
    labels: List[str] = Field(default=["Model", "Source", "Seed", "Snapshot"])

    def _get_relevant_documents(
        self,
        query: str,
        *,
        run_manager: CallbackManagerForRetrieverRun,
    ) -> List[Document]:
        query_vec = _embedder().embed_query(query)

        db = FalkorDB(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
        )
        graph = db.select_graph(GRAPH_NAME)

        docs: list[Document] = []
        seen: set[str] = set()

        if _SPLIT_EMBEDDINGS:
            try:
                chunk_graph = db.select_graph(CHUNK_GRAPH_NAME)
                # Fetch more chunks than k so deduplication still yields k unique parents
                chunk_result = chunk_graph.query(
                    "CALL db.idx.vector.queryNodes('Chunk', 'embedding', $k, vecf32($vec)) "
                    "YIELD node AS chunk, score "
                    "RETURN chunk.parent_id, chunk.name, chunk.description, "
                    "chunk.resource_type, chunk.schema, chunk.materialized, chunk.text, score",
                    {"k": self.k * 3, "vec": query_vec},
                )
                # Deduplicate by parent_id, keeping best-scoring chunk's data
                parent_hits: dict[str, tuple] = {}
                for row in chunk_result.result_set:
                    parent_id, name, description, resource_type, schema, materialized, text, score = row
                    if parent_id not in parent_hits or score > parent_hits[parent_id][7]:
                        parent_hits[parent_id] = (parent_id, name, description, resource_type, schema, materialized, text, score)

                for hit in list(parent_hits.values())[:self.k]:
                    parent_id, name, description, resource_type, schema, materialized, text, score = hit
                    if parent_id in seen:
                        continue
                    seen.add(parent_id)
                    docs.append(Document(
                        page_content=text or f"{resource_type}: {name}\n{description or ''}",
                        metadata={
                            "unique_id": parent_id,
                            "name": name,
                            "resource_type": resource_type,
                            "schema": schema,
                            "materialized": materialized,
                            "score": score,
                        },
                    ))
            except Exception as e:
                logger.debug("Chunk vector KNN query failed: %s", e)
        else:
            for label in self.labels:
                try:
                    result = graph.query(
                        f"CALL db.idx.vector.queryNodes('{label}', 'embedding', $k, vecf32($vec)) "
                        f"YIELD node, score "
                        f"RETURN node.name AS name, node.description AS description, "
                        f"node.unique_id AS unique_id, node.resource_type AS resource_type, score",
                        {"k": self.k, "vec": query_vec},
                    )
                    for row in result.result_set:
                        uid = row[2]
                        if uid in seen:
                            continue
                        seen.add(uid)
                        name, description, _, resource_type, score = row
                        docs.append(Document(
                            page_content=f"{resource_type}: {name}\n{description or ''}",
                            metadata={
                                "unique_id": uid,
                                "name": name,
                                "resource_type": resource_type,
                                "score": score,
                            },
                        ))
                except Exception as e:
                    logger.debug("Vector KNN query skipped for label %s: %s", label, e)

        docs.sort(key=lambda d: d.metadata.get("score", 0), reverse=True)

        # Rerank with Amazon Rerank 1.0 if on Bedrock (passes all candidates)
        reranked_docs, was_reranked = _rerank(query, docs, self.k)

        if was_reranked:
            # Show all KNN candidates so the user can see what the reranker chose from.
            knn_lines = [
                f"[similarity:{d.metadata.get('score', '')}] {d.page_content}"
                for d in docs
            ]
            reranked_lines = [
                f"[rerank:{d.metadata.get('rerank_score', '')}] {d.page_content}"
                for d in reranked_docs
            ]
            combined = (
                f"===KNN({len(docs)})===\n" + "\n".join(knn_lines) +
                "\n===RERANKED===\n" + "\n".join(reranked_lines)
            )
            return [Document(page_content=combined)]

        # No reranking: return top-k by similarity score
        for doc in docs[:self.k]:
            doc.page_content = f"[similarity:{doc.metadata.get('score', '')}] {doc.page_content}"
        return docs[:self.k]
