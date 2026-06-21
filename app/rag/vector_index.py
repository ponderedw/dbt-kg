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


def build_node_embeddings(
    manifest_data: dict,
    catalog_data: dict,
    host: str = "falkordb",
    port: int = 6379,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> None:
    """Compute embeddings and store them as an `embedding` property on existing
    Model / Source / Seed / Snapshot nodes in dbt_graph.

    Also creates a vector index on each label so KNN queries are efficient.
    Safe to call repeatedly – index creation is idempotent.
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

    if not to_embed:
        logger.warning("No embeddable nodes found in manifest")
        db.close()
        return

    # Create vector indexes (one per label, idempotent)
    for label in {label for _, label in to_embed.values()}:
        try:
            graph.query(
                f"CREATE VECTOR INDEX FOR (n:{label}) ON (n.embedding) "
                f"OPTIONS {{dimension: {EMBEDDING_DIM}, similarityFunction: 'cosine'}}"
            )
            logger.info("Created vector index on %s.embedding", label)
        except Exception as e:
            logger.debug("Vector index on %s already exists or failed: %s", label, e)

    # Batch-compute embeddings
    ids = list(to_embed.keys())
    texts = [_node_text(to_embed[uid][0], catalog_nodes) for uid in ids]
    logger.info("Computing embeddings for %d nodes…", len(texts))
    vectors = _embedder().embed_documents(texts)

    # Store embedding on each existing node
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

        for label in self.labels:
            try:
                result = graph.query(
                    f"CALL db.idx.fulltext.queryNodes('{label}', $query) "
                    f"YIELD node, score "
                    f"RETURN node.name AS name, node.description AS description, "
                    f"node.unique_id AS unique_id, node.resource_type AS resource_type, score",
                    {"query": query},
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
                logger.debug("Fulltext query skipped for label %s: %s", label, e)

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
        region = session.region_name or "us-east-1"
        client = boto3.client("bedrock-agent-runtime", region_name=region)

        response = client.rerank(
            rerankingConfiguration={
                "type": "BEDROCK_RERANKING_MODEL",
                "bedrockRerankingConfiguration": {
                    "modelConfiguration": {
                        "modelArn": f"arn:aws:bedrock:{region}::foundation-model/amazon.rerank-v1:0",
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
            textQuery={"text": query},
        )

        reranked: list[Document] = []
        for result in response.get("rerankingResults", []):
            doc = docs[result["index"]]
            doc.metadata["rerank_score"] = round(result["relevanceScore"], 4)
            reranked.append(doc)

        logger.info("Reranked %d docs with Amazon Rerank 1.0", len(reranked))
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

        # Rerank with Amazon Rerank 1.0 if on Bedrock
        docs, was_reranked = _rerank(query, docs, self.k)

        # Prefix each doc with its score so Streamlit can surface it
        score_key = "rerank_score" if was_reranked else "score"
        for doc in docs:
            score = doc.metadata.get(score_key, "")
            label = "rerank" if was_reranked else "knn"
            doc.page_content = f"[{label}:{score}] {doc.page_content}"

        return docs
