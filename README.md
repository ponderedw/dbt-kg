# dbt-kg: Graph-Powered AI Chat for dbt

Transform your dbt metadata into an intelligent, queryable knowledge graph using FalkorDB or Neo4j, and interact with it through AI-powered natural language chat.

## Overview

dbt-kg bridges the gap between complex dbt project structures and intuitive querying by leveraging graph databases and GraphRAG. Upload your dbt metadata (`manifest.json` and `catalog.json`) and start asking sophisticated questions about dependencies, materializations, lineage, and business concepts — all in plain English.

The AI agent uses three complementary search tools to answer queries:

| Tool | Best for |
|------|----------|
| **Graph Cypher retriever** | Structural questions: lineage, blast radius, test coverage, dependency chains |
| **Full-text search** | Exact keywords or name fragments: "GPA", "retention", `stud*`, boolean operators |
| **Semantic vector search** | Business concepts and meaning: "models related to student financial hardship" |

For semantic search on AWS Bedrock, results are automatically reranked with **Cohere Rerank v3.5** — surfacing the most relevant candidates from the full vector search pool.

## Features

- **Graph Database Integration** — works with FalkorDB (default) and Neo4j
- **AI-Powered Chat** — natural language querying via LangGraph ReAct agent with persistent conversation memory
- **GraphRAG** — embeddings stored directly on graph nodes; one embedding per node combining name, description, schema, and all column descriptions
- **Split Embeddings** (`SPLIT_EMBEDDINGS=true`) — large node text is chunked (6 000 chars, 200 overlap) and stored in a separate `dbt_graph_chunks` FalkorDB graph; KNN search queries the chunk graph and deduplicates to parent node `unique_id` before Cypher lookup in the main graph; required when using Bedrock Titan (8 192 token limit)
- **Full-Text Search** — FalkorDB full-text index over name, description, schema, alias, materialization, and resource type
- **Semantic Vector Search + Reranking** — KNN vector search across all node types (top 35 unique parents), with optional Cohere Rerank v3.5 reranking (Bedrock only)
- **Transparent Search UI** — Streamlit surfaces all search steps: query, full candidate pool by similarity, and top-k reranked results
- **Multi-provider LLM support** — AWS Bedrock, Anthropic, OpenAI
- **Incremental Embedding Rebuild** — `POST /embeddings/rebuild_embeddings/` re-embeds only nodes whose text changed (compare old vs new manifest); callable from CI after each dbt run

## Quick Start

### Prerequisites

- Docker and Docker Compose
- [Just](https://github.com/casey/just) command runner (optional but recommended)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/ponderedw/dbt-kg.git
   cd dbt-kg
   ```

2. **Set up environment configuration**
   ```bash
   cp template.env .env
   ```

3. **Configure your environment variables** (see [Configuration](#configuration) below)

4. **Start the application**
   ```bash
   just all
   # or without Just:
   docker-compose up -d
   ```

5. **Access the application**
   - **Chat UI**: http://localhost:8501
   - **Graph DB browser**: http://localhost:3000

### Loading Your dbt Project

1. Navigate to http://localhost:8501
2. Go to **Load DBT Manifest** in the sidebar
3. Upload your `manifest.json` and `catalog.json`
   - Or use the included sample: `DbtEducationalDataProject/target/`
4. Wait for the confirmation — this builds the graph, computes embeddings on each node, and creates vector + full-text indexes

### Exploring the Graph

Open the graph browser at http://localhost:3000 and run:
```cypher
MATCH (a)-[b]-(c) RETURN a, b, c
```

## Example Queries

**Lineage & blast radius**
- "What is the full blast radius if I drop `stg_students`?"
- "Which models have no downstream dependents?"
- "Show me the complete lineage from raw sources to `student_academic_summary`."

**Full-text search**
- "Find all models that mention 'GPA' in their description."
- "Which models describe something related to 'tuition'?"
- "Find models that mention 'retention' but NOT 'dropout'."

**Semantic / business concept**
- "Which models deal with student financial hardship or outstanding balances?"
- "Find models related to grade inflation analysis."
- "Which models track early warning signals for students at academic risk?"

**Combined graph + semantic**
- "Find the model that tracks at-risk students, then show me all the models it depends on."
- "Which model best represents a student's financial profile? Show me everything downstream of its source tables."

## Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `LLM_MODEL_ID` | LLM model with provider prefix (`bedrock:...`, `antropic:...`, `openai:...`) | Yes | — |
| `ANTHROPIC_API_KEY` | Anthropic API key | If using Anthropic | — |
| `OPENAI_API_KEY` | OpenAI API key | If using OpenAI | — |
| `AWS_ACCESS_KEY_ID` | AWS access key | If using Bedrock | — |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | If using Bedrock | — |
| `AWS_DEFAULT_REGION` | AWS region | If using Bedrock | `us-east-1` |
| `BEDROCK_RERANKER_MODEL_ID` | Cohere reranker model ID | No | `cohere.rerank-v3-5:0` |
| `BEDROCK_RERANKER_MODEL_ARN` | Full ARN override for reranker | No | built from model ID + region |
| `SPLIT_EMBEDDINGS` | Split large node text into chunks stored in `dbt_graph_chunks` FalkorDB graph (recommended for Bedrock Titan) | No | `false` |
| `GRAPH_DB` | Graph database type (`falkordb` or `neo4j`) | Yes | `falkordb` |
| `GRAPH_USER` | Graph database username | If auth required | — |
| `GRAPH_PASSWORD` | Graph database password | If auth required | — |
| `SECRET_KEY` | Session secret key | Yes | — |
| `FAST_API_ACCESS_SECRET_TOKEN` | API access token | Yes | — |

### Supported LLM Providers

```bash
LLM_MODEL_ID='bedrock:us.anthropic.claude-sonnet-4-20250514-v1:0'
LLM_MODEL_ID='antropic:claude-opus-4-5'
LLM_MODEL_ID='openai:gpt-4o'
```

Embeddings are provider-matched automatically:
- **Bedrock** → Amazon Titan Embed Text v1 (1536 dims)
- **OpenAI** → `text-embedding-3-small` (1536 dims)
- **Anthropic** → Amazon Titan Embed Text v1 via Bedrock (requires AWS credentials)

### Supported Graph Databases

- **FalkorDB** (recommended) — open-source, Redis-based, native vector index and full-text index support
- **Neo4j** — full-featured graph database; graph Cypher retriever only (no vector/full-text search)

## Architecture

```
Upload manifest.json + catalog.json
        │
        ▼
  dbt_graph_loader ──► dbt_graph (Model, Source, Seed, Snapshot nodes + relationships)
        │
        ▼
  build_node_embeddings()
  ├── SPLIT_EMBEDDINGS=false (default)
  │     └── embedding attribute stored directly on each node in dbt_graph
  └── SPLIT_EMBEDDINGS=true
        └── node text split into 6 000-char chunks (200 overlap)
              └── Chunk nodes stored in dbt_graph_chunks
                    (unique_id, parent_id, embedding, text, name, description,
                     resource_type, schema, materialized, parent_label)
  build_fulltext_index() ──► full-text index on 6 properties per label in dbt_graph
        │
        ▼
  LangGraph ReAct agent
  ├── Falkor_Knowledge_Graph_Retriever  (Cypher against dbt_graph)
  ├── DBT_Fulltext_Search               (FalkorDBFulltextRetriever, k=20)
  └── DBT_Semantic_Search               (FalkorDBNodeRetriever, k=35)
        │   ├── queries dbt_graph_chunks (SPLIT_EMBEDDINGS=true)
        │   │     deduplicates by parent_id → returns unique_id for Cypher join
        │   └── or queries dbt_graph directly (SPLIT_EMBEDDINGS=false)
        │   └── optional Cohere Rerank v3.5 (Bedrock only)
        ▼
  Streamlit chat UI (streaming, shows search steps + rerank comparison)
```

## API Endpoints

### `POST /embeddings/rebuild_embeddings/`

Rebuilds vector and full-text indexes from a manifest without re-uploading the full graph. Callable from CI after each dbt run.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `manifest_file` | multipart file | Yes | Current `manifest.json` |
| `old_manifest_file` | multipart file | No | Previous `manifest.json` — enables incremental mode (only re-embeds nodes whose text changed) |

**Full rebuild:**
```bash
curl -X POST "$FAST_API_URL/embeddings/rebuild_embeddings/" \
  -F "manifest_file=@manifest.json"
```

**Incremental rebuild (CI):**
```bash
# Download previous manifest from S3, upload both
aws s3 cp s3://my-bucket/dbt-kg/main/manifest.json /tmp/old-manifest.json
curl -X POST "$FAST_API_URL/embeddings/rebuild_embeddings/" \
  -F "manifest_file=@/tmp/manifest.json" \
  -F "old_manifest_file=@/tmp/old-manifest.json"
```

Response: `{"results": "ok", "mode": "incremental (12 nodes)"}` or `{"results": "ok", "mode": "full"}`
