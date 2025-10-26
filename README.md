# dbt-kg: Graph-Powered AI Chat for dbt

Transform your dbt metadata into an intelligent, queryable knowledge graph using FalkorDB or Neo4j, and interact with it through AI-powered natural language chat.

## Overview

dbt-kg bridges the gap between complex dbt project structures and intuitive querying by leveraging graph databases. Upload your dbt metadata (manifest.json and catalog.json) and start asking sophisticated questions about dependencies, materializations, and project-wide relationships in plain English.

## Features

- 🚀 **Graph Database Integration**: Works with both FalkorDB and Neo4j
- 🤖 **AI-Powered Chat**: Natural language querying of your dbt project
- 📊 **Complex Dependency Analysis**: Recursive dependency tracking and analysis
- 🔍 **Smart Job Grouping**: Intelligent separation strategies for orchestration
- 🌐 **Web Interface**: Easy-to-use Streamlit-based UI

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

3. **Configure your environment variables**
   
   Edit `.env` file with your preferred settings:

   **LLM Provider Configuration:**
   ```bash
   # Choose your LLM provider (bedrock, anthropic, or openai)
   LLM_MODEL_ID='anthropic:claude-3-sonnet-20240229'
   # LLM_MODEL_ID='openai:gpt-4'
   # LLM_MODEL_ID='bedrock:anthropic.claude-3-sonnet-20240229-v1:0'
   ```

   **LLM API Keys:**
   ```bash
   # Anthropic
   ANTHROPIC_API_KEY=your_anthropic_api_key_here

   # OpenAI
   # OPENAI_API_KEY=your_openai_api_key_here

   # AWS Bedrock
   # AWS_ACCESS_KEY_ID=your_aws_access_key_id
   # AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
   # AWS_DEFAULT_REGION=us-east-1
   ```

   **Graph Database Configuration:**
   ```bash
   # Choose your graph database (falkordb or neo4j)
   GRAPH_DB=falkordb

   # Database authentication (uncomment if needed)
   # GRAPH_USER=neo4j
   # GRAPH_PASSWORD=password
   ```

4. **Start the application**
   ```bash
   just all
   ```
   
   Or without Just:
   ```bash
   docker-compose up -d
   ```

5. **Access the application**
   - **Main Application**: http://localhost:8501
   - **Graph Database UI**: http://localhost:3000

## Usage

### Loading Your dbt Project

1. Navigate to http://localhost:8501
2. Go to the "Load DBT Manifest" page from the sidebar
3. Upload your `manifest.json` and `catalog.json` files
   - Use your own project files, or
   - Use the sample files in `DbtExampleProject/target/`
4. Wait for the confirmation message

### Exploring Your Graph

1. Open the graph database UI at http://localhost:3000
2. Run this query to visualize your dbt knowledge graph:
   ```cypher
   MATCH (a)-[b]-(c)
   RETURN a, b, c
   ```

### AI-Powered Querying

Return to the main page (http://localhost:8501) and start asking questions:

**Example queries:**
- "Can you find the source tables that are used by the most models, including indirect dependencies?"
- "Please identify the model with the highest number of dependent models, including transitive dependencies"
- "Can you analyze our dbt dependency graph and create a job separation strategy?"
- "List all models that depend on the raw.orders source"

## Configuration Options

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `LLM_MODEL_ID` | LLM model identifier with provider prefix | Yes | - |
| `ANTHROPIC_API_KEY` | Anthropic API key | If using Anthropic | - |
| `OPENAI_API_KEY` | OpenAI API key | If using OpenAI | - |
| `AWS_ACCESS_KEY_ID` | AWS access key for Bedrock | If using Bedrock | - |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key for Bedrock | If using Bedrock | - |
| `AWS_DEFAULT_REGION` | AWS region for Bedrock | If using Bedrock | us-east-1 |
| `GRAPH_DB` | Graph database type | Yes | falkordb |
| `GRAPH_USER` | Graph database username | If auth required | - |
| `GRAPH_PASSWORD` | Graph database password | If auth required | - |

### Supported LLM Providers

- **Anthropic**: `anthropic:claude-3-sonnet-20240229`
- **OpenAI**: `openai:gpt-4` or `openai:gpt-3.5-turbo`
- **AWS Bedrock**: `bedrock:anthropic.claude-3-sonnet-20240229-v1:0`

### Supported Graph Databases

- **FalkorDB**: Lightweight, Redis-based graph database
- **Neo4j**: Full-featured graph database platform

## Use Cases

### Dependency Analysis
- Find circular dependencies
- Identify most reused sources
- Trace dependency chains recursively

### Project Optimization
- Identify bottleneck models
- Optimize materialization strategies
- Plan incremental improvements

### Orchestration Planning
- Break monolithic jobs into manageable chunks
- Define clear inter-job dependencies
- Eliminate circular dependencies in DAG design
