import uuid
import os
import logging
import traceback

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from app.models import ChatModel

logger = logging.getLogger(__name__)

from app.server.llm import LLMAgent

from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
from langchain_core.tools import create_retriever_tool, StructuredTool
from falkordb import FalkorDB as FalkorDBClient

from app.rag.vector_index import FalkorDBNodeRetriever, FalkorDBFulltextRetriever

chat_router = APIRouter()


class ChatRequest(BaseModel):
    message: str


def get_user_chat_config(session_id: str) -> dict:
    return {'configurable': {'thread_id': session_id},
            "recursion_limit": 100}


@chat_router.post("/new")
async def new_chat(request: Request):
    """Create a new chat session."""
    request.session['chat_session_id'] = f'user_{uuid.uuid4()}'
    return {'results': 'ok'}


@chat_router.post("/ask")
async def chat(
    request: Request,
    chat_request: ChatRequest,
):
    if 'chat_session_id' not in request.session:
        await new_chat(request)
    session_id = request.session['chat_session_id']
    user_config = get_user_chat_config(session_id)

    graph_db = os.environ.get('GRAPH_DB')
    graph_user = os.environ.get('GRAPH_USER')
    graph_password = os.environ.get('GRAPH_PASSWORD')

    tools = []

    if graph_db == 'falkordb':
        _falkor_kwargs = {'host': 'falkordb', 'port': 6379}
        if graph_user:
            _falkor_kwargs['username'] = graph_user
        if graph_password:
            _falkor_kwargs['password'] = graph_password

        def _run_cypher(query: str) -> str:
            try:
                db = FalkorDBClient(**_falkor_kwargs)
                g = db.select_graph("dbt_graph")
                result = g.query(query)
                if not result.result_set:
                    return "No results found."
                header = [col[1] if isinstance(col, (list, tuple)) else col
                          for col in result.header]
                rows = [dict(zip(header, row)) for row in result.result_set]
                return str(rows)
            except Exception as e:
                return f"Query error: {e}"

        tools.append(StructuredTool.from_function(
            func=_run_cypher,
            name="Falkor_Knowledge_Graph_Retriever",
            description=(
                "Execute a Cypher query directly against the dbt knowledge graph "
                "in FalkorDB and return raw results. Use for structural questions: "
                "lineage, dependencies, tests, metadata. Pass a valid Cypher string."
            ),
        ))

        # Full-text search over description property
        fulltext_retriever = FalkorDBFulltextRetriever(
            host='falkordb', port=6379,
            username=graph_user, password=graph_password,
            k=20,
        )
        tools.append(create_retriever_tool(
            fulltext_retriever,
            name="DBT_Fulltext_Search",
            description=(
                "Search for dbt models by keywords in their descriptions using "
                "full-text search. Supports prefix matching (e.g. 'stud*'), "
                "fuzzy matching, and boolean operators (AND, OR, NOT). Use when "
                "the user provides specific terms or model name fragments."
            ),
        ))

        # Semantic search over embeddings stored on graph nodes
        retriever = FalkorDBNodeRetriever(
            host='falkordb', port=6379,
            username=graph_user, password=graph_password,
            k=35,
        )
        tools.append(create_retriever_tool(
            retriever,
            name="DBT_Semantic_Search",
            description=(
                "Search for dbt models by meaning or business concept using "
                "semantic similarity. The embeddings are stored directly on "
                "graph nodes. Use when the question is about what a model "
                "represents, or to find models related to a domain "
                "(e.g. 'financial aid', 'student retention', 'grade inflation')."
            ),
        ))

    elif graph_db == 'neo4j':
        graph = Neo4jGraph(
            url="bolt://neo4j:7687",
            username=graph_user,
            password=graph_password,
            enhanced_schema=True,
        )
        chain = GraphCypherQAChain.from_llm(
            ChatModel(), graph=graph, verbose=True,
            allow_dangerous_requests=True,
        )
        tools.append(chain.as_tool(
            name="Neo4J_Knowledge_Graph_Retriever",
            description=(
                "Query your Neo4j graph database using Cypher to retrieve "
                "nodes, relationships, and insights."
            ),
        ))

    async def stream_agent_response():
        try:
            async with LLMAgent(tools=tools) as llm_agent:
                async for chat_msg in llm_agent.astream_events(
                        chat_request.message, user_config):
                    yield chat_msg.content
        except Exception as e:
            logger.error("Stream error: %s\n%s", e, traceback.format_exc())
            yield f"\n\n*Error: {e}*"

    return StreamingResponse(stream_agent_response(), media_type='application/json')
