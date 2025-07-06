import uuid
import os

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from app.models import ChatModel

from app.server.llm import LLMAgent

from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
from langchain.chains import FalkorDBQAChain
from langchain_community.graphs import FalkorDBGraph
from langchain_core.prompts.prompt import PromptTemplate
# from langchain.callbacks import TimeoutCallback

chat_router = APIRouter()

# CYPHER_GENERATION_TEMPLATE = """Task:Generate Cypher statement to query a graph database.
# Instructions:
# Use only the provided relationship types and properties in the schema.
# Do not use any other relationship types or properties that are not provided.
# Schema:
# {schema}
# Note: Do not include any explanations or apologies in your responses.
# Do not respond to any questions that might ask anything else than for you to construct a Cypher statement.
# Do not include any text except the generated Cypher statement.
# Examples: Here are a few examples of generated Cypher statements for particular questions:
# # How many people work for TechCorp?
# MATCH (a:Company {name: "TechCorp"})<-[c:WORKS_FOR*]-(b)
# RETURN count(distinct b) as numberOfWorkers

# The question is:
# {question}"""

# CYPHER_GENERATION_PROMPT = PromptTemplate(
#     input_variables=["schema", "question"], template=CYPHER_GENERATION_TEMPLATE
# )


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
    # Get the user chat configuration and the LLM agent.
    user_config = get_user_chat_config(session_id)
    retriever_tool = None
    if os.environ.get('GRAPH_DB') == 'falkordb':
        graph = FalkorDBGraph(host='falkordb', port=6379, database="dbt_graph",
                              username=os.environ.get('GRAPH_USER_FALKORDB'),
                              password=os.environ.get('GRAPH_PASSWORD_FALKORDB'
                                                      ),)
        chain = FalkorDBQAChain.from_llm(ChatModel(), graph=graph, verbose=True, allow_dangerous_requests=True)
        retriever_tool = chain.as_tool(name="Falkor_Knowledge_Graph_Retriever",
                                            description="Query and retrieve data from your Falkor graph database using Cypher syntax")
    elif os.environ.get('GRAPH_DB') == 'neo4j':
        graph = Neo4jGraph(url="bolt://neo4j:7687",
                           username=os.environ.get('GRAPH_USER_NEO4J'),
                           password=os.environ.get('GRAPH_PASSWORD_NEO4J'),
                           enhanced_schema=True)
        chain = GraphCypherQAChain.from_llm(
            ChatModel(),
            graph=graph,
            verbose=True,
            allow_dangerous_requests=True,
        )
        retriever_tool = chain.as_tool(name="Neo4J_Knowledge_Graph_Retriever",
                                       description="Query your Neo4j graph database using Cypher to retrieve nodes, relationships, and insights.")
    tools = [retriever_tool] if retriever_tool else []

    async def stream_agent_response():
        async with LLMAgent(tools=tools) as llm_agent:
            async for chat_msg in llm_agent.astream_events(
                 chat_request.message, user_config):
                yield chat_msg.content

    # Return the agent's response as a stream of JSON objects.
    return StreamingResponse(stream_agent_response(),
                             media_type='application/json')
