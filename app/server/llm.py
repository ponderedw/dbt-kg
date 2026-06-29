from enum import Enum
from typing import AsyncGenerator

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage, BaseMessage, \
    SystemMessage, ToolMessage, AIMessage, AIMessageChunk

from app.databases.postgres import Database
from app.models import ChatModel
from app.utils.logger import Logger
import os


graphdb_name = os.environ.get('GRAPH_DB')
PROMPT_MESSAGE = f"""You are a DBT Knowledge Assistant with access to a {graphdb_name} knowledge graph and a semantic vector index containing our dbt project metadata.

## Knowledge Graph ({graphdb_name})
Node types: Model, Source, Macro, Test, Seed, Snapshot
Model attributes: name, materialized, resource_type, alias, schema, description
Relationships: DEPENDS_ON, REFERENCES, TESTS, USES_MACRO

Example – find all downstreams of stg_students:
MATCH (start:Model {{name: 'stg_students'}})<-[:DEPENDS_ON]-(downstream:Model)
RETURN downstream.name AS model_name, downstream.materialized AS materialization_type
ORDER BY model_name

Example – find all upstreams of stg_students:
MATCH (start:Model {{name: 'stg_students'}})-[:DEPENDS_ON]->(upstream:Model)
RETURN upstream.name AS model_name, upstream.materialized AS materialization_type
ORDER BY model_name

## Full-Text Search (DBT_Fulltext_Search)
A full-text index over model descriptions. Use it to:
- Find models by exact keywords or name fragments (e.g. "retention", "GPA", "tuition")
- Prefix search: "stud*" matches student, students, etc.
- Boolean operators: "retention AND graduation", "financial NOT aid"
- Faster and more precise than semantic search when the user provides specific terms

## Semantic Search (DBT_Semantic_Search)
A vector index over chunked model/source/seed/snapshot text. Use it to:
- Find models or columns by business concept or meaning
- Discover models related to a domain without knowing exact names
- Answer "which models deal with X?" when X is a concept, not a keyword

**IMPORTANT:** DBT_Semantic_Search returns `unique_id` values — these are the join key
into the main knowledge graph. After getting `unique_id`s from semantic search, always
follow up with the {graphdb_name} retriever to fetch full node details, lineage, tests,
or any other attributes. Example:

  1. DBT_Semantic_Search("financial aid disbursement") → unique_id: "model.psdw.fct_aid"
  2. Cypher: MATCH (m:Model {{unique_id: 'model.psdw.fct_aid'}})-[:DEPENDS_ON]->(upstream) RETURN upstream.name

## Tool selection guide
- Structural / lineage questions → {graphdb_name} retriever (Cypher)
- Specific keywords or name fragments → DBT_Fulltext_Search
- Business concepts or meaning → DBT_Semantic_Search, then Cypher with the returned unique_ids
- Complex questions → combine tools"""


class LLMEventType(Enum):
    """Event types for the LLM agent."""

    STORED_MESSAGE = 'stored_message'
    RETRIEVER_START = 'on_retriever_start'
    RETRIEVER_END = 'on_retriever_end'
    CHAT_CHUNK = 'on_chat_model_stream'
    DONE = 'done'


class ChatMessage:
    class Sender(Enum):
        """The sender of the message."""
        SYSTEM = 'system'
        AI = 'ai'
        HUMAN = 'human'
        TOOL = 'tool'

    def __init__(self, type: LLMEventType, sender: Sender,
                 content: str, payload: dict = None):
        self.type = type
        self.sender: str = sender.value
        self.content = content
        self.payload = payload or {}

    @classmethod
    def from_base_message(cls, message: BaseMessage) -> 'ChatMessage':
        message_type_lookup = {
            HumanMessage: cls.Sender.HUMAN,
            SystemMessage: cls.Sender.SYSTEM,
            ToolMessage: cls.Sender.TOOL,
            AIMessage: cls.Sender.AI,
            AIMessageChunk: cls.Sender.AI,
        }

        # Different message types have different structures.
        try:
            content = message.content[0]['text']
        except (KeyError, TypeError, IndexError):
            content = message.content

        return ChatMessage(
            LLMEventType.STORED_MESSAGE,
            sender=message_type_lookup[type(message)],
            content=content,
        )

    @classmethod
    def from_event(cls, event: dict) -> 'ChatMessage':
        """Convert an event from the LLM agent to a `ChatMessage` object."""
        match event['event']:
            case 'on_chat_model_stream':
                if event['data']['chunk'].content:
                    return cls._handle_on_chat_model_stream(event)
            case 'on_tool_start':
                tool_name = event.get('name', 'Tool')
                query = event['data']['input'].get('query', '')
                if tool_name == 'DBT_Semantic_Search':
                    rerank_note = ''
                    if os.environ.get('LLM_MODEL_ID', '').startswith('bedrock'):
                        reranker = os.environ.get('BEDROCK_RERANKER_MODEL_ID', 'cohere.rerank-v3-5:0')
                        rerank_note = f'  \n*Reranking with {reranker}…*'
                    content = f'\n\n**Semantic search:** `{query}`{rerank_note}\n'
                elif tool_name == 'DBT_Fulltext_Search':
                    content = (
                        f'\n\n**Full-text search:** `{query}`  \n'
                        f'*Searching: name, description, schema, alias, materialized, resource\\_type*\n'
                    )
                else:
                    content = f'\n\n**{tool_name}:**\n```\n{query}\n```\n'
                return ChatMessage(LLMEventType.CHAT_CHUNK, cls.Sender.AI, content)
            case 'on_prompt_start' | 'on_parser_start':
                return ChatMessage(LLMEventType.CHAT_CHUNK, cls.Sender.AI, '')
            case 'on_tool_end':
                tool_name = event.get('name', '')
                if tool_name == 'Falkor_Knowledge_Graph_Retriever':
                    raw = event['data'].get('output', '')
                    output = raw.content if hasattr(raw, 'content') else str(raw)
                    content = f'\n```\n{output}\n```\n' if output else '\n*No results*\n'
                    return ChatMessage(LLMEventType.CHAT_CHUNK, cls.Sender.AI, content)
                if tool_name in ('DBT_Semantic_Search', 'DBT_Fulltext_Search'):
                    raw = event['data'].get('output', '')
                    output = raw.content if hasattr(raw, 'content') else str(raw)
                    search_label = 'Semantic' if tool_name == 'DBT_Semantic_Search' else 'Full-text'

                    if not output:
                        content = f'\n*{search_label} search: no results found*\n\n'
                        return ChatMessage(LLMEventType.CHAT_CHUNK, cls.Sender.AI, content)

                    node_types = {'Model', 'Source', 'Seed', 'Snapshot',
                                  'model', 'source', 'seed', 'snapshot'}

                    def _parse_bullets(lines):
                        bullets = []
                        for line in lines:
                            line = line.strip()
                            if not line:
                                continue
                            score_str = ''
                            if line.startswith('[rerank:'):
                                end = line.index(']')
                                score_str = f' _(rerank: {line[8:end]})_'
                                line = line[end + 2:]
                            elif line.startswith('[similarity:'):
                                end = line.index(']')
                                score_str = f' _(similarity: {line[12:end]})_'
                                line = line[end + 2:]
                            elif line.startswith('[fulltext:'):
                                end = line.index(']')
                                score_str = f' _(score: {line[10:end]})_'
                                line = line[end + 2:]
                            if line.split(':')[0].strip() in node_types:
                                bullets.append(f'- {line}{score_str}')
                        return bullets

                    import re as _re
                    knn_header_match = _re.search(r'===KNN\((\d+)\)===', output)
                    if knn_header_match and '===RERANKED===' in output:
                        knn_count = knn_header_match.group(1)
                        knn_part, reranked_part = output.split('===RERANKED===', 1)
                        knn_lines = _re.split(r'===KNN\(\d+\)===', knn_part)[-1].splitlines()
                        reranked_lines = reranked_part.splitlines()
                        knn_bullets = _parse_bullets(knn_lines)
                        reranked_bullets = _parse_bullets(reranked_lines)
                        reranker = os.environ.get('BEDROCK_RERANKER_MODEL_ID', 'cohere.rerank-v3-5:0')
                        content = ''
                        if knn_bullets:
                            content += f'\n**{search_label} — {knn_count} candidates by vector similarity (sent to reranker):**\n' + '\n'.join(knn_bullets) + '\n\n'
                        if reranked_bullets:
                            content += f'**Top {len(reranked_bullets)} reranked by {reranker}:**\n' + '\n'.join(reranked_bullets) + '\n\n'
                        elif knn_bullets:
                            content += f'*⚠️ Reranker ({reranker}): none returned — check FastAPI logs*\n\n'
                        if not content:
                            content = f'\n*{search_label} search: no results found*\n\n'
                    else:
                        bullets = []
                        reranked = False
                        for line in output.splitlines():
                            line = line.strip()
                            if not line:
                                continue
                            score_str = ''
                            if line.startswith('[rerank:'):
                                reranked = True
                                end = line.index(']')
                                score_str = f' _(rerank: {line[8:end]})_'
                                line = line[end + 2:]
                            elif line.startswith('[similarity:'):
                                end = line.index(']')
                                score_str = f' _(similarity: {line[12:end]})_'
                                line = line[end + 2:]
                            elif line.startswith('[fulltext:'):
                                end = line.index(']')
                                score_str = f' _(score: {line[10:end]})_'
                                line = line[end + 2:]
                            if line.split(':')[0].strip() in node_types:
                                bullets.append(f'- {line}{score_str}')
                        on_bedrock = os.environ.get('LLM_MODEL_ID', '').startswith('bedrock')
                        if bullets:
                            if reranked:
                                suffix = ' · reranked'
                            elif on_bedrock and tool_name == 'DBT_Semantic_Search':
                                suffix = ' · ⚠️ reranking failed (check FastAPI logs)'
                            else:
                                suffix = ''
                            content = f'\n**{search_label} search found{suffix}:**\n' + '\n'.join(bullets) + '\n\n'
                        else:
                            content = f'\n*{search_label} search: no results found*\n\n'
                    return ChatMessage(LLMEventType.CHAT_CHUNK, cls.Sender.AI, content)
                return ChatMessage(LLMEventType.CHAT_CHUNK, cls.Sender.AI, '')
            case 'on_prompt_end' | 'on_parser_end':
                return ChatMessage(LLMEventType.CHAT_CHUNK, cls.Sender.AI, '')
            # The conversation is done.
            case 'done':
                return ChatMessage(
                    LLMEventType.DONE,
                    cls.Sender.SYSTEM,
                    'Done',
                )
            # Known events that we ignore.
            case 'on_chat_model_start' | 'on_chain_start' | 'on_chain_end' \
                | 'on_chat_model_stream' | 'on_chat_model_end' | 'on_chain_stream' \
                | 'on_retriever_start' | 'on_retriever_end':
                Logger().get_logger().debug('Ignoring message', event['event'])
                return ''
            # Unknown events — log and ignore so new LangGraph event types don't crash the stream.
            case _:
                Logger().get_logger().warning('Unhandled LangGraph event: %s', event.get('event'))
                return ''

    @classmethod
    def _handle_on_chat_model_stream(cls, event: dict) -> 'ChatMessage':
        content = event['data']['chunk'].content
        content_type = ''
        if event.get('metadata', {}).get('langgraph_node') == 'tools':
            return ''
        if not isinstance(content, str):
            content_type = content[0]['type']
            content = content[0].get('text')
        # If the message is a tool call, just print a debug message.
        if content_type in ('tool_use', 'tool_call'):
            Logger().get_logger().debug('Stream.tool_calls:',
                                        event['data']['chunk'].tool_calls,
                                        flush=True)
            return ''
        else:
            return ChatMessage(LLMEventType.CHAT_CHUNK, cls.Sender.AI, content
                               if content is not None else '')

    def to_dict(self) -> dict:
        """Returns a dictionary representation of the message."""
        return {
            'sender': self.sender,
            'content': self.content,
            'payload': self.payload,
        }


class LLMAgent:

    def __init__(self, tools):
        self._agent = None
        self._llm = None
        self.retriever_tool_name = 'Internal_Company_Info_Retriever'
        self._checkpointer_ctx = None
        self.tools = tools

    async def __aenter__(self) -> 'LLMAgent':

        tools = self.tools
        self._llm = ChatModel()

        # Checkpointer for the agent.
        self._checkpointer_ctx = AsyncPostgresSaver\
            .from_conn_string(
                Database().get_connection_string())
        checkpointer = await self._checkpointer_ctx.__aenter__()

        # Create the agent itself.
        self._agent = create_react_agent(
            self._llm,
            tools,
            checkpointer=checkpointer,
            # state_modifier=SystemMessage(PROMPT_MESSAGE),
            prompt=PROMPT_MESSAGE
        )

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the agent and the checkpointer."""
        await self._checkpointer_ctx.__aexit__(exc_type, exc_val, exc_tb)
        self._llm = None
        self._agent = None
        self._checkpointer_ctx = None

    async def astream_events(self, message: str,
                             chat_session: dict) -> AsyncGenerator[ChatMessage,
                                                                   None]:
        async for event in self._agent.astream_events(
            {"messages": [HumanMessage(content=message)]},
            config=chat_session,
            version='v2',
        ):
            message = ChatMessage.from_event(event)
            if message:
                yield message

        # Let the client know that the conversation is done.
        # yield ChatMessage.from_event({'event': 'done'})
