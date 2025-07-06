from fastapi import APIRouter, Request, File, UploadFile
from typing import Annotated
from app.server.load_falkor import DBTFalkorDBLoader
from app.server.load_neo4j import DBTNeo4jLoader
import os

embeddings_router = APIRouter()


@embeddings_router.get("/")
async def new_chat(request: Request):
    """Create a new chat session."""
    return {'results': 'ok'}


@embeddings_router.post("/upload_dbt_to_kg/")
async def upload_dbt_metadata(catalog_file: Annotated[UploadFile, File()],
                              manifest_file: Annotated[UploadFile, File()]):
    manifest_str = await manifest_file.read()
    catalog_str = await catalog_file.read()
    loader = DBTFalkorDBLoader(username=os.environ.get('GRAPH_USER_FALKORDB'),
                               password=os.environ.get('GRAPH_PASSWORD_FALKORDB'))
    loader.load_dbt_to_falkordb_from_strings(manifest_str, catalog_str)
    loader = DBTNeo4jLoader('neo4j://neo4j:7687',
                            os.environ.get('GRAPH_USER_NEO4J'),
                            os.environ.get('GRAPH_PASSWORD_NEO4J'))
    loader.load_dbt_to_neo4j_from_strings(manifest_str, catalog_str)
    return {'results': 'ok'}
