from fastapi import APIRouter, Request, File, UploadFile
from typing import Annotated
from dbt_graph_loader.loaders.falkordb_loader import DBTFalkorDBLoader
from dbt_graph_loader.loaders.neo4j_loader import DBTNeo4jLoader
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
    if os.environ.get('GRAPH_DB') == 'falkordb':
        loader = DBTFalkorDBLoader(username=os.environ.get('GRAPH_USER'),
                                   password=os.environ.get('GRAPH_PASSWORD'))
        loader.load_dbt_to_falkordb_from_strings(manifest_str, catalog_str)
    elif os.environ.get('GRAPH_DB') == 'neo4j':
        loader = DBTNeo4jLoader('neo4j://neo4j:7687',
                                os.environ.get('GRAPH_USER'),
                                os.environ.get('GRAPH_PASSWORD'))
        loader.load_dbt_to_neo4j_from_strings(manifest_str, catalog_str)
    else:
        raise Exception('GRAPH_DB value is incorrect')
    return {'results': 'ok'}
