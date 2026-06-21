import json
import os

from fastapi import APIRouter, Request, File, UploadFile
from typing import Annotated

from dbt_graph_loader.loaders.falkordb_loader import DBTFalkorDBLoader
from dbt_graph_loader.loaders.neo4j_loader import DBTNeo4jLoader
from app.rag.vector_index import build_node_embeddings

embeddings_router = APIRouter()


@embeddings_router.get("/")
async def new_chat(request: Request):
    return {'results': 'ok'}


@embeddings_router.post("/upload_dbt_to_kg/")
async def upload_dbt_metadata(catalog_file: Annotated[UploadFile, File()],
                              manifest_file: Annotated[UploadFile, File()]):
    manifest_bytes = await manifest_file.read()
    catalog_bytes = await catalog_file.read()

    manifest_str = manifest_bytes.decode()
    catalog_str = catalog_bytes.decode() if catalog_bytes else None

    graph_db = os.environ.get('GRAPH_DB')
    graph_user = os.environ.get('GRAPH_USER')
    graph_password = os.environ.get('GRAPH_PASSWORD')

    if graph_db == 'falkordb':
        loader = DBTFalkorDBLoader(username=graph_user, password=graph_password)
        loader.load_dbt_to_falkordb_from_strings(manifest_str, catalog_str)

        # Build vector index from model and column descriptions
        manifest_data = json.loads(manifest_str)
        catalog_data = json.loads(catalog_str) if catalog_str else {}
        build_node_embeddings(
            manifest_data=manifest_data,
            catalog_data=catalog_data,
            username=graph_user,
            password=graph_password,
        )

    elif graph_db == 'neo4j':
        loader = DBTNeo4jLoader('neo4j://neo4j:7687', graph_user, graph_password)
        loader.load_dbt_to_neo4j_from_strings(manifest_str, catalog_str)
    else:
        raise Exception('GRAPH_DB value is incorrect')

    return {'results': 'ok'}
