import json
import os

from fastapi import APIRouter, Request, File, UploadFile
from typing import Annotated, Optional

from dbt_graph_loader.loaders.falkordb_loader import DBTFalkorDBLoader
from dbt_graph_loader.loaders.neo4j_loader import DBTNeo4jLoader
from app.rag.vector_index import build_node_embeddings, build_fulltext_index, _get_changed_node_ids

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
        build_fulltext_index(
            username=graph_user,
            password=graph_password,
        )

    elif graph_db == 'neo4j':
        loader = DBTNeo4jLoader('neo4j://neo4j:7687', graph_user, graph_password)
        loader.load_dbt_to_neo4j_from_strings(manifest_str, catalog_str)
    else:
        raise Exception('GRAPH_DB value is incorrect')

    return {'results': 'ok'}


@embeddings_router.post("/rebuild_embeddings/")
async def rebuild_embeddings(
    manifest_file: Annotated[UploadFile, File()],
    old_manifest_file: Annotated[Optional[UploadFile], File()] = None,
):
    """Rebuild vector + fulltext indexes from a manifest.

    If old_manifest_file is provided, only re-embeds nodes whose text changed
    (incremental mode). Otherwise re-embeds all nodes.
    """
    graph_user = os.environ.get('GRAPH_USER')
    graph_password = os.environ.get('GRAPH_PASSWORD')

    manifest_data = json.loads((await manifest_file.read()).decode())

    node_ids = None
    if old_manifest_file is not None:
        old_manifest_data = json.loads((await old_manifest_file.read()).decode())
        node_ids = _get_changed_node_ids(old_manifest_data, manifest_data)

    build_node_embeddings(
        manifest_data=manifest_data,
        catalog_data={},
        username=graph_user,
        password=graph_password,
        node_ids=node_ids,
    )
    build_fulltext_index(
        username=graph_user,
        password=graph_password,
    )

    mode = f"incremental ({len(node_ids)} nodes)" if node_ids is not None else "full"
    return {'results': 'ok', 'mode': mode}
