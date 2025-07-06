import json
import logging
from typing import Dict, Any, List, Optional
from neo4j import GraphDatabase

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DBTNeo4jLoader:
    """Load DBT manifest and catalog data into Neo4j as a knowledge graph"""
    
    def __init__(self, neo4j_uri: str, username: str, password: str):
        """Initialize Neo4j connection"""
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(username, password))
        
    def close(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
    
    def clear_database(self):
        """Clear all nodes and relationships"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            logger.info("Database cleared")
    
    def create_constraints(self):
        """Create constraints and indexes for better performance"""
        constraints = [
            "CREATE CONSTRAINT model_unique IF NOT EXISTS FOR (m:Model) REQUIRE m.unique_id IS UNIQUE",
            "CREATE CONSTRAINT source_unique IF NOT EXISTS FOR (s:Source) REQUIRE s.unique_id IS UNIQUE",
            "CREATE CONSTRAINT test_unique IF NOT EXISTS FOR (t:Test) REQUIRE t.unique_id IS UNIQUE",
            "CREATE CONSTRAINT macro_unique IF NOT EXISTS FOR (mac:Macro) REQUIRE mac.unique_id IS UNIQUE",
            "CREATE CONSTRAINT operation_unique IF NOT EXISTS FOR (o:Operation) REQUIRE o.unique_id IS UNIQUE",
            "CREATE CONSTRAINT seed_unique IF NOT EXISTS FOR (seed:Seed) REQUIRE seed.unique_id IS UNIQUE",
            "CREATE CONSTRAINT snapshot_unique IF NOT EXISTS FOR (snap:Snapshot) REQUIRE snap.unique_id IS UNIQUE",
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    logger.warning(f"Constraint creation failed (may already exist): {e}")
        
        logger.info("Constraints created")
    
    def load_manifest_data_from_strings(self, manifest_str: str, catalog_str: Optional[str] = None):
        """Load manifest and optional catalog data from strings"""
        # Parse manifest JSON string
        manifest_data = json.loads(manifest_str)
        
        # Parse catalog JSON string if provided
        catalog_data = {}
        if catalog_str:
            catalog_data = json.loads(catalog_str)
        
        return manifest_data, catalog_data
    
    def load_manifest_data_from_files(self, manifest_path: str, catalog_path: Optional[str] = None):
        """Load manifest and optional catalog data from files (kept for backward compatibility)"""
        # Load manifest
        with open(manifest_path, 'r') as f:
            manifest_data = json.load(f)
        
        # Load catalog if provided
        catalog_data = {}
        if catalog_path:
            with open(catalog_path, 'r') as f:
                catalog_data = json.load(f)
        
        return manifest_data, catalog_data
    
    def create_models(self, models: Dict[str, Any], catalog_nodes: Dict[str, Any] = None):
        """Create model nodes"""
        with self.driver.session() as session:
            for model_id, model_data in models.items():
                # Extract basic properties
                properties = {
                    'unique_id': model_id,
                    'name': model_data.get('name', ''),
                    'resource_type': model_data.get('resource_type', ''),
                    'package_name': model_data.get('package_name', ''),
                    'path': model_data.get('path', ''),
                    'original_file_path': model_data.get('original_file_path', ''),
                    'database': model_data.get('database', ''),
                    'schema': model_data.get('schema', ''),
                    'alias': model_data.get('alias', ''),
                    'materialized': model_data.get('config', {}).get('materialized', ''),
                    'description': model_data.get('description', ''),
                    'checksum': model_data.get('checksum', {}).get('checksum', ''),
                    'relation_name': model_data.get('relation_name', ''),
                    'language': model_data.get('language', 'sql'),
                }
                
                # Add raw and compiled code
                if 'raw_code' in model_data:
                    properties['raw_code'] = model_data['raw_code']
                if 'compiled_code' in model_data:
                    properties['compiled_code'] = model_data['compiled_code']
                
                # Add config details
                config = model_data.get('config', {})
                properties.update({
                    'enabled': config.get('enabled', True),
                    'tags': config.get('tags', []),
                    'meta': json.dumps(config.get('meta', {})),
                    'access': config.get('access', ''),
                })
                
                # Add version and constraints info
                if 'version' in model_data and model_data['version']:
                    properties['version'] = model_data['version']
                if 'constraints' in model_data:
                    properties['constraints'] = json.dumps(model_data['constraints'])
                
                # Add catalog information if available
                if catalog_nodes and model_id in catalog_nodes:
                    catalog_info = catalog_nodes[model_id]
                    properties.update({
                        'table_type': catalog_info.get('metadata', {}).get('type', ''),
                        'table_comment': catalog_info.get('metadata', {}).get('comment', ''),
                        'owner': catalog_info.get('metadata', {}).get('owner', ''),
                    })
                
                # Create the node
                session.run("""
                    MERGE (m:Model {unique_id: $unique_id})
                    SET m += $properties
                """, unique_id=model_id, properties=properties)
        
        logger.info(f"Created {len(models)} model nodes")
    
    def create_sources(self, sources: Dict[str, Any]):
        """Create source nodes with proper naming: source_name.identifier"""
        with self.driver.session() as session:
            for source_id, source_data in sources.items():
                # Create full name as source_name.identifier
                source_name = source_data.get('source_name', '')
                identifier = source_data.get('identifier', source_data.get('name', ''))
                full_name = f"{source_name}.{identifier}" if source_name and identifier else identifier
                
                properties = {
                    'unique_id': source_id,
                    'name': full_name,  # Changed to use full name
                    'identifier': identifier,
                    'resource_type': source_data.get('resource_type', ''),
                    'package_name': source_data.get('package_name', ''),
                    'source_name': source_name,
                    'database': source_data.get('database', ''),
                    'schema': source_data.get('schema', ''),
                    'description': source_data.get('description', ''),
                    'loader': source_data.get('loader', ''),
                    'source_description': source_data.get('source_description', ''),
                    'relation_name': source_data.get('relation_name', ''),
                }
                
                # Add freshness configuration
                freshness = source_data.get('freshness', {})
                if freshness:
                    properties['freshness_warn_after'] = json.dumps(freshness.get('warn_after', {}))
                    properties['freshness_error_after'] = json.dumps(freshness.get('error_after', {}))
                    properties['freshness_filter'] = freshness.get('filter', '')
                
                # Add columns information
                columns = source_data.get('columns', {})
                if columns:
                    properties['column_count'] = len(columns)
                    properties['columns'] = json.dumps(columns)
                
                # Add quoting and external info
                quoting = source_data.get('quoting', {})
                if quoting:
                    properties['quoting'] = json.dumps(quoting)
                
                if 'external' in source_data and source_data['external']:
                    properties['external'] = json.dumps(source_data['external'])
                
                session.run("""
                    MERGE (s:Source {unique_id: $unique_id})
                    SET s += $properties
                """, unique_id=source_id, properties=properties)
        
        logger.info(f"Created {len(sources)} source nodes")
    
    def create_seeds(self, seeds: Dict[str, Any]):
        """Create seed nodes"""
        with self.driver.session() as session:
            for seed_id, seed_data in seeds.items():
                properties = {
                    'unique_id': seed_id,
                    'name': seed_data.get('name', ''),
                    'resource_type': seed_data.get('resource_type', ''),
                    'package_name': seed_data.get('package_name', ''),
                    'path': seed_data.get('path', ''),
                    'original_file_path': seed_data.get('original_file_path', ''),
                    'database': seed_data.get('database', ''),
                    'schema': seed_data.get('schema', ''),
                    'alias': seed_data.get('alias', ''),
                    'checksum': seed_data.get('checksum', {}).get('checksum', ''),
                    'relation_name': seed_data.get('relation_name', ''),
                    'root_path': seed_data.get('root_path', ''),
                }
                
                # Add config details
                config = seed_data.get('config', {})
                properties.update({
                    'enabled': config.get('enabled', True),
                    'tags': config.get('tags', []),
                    'meta': json.dumps(config.get('meta', {})),
                    'materialized': config.get('materialized', 'seed'),
                    'delimiter': config.get('delimiter', ','),
                    'quote_columns': config.get('quote_columns'),
                })
                
                # Add column types if available
                column_types = config.get('column_types', {})
                if column_types:
                    properties['column_types'] = json.dumps(column_types)
                
                session.run("""
                    MERGE (seed:Seed {unique_id: $unique_id})
                    SET seed += $properties
                """, unique_id=seed_id, properties=properties)
        
        logger.info(f"Created {len(seeds)} seed nodes")
    
    def create_snapshots(self, snapshots: Dict[str, Any]):
        """Create snapshot nodes"""
        with self.driver.session() as session:
            for snapshot_id, snapshot_data in snapshots.items():
                properties = {
                    'unique_id': snapshot_id,
                    'name': snapshot_data.get('name', ''),
                    'resource_type': snapshot_data.get('resource_type', ''),
                    'package_name': snapshot_data.get('package_name', ''),
                    'path': snapshot_data.get('path', ''),
                    'original_file_path': snapshot_data.get('original_file_path', ''),
                    'database': snapshot_data.get('database', ''),
                    'schema': snapshot_data.get('schema', ''),
                    'alias': snapshot_data.get('alias', ''),
                    'checksum': snapshot_data.get('checksum', {}).get('checksum', ''),
                    'relation_name': snapshot_data.get('relation_name', ''),
                }
                
                # Add raw and compiled code
                if 'raw_code' in snapshot_data:
                    properties['raw_code'] = snapshot_data['raw_code']
                if 'compiled_code' in snapshot_data:
                    properties['compiled_code'] = snapshot_data['compiled_code']
                
                # Add snapshot-specific config
                config = snapshot_data.get('config', {})
                properties.update({
                    'enabled': config.get('enabled', True),
                    'tags': config.get('tags', []),
                    'meta': json.dumps(config.get('meta', {})),
                    'materialized': config.get('materialized', 'snapshot'),
                    'strategy': config.get('strategy', ''),
                    'unique_key': config.get('unique_key', ''),
                    'updated_at': config.get('updated_at', ''),
                    'target_schema': config.get('target_schema', ''),
                    'target_database': config.get('target_database', ''),
                })
                
                # Add check_cols for check strategy
                check_cols = config.get('check_cols', [])
                if check_cols:
                    properties['check_cols'] = json.dumps(check_cols)
                
                # Add snapshot meta column names if available
                snapshot_meta_column_names = config.get('snapshot_meta_column_names', {})
                if snapshot_meta_column_names:
                    properties['snapshot_meta_column_names'] = json.dumps(snapshot_meta_column_names)
                
                session.run("""
                    MERGE (snap:Snapshot {unique_id: $unique_id})
                    SET snap += $properties
                """, unique_id=snapshot_id, properties=properties)
        
        logger.info(f"Created {len(snapshots)} snapshot nodes")
    
    def create_tests(self, tests: Dict[str, Any]):
        """Create test nodes"""
        with self.driver.session() as session:
            for test_id, test_data in tests.items():
                properties = {
                    'unique_id': test_id,
                    'name': test_data.get('name', ''),
                    'resource_type': test_data.get('resource_type', ''),
                    'package_name': test_data.get('package_name', ''),
                    'path': test_data.get('path', ''),
                    'original_file_path': test_data.get('original_file_path', ''),
                    'test_metadata': json.dumps(test_data.get('test_metadata', {})),
                    'column_name': test_data.get('column_name', ''),
                    'file_key_name': test_data.get('file_key_name', ''),
                    'language': test_data.get('language', 'sql'),
                }
                
                # Add compiled code if available
                if 'compiled_code' in test_data:
                    properties['compiled_code'] = test_data['compiled_code']
                if 'raw_code' in test_data:
                    properties['raw_code'] = test_data['raw_code']
                
                # Add config details
                config = test_data.get('config', {})
                properties.update({
                    'enabled': config.get('enabled', True),
                    'tags': config.get('tags', []),
                    'meta': json.dumps(config.get('meta', {})),
                    'severity': config.get('severity', 'ERROR'),
                    'store_failures': config.get('store_failures'),
                    'store_failures_as': config.get('store_failures_as'),
                    'where_clause': config.get('where'),
                    'limit_clause': config.get('limit'),
                    'fail_calc': config.get('fail_calc', 'count(*)'),
                    'warn_if': config.get('warn_if', '!= 0'),
                    'error_if': config.get('error_if', '!= 0'),
                })
                
                # Add test metadata details
                test_metadata = test_data.get('test_metadata', {})
                if test_metadata:
                    properties.update({
                        'test_name': test_metadata.get('name', ''),
                        'test_kwargs': json.dumps(test_metadata.get('kwargs', {})),
                        'test_namespace': test_metadata.get('namespace', ''),
                    })
                
                session.run("""
                    MERGE (t:Test {unique_id: $unique_id})
                    SET t += $properties
                """, unique_id=test_id, properties=properties)
        
        logger.info(f"Created {len(tests)} test nodes")
    
    def create_macros(self, macros: Dict[str, Any]):
        """Create macro nodes"""
        with self.driver.session() as session:
            for macro_id, macro_data in macros.items():
                properties = {
                    'unique_id': macro_id,
                    'name': macro_data.get('name', ''),
                    'resource_type': macro_data.get('resource_type', ''),
                    'package_name': macro_data.get('package_name', ''),
                    'path': macro_data.get('path', ''),
                    'original_file_path': macro_data.get('original_file_path', ''),
                    'macro_sql': macro_data.get('macro_sql', ''),
                    'description': macro_data.get('description', ''),
                    'arguments': json.dumps(macro_data.get('arguments', [])),
                    'supported_languages': json.dumps(macro_data.get('supported_languages', [])),
                }
                
                # Add created_at timestamp if available
                if 'created_at' in macro_data:
                    properties['created_at'] = macro_data['created_at']
                
                # Add docs information
                docs = macro_data.get('docs', {})
                if docs:
                    properties.update({
                        'docs_show': docs.get('show', True),
                        'docs_node_color': docs.get('node_color', ''),
                    })
                
                session.run("""
                    MERGE (mac:Macro {unique_id: $unique_id})
                    SET mac += $properties
                """, unique_id=macro_id, properties=properties)
        
        logger.info(f"Created {len(macros)} macro nodes")
    
    def create_operations(self, operations: Dict[str, Any]):
        """Create operation nodes"""
        with self.driver.session() as session:
            for op_id, op_data in operations.items():
                properties = {
                    'unique_id': op_id,
                    'name': op_data.get('name', ''),
                    'resource_type': op_data.get('resource_type', ''),
                    'package_name': op_data.get('package_name', ''),
                    'path': op_data.get('path', ''),
                    'original_file_path': op_data.get('original_file_path', ''),
                    'database': op_data.get('database', ''),
                    'schema': op_data.get('schema', ''),
                    'raw_code': op_data.get('raw_code', ''),
                    'compiled_code': op_data.get('compiled_code', ''),
                    'index': op_data.get('index', 0),
                    'language': op_data.get('language', 'sql'),
                }
                
                # Add config details
                config = op_data.get('config', {})
                properties.update({
                    'enabled': config.get('enabled', True),
                    'tags': config.get('tags', []),
                    'meta': json.dumps(config.get('meta', {})),
                })
                
                # Add created_at timestamp if available
                if 'created_at' in op_data:
                    properties['created_at'] = op_data['created_at']
                
                session.run("""
                    MERGE (o:Operation {unique_id: $unique_id})
                    SET o += $properties
                """, unique_id=op_id, properties=properties)
        
        logger.info(f"Created {len(operations)} operation nodes")
    
    def create_dependencies(self, parent_map: Dict[str, List[str]], child_map: Dict[str, List[str]]):
        """Create dependency relationships using DEPENDS_ON for all types"""
        with self.driver.session() as session:
            # Create DEPENDS_ON relationships from parent_map
            for child, parents in parent_map.items():
                for parent in parents:
                    session.run("""
                        MATCH (parent) WHERE parent.unique_id = $parent_id
                        MATCH (child) WHERE child.unique_id = $child_id
                        MERGE (child)-[:DEPENDS_ON]->(parent)
                    """, parent_id=parent, child_id=child)
            
            logger.info(f"Created dependency relationships from parent_map")
    
    def create_ref_relationships(self, nodes: Dict[str, Any]):
        """Create REFERENCES relationships between models"""
        with self.driver.session() as session:
            for node_id, node_data in nodes.items():
                refs = node_data.get('refs', [])
                for ref in refs:
                    if isinstance(ref, dict):
                        ref_name = ref.get('name')
                        ref_package = ref.get('package')
                        ref_version = ref.get('version')
                    else:
                        ref_name = ref
                        ref_package = None
                        ref_version = None
                    
                    if ref_name:
                        # Find the referenced model
                        query = """
                            MATCH (referencing) WHERE referencing.unique_id = $referencing_id
                            MATCH (referenced:Model) WHERE referenced.name = $ref_name
                        """
                        params = {'referencing_id': node_id, 'ref_name': ref_name}
                        
                        # Add package filter if specified
                        if ref_package:
                            query += " AND referenced.package_name = $ref_package"
                            params['ref_package'] = ref_package
                        
                        # Add version filter if specified
                        if ref_version:
                            query += " AND referenced.version = $ref_version"
                            params['ref_version'] = ref_version
                        
                        query += " MERGE (referencing)-[:REFERENCES]->(referenced)"
                        
                        session.run(query, params)
        
        logger.info("Created REFERENCES relationships")
    
    def create_source_relationships(self, nodes: Dict[str, Any]):
        """Create DEPENDS_ON relationships to sources"""
        with self.driver.session() as session:
            for node_id, node_data in nodes.items():
                sources = node_data.get('sources', [])
                for source in sources:
                    if len(source) >= 2:
                        source_name, table_name = source[0], source[1]
                        # Use the new full name format: source_name.identifier
                        full_source_name = f"{source_name}.{table_name}"
                        session.run("""
                            MATCH (node) WHERE node.unique_id = $node_id
                            MATCH (source:Source) WHERE source.name = $full_source_name
                            MERGE (node)-[:DEPENDS_ON]->(source)
                        """, node_id=node_id, full_source_name=full_source_name)
        
        logger.info("Created DEPENDS_ON relationships to sources")
    
    def create_macro_relationships(self, nodes: Dict[str, Any]):
        """Create USES_MACRO relationships"""
        with self.driver.session() as session:
            for node_id, node_data in nodes.items():
                depends_on = node_data.get('depends_on', {})
                macros = depends_on.get('macros', [])
                for macro in macros:
                    session.run("""
                        MATCH (node) WHERE node.unique_id = $node_id
                        MATCH (macro:Macro) WHERE macro.unique_id = $macro_id
                        MERGE (node)-[:USES_MACRO]->(macro)
                    """, node_id=node_id, macro_id=macro)
        
        logger.info("Created USES_MACRO relationships")
    
    def create_test_relationships(self, tests: Dict[str, Any]):
        """Create TESTS relationships"""
        with self.driver.session() as session:
            for test_id, test_data in tests.items():
                # Link tests to the nodes they test via attached_node
                attached_node = test_data.get('attached_node')
                if attached_node:
                    session.run("""
                        MATCH (test:Test) WHERE test.unique_id = $test_id
                        MATCH (node) WHERE node.unique_id = $attached_node
                        MERGE (test)-[:TESTS]->(node)
                    """, test_id=test_id, attached_node=attached_node)
                
                # Also create DEPENDS_ON relationships based on refs and sources
                # These are already handled by create_dependencies, but we can add specific test logic here if needed
        
        logger.info("Created TESTS relationships")
    
    def load_dbt_to_neo4j_from_strings(self, manifest_str: str, catalog_str: Optional[str] = None):
        """Main method to load DBT data into Neo4j from JSON strings"""
        logger.info("Starting DBT to Neo4j load process from strings")
        
        # Load data from strings
        manifest_data, catalog_data = self.load_manifest_data_from_strings(manifest_str, catalog_str)
        
        # Clear database and create constraints
        self.clear_database()
        self.create_constraints()
        
        # Extract data sections
        nodes = manifest_data.get('nodes', {})
        sources = manifest_data.get('sources', {})
        macros = manifest_data.get('macros', {})
        parent_map = manifest_data.get('parent_map', {})
        child_map = manifest_data.get('child_map', {})
        
        # Separate different node types
        models = {k: v for k, v in nodes.items() if v.get('resource_type') == 'model'}
        tests = {k: v for k, v in nodes.items() if v.get('resource_type') == 'test'}
        operations = {k: v for k, v in nodes.items() if v.get('resource_type') == 'operation'}
        seeds = {k: v for k, v in nodes.items() if v.get('resource_type') == 'seed'}
        snapshots = {k: v for k, v in nodes.items() if v.get('resource_type') == 'snapshot'}
        
        # Create nodes
        self.create_models(models, catalog_data.get('nodes', {}))
        self.create_sources(sources)
        self.create_seeds(seeds)
        self.create_snapshots(snapshots)
        self.create_tests(tests)
        self.create_macros(macros)
        self.create_operations(operations)
        
        # Create relationships
        self.create_dependencies(parent_map, child_map)
        self.create_ref_relationships(nodes)
        self.create_source_relationships(nodes)
        self.create_macro_relationships(nodes)
        self.create_test_relationships(tests)
        
        logger.info("DBT to Neo4j load process completed successfully")
    
    def load_dbt_to_neo4j_from_files(self, manifest_path: str, catalog_path: Optional[str] = None):
        """Main method to load DBT data into Neo4j from files (kept for backward compatibility)"""
        logger.info("Starting DBT to Neo4j load process from files")
        
        # Load data from files
        manifest_data, catalog_data = self.load_manifest_data_from_files(manifest_path, catalog_path)
        
        # Clear database and create constraints
        self.clear_database()
        self.create_constraints()
        
        # Extract data sections
        nodes = manifest_data.get('nodes', {})
        sources = manifest_data.get('sources', {})
        macros = manifest_data.get('macros', {})
        parent_map = manifest_data.get('parent_map', {})
        child_map = manifest_data.get('child_map', {})
        
        # Separate different node types
        models = {k: v for k, v in nodes.items() if v.get('resource_type') == 'model'}
        tests = {k: v for k, v in nodes.items() if v.get('resource_type') == 'test'}
        operations = {k: v for k, v in nodes.items() if v.get('resource_type') == 'operation'}
        seeds = {k: v for k, v in nodes.items() if v.get('resource_type') == 'seed'}
        snapshots = {k: v for k, v in nodes.items() if v.get('resource_type') == 'snapshot'}
        
        # Create nodes
        self.create_models(models, catalog_data.get('nodes', {}))
        self.create_sources(sources)
        self.create_seeds(seeds)
        self.create_snapshots(snapshots)
        self.create_tests(tests)
        self.create_macros(macros)
        self.create_operations(operations)
        
        # Create relationships
        self.create_dependencies(parent_map, child_map)
        self.create_ref_relationships(nodes)
        self.create_source_relationships(nodes)
        self.create_macro_relationships(nodes)
        self.create_test_relationships(tests)
        
        logger.info("DBT to Neo4j load process completed successfully")
    
    def get_graph_stats(self):
        """Get statistics about the created graph"""
        with self.driver.session() as session:
            # Count nodes by type
            node_counts = session.run("""
                MATCH (n)
                RETURN labels(n)[0] as node_type, count(n) as count
                ORDER BY count DESC
            """).data()
            
            # Count relationships by type
            rel_counts = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as relationship_type, count(r) as count
                ORDER BY count DESC
            """).data()
            
            print("\n=== Graph Statistics ===")
            print("\nNode counts:")
            for record in node_counts:
                print(f"  {record['node_type']}: {record['count']}")
            
            print("\nRelationship counts:")
            for record in rel_counts:
                print(f"  {record['relationship_type']}: {record['count']}")

async def load_from_uploaded_files(loader: DBTNeo4jLoader, manifest_file, catalog_file=None):
    """Async function to load DBT data from uploaded file objects"""
    try:
        # Read file contents as strings
        manifest_str = await manifest_file.read()
        
        catalog_str = None
        if catalog_file:
            catalog_str = await catalog_file.read()
        
        # Load into Neo4j using the string method
        loader.load_dbt_to_neo4j_from_strings(manifest_str, catalog_str)
        
        # Print statistics
        loader.get_graph_stats()
        
        print("\n=== Load completed successfully! ===")
        
    except Exception as e:
        logger.error(f"Error during async file loading: {e}")
        raise

def main():
    """Main execution function (kept for backward compatibility)"""
    # Configuration
    NEO4J_URI = "neo4j://neo4j:7687"
    NEO4J_USERNAME = "neo4j"
    NEO4J_PASSWORD = "Testtest123"
    
    # File paths - update these to match your file locations
    MANIFEST_PATH = "manifest.json"
    CATALOG_PATH = "catalog.json"  # Optional
    
    # Initialize loader
    loader = DBTNeo4jLoader(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)
    
    try:
        # Load data into Neo4j using the file method
        loader.load_dbt_to_neo4j_from_files(MANIFEST_PATH, CATALOG_PATH)
        
        # Print statistics
        loader.get_graph_stats()
        
        print("\n=== Sample Queries ===")
        print("1. Find all models and their dependencies:")
        print("   MATCH (m:Model)-[:DEPENDS_ON]->(dep) RETURN m.name, dep.name, labels(dep)")
        print("\n2. Find models that depend on a specific source:")
        print("   MATCH (m:Model)-[:DEPENDS_ON]->(s:Source) WHERE s.name = 'raw.customers' RETURN m.name")
        print("\n3. Find all tests for a model:")
        print("   MATCH (t:Test)-[:TESTS]->(m:Model) WHERE m.name = 'stg_customers' RETURN t.name")
        print("\n4. Find the dependency chain for a model:")
        print("   MATCH path = (m:Model {name: 'customer_ltv'})-[:DEPENDS_ON*]->(dep) RETURN path")
        print("\n5. Find all seeds and what depends on them:")
        print("   MATCH (seed:Seed)<-[:DEPENDS_ON]-(dependent) RETURN seed.name, dependent.name, labels(dependent)")
        print("\n6. Find all snapshots and their dependencies:")
        print("   MATCH (snap:Snapshot)-[:DEPENDS_ON]->(dep) RETURN snap.name, dep.name, labels(dep)")
        print("\n7. Find models that use macros:")
        print("   MATCH (m:Model)-[:USES_MACRO]->(mac:Macro) RETURN m.name, mac.name")
        
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        raise
    finally:
        loader.close()


if __name__ == "__main__":
    main()