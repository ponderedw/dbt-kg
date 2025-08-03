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
                
                # Add config details
                config = model_data.get('config', {})
                properties.update({
                    'enabled': config.get('enabled', True),
                    'tags': config.get('tags', []),
                    'meta': json.dumps(config.get('meta', {})),
                    'access': config.get('access', ''),
                })
                
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
                    'name': full_name,
                    'identifier': identifier,
                    'resource_type': source_data.get('resource_type', ''),
                    'package_name': source_data.get('package_name', ''),
                    'source_name': source_name,
                    'database': source_data.get('database', ''),
                    'schema': source_data.get('schema', ''),
                    'description': source_data.get('description', ''),
                    'loader': source_data.get('loader', ''),
                    'relation_name': source_data.get('relation_name', ''),
                }
                
                # Add freshness and columns info
                freshness = source_data.get('freshness', {})
                if freshness:
                    properties['freshness_warn_after'] = json.dumps(freshness.get('warn_after', {}))
                    properties['freshness_error_after'] = json.dumps(freshness.get('error_after', {}))
                
                columns = source_data.get('columns', {})
                if columns:
                    properties['column_count'] = len(columns)
                    properties['columns'] = json.dumps(columns)
                
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
                    'database': seed_data.get('database', ''),
                    'schema': seed_data.get('schema', ''),
                    'alias': seed_data.get('alias', ''),
                    'relation_name': seed_data.get('relation_name', ''),
                }
                
                # Add config details
                config = seed_data.get('config', {})
                properties.update({
                    'enabled': config.get('enabled', True),
                    'tags': config.get('tags', []),
                    'materialized': config.get('materialized', 'seed'),
                    'delimiter': config.get('delimiter', ','),
                })
                
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
                    'database': snapshot_data.get('database', ''),
                    'schema': snapshot_data.get('schema', ''),
                    'alias': snapshot_data.get('alias', ''),
                    'relation_name': snapshot_data.get('relation_name', ''),
                }
                
                # Add snapshot-specific config
                config = snapshot_data.get('config', {})
                properties.update({
                    'enabled': config.get('enabled', True),
                    'tags': config.get('tags', []),
                    'materialized': config.get('materialized', 'snapshot'),
                    'strategy': config.get('strategy', ''),
                    'unique_key': config.get('unique_key', ''),
                    'updated_at': config.get('updated_at', ''),
                })
                
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
                    'column_name': test_data.get('column_name', ''),
                    'language': test_data.get('language', 'sql'),
                }
                
                # Add config and test metadata
                config = test_data.get('config', {})
                properties.update({
                    'enabled': config.get('enabled', True),
                    'tags': config.get('tags', []),
                    'severity': config.get('severity', 'ERROR'),
                })
                
                test_metadata = test_data.get('test_metadata', {})
                if test_metadata:
                    properties.update({
                        'test_name': test_metadata.get('name', ''),
                        'test_kwargs': json.dumps(test_metadata.get('kwargs', {})),
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
                    'description': macro_data.get('description', ''),
                    'arguments': json.dumps(macro_data.get('arguments', [])),
                }
                
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
                    'database': op_data.get('database', ''),
                    'schema': op_data.get('schema', ''),
                    'language': op_data.get('language', 'sql'),
                }
                
                session.run("""
                    MERGE (o:Operation {unique_id: $unique_id})
                    SET o += $properties
                """, unique_id=op_id, properties=properties)
        
        logger.info(f"Created {len(operations)} operation nodes")
    
    def create_dependencies(self, parent_map: Dict[str, List[str]], child_map: Dict[str, List[str]]):
        """Create dependency relationships"""
        with self.driver.session() as session:
            dependency_count = 0
            for child, parents in parent_map.items():
                for parent in parents:
                    session.run("""
                        MATCH (parent) WHERE parent.unique_id = $parent_id
                        MATCH (child) WHERE child.unique_id = $child_id
                        MERGE (child)-[:DEPENDS_ON]->(parent)
                    """, parent_id=parent, child_id=child)
                    dependency_count += 1
            
            logger.info(f"Created {dependency_count} dependency relationships")
    
    def create_ref_relationships(self, nodes: Dict[str, Any]):
        """Create REFERENCES relationships between models"""
        with self.driver.session() as session:
            ref_count = 0
            for node_id, node_data in nodes.items():
                refs = node_data.get('refs', [])
                for ref in refs:
                    ref_name = ref.get('name') if isinstance(ref, dict) else ref
                    if ref_name:
                        session.run("""
                            MATCH (referencing) WHERE referencing.unique_id = $referencing_id
                            MATCH (referenced:Model) WHERE referenced.name = $ref_name
                            MERGE (referencing)-[:REFERENCES]->(referenced)
                        """, referencing_id=node_id, ref_name=ref_name)
                        ref_count += 1
            
            logger.info(f"Created {ref_count} REFERENCES relationships")
    
    def create_source_relationships(self, nodes: Dict[str, Any]):
        """Create DEPENDS_ON relationships to sources"""
        with self.driver.session() as session:
            source_dep_count = 0
            for node_id, node_data in nodes.items():
                sources = node_data.get('sources', [])
                for source in sources:
                    if len(source) >= 2:
                        source_name, table_name = source[0], source[1]
                        full_source_name = f"{source_name}.{table_name}"
                        session.run("""
                            MATCH (node) WHERE node.unique_id = $node_id
                            MATCH (source:Source) WHERE source.name = $full_source_name
                            MERGE (node)-[:DEPENDS_ON]->(source)
                        """, node_id=node_id, full_source_name=full_source_name)
                        source_dep_count += 1
            
            logger.info(f"Created {source_dep_count} source dependency relationships")
    
    def create_macro_relationships(self, nodes: Dict[str, Any]):
        """Create USES_MACRO relationships"""
        with self.driver.session() as session:
            macro_count = 0
            for node_id, node_data in nodes.items():
                depends_on = node_data.get('depends_on', {})
                macros = depends_on.get('macros', [])
                for macro in macros:
                    session.run("""
                        MATCH (node) WHERE node.unique_id = $node_id
                        MATCH (macro:Macro) WHERE macro.unique_id = $macro_id
                        MERGE (node)-[:USES_MACRO]->(macro)
                    """, node_id=node_id, macro_id=macro)
                    macro_count += 1
            
            logger.info(f"Created {macro_count} USES_MACRO relationships")
    
    def create_test_relationships(self, tests: Dict[str, Any]):
        """Create TESTS relationships"""
        with self.driver.session() as session:
            test_count = 0
            for test_id, test_data in tests.items():
                attached_node = test_data.get('attached_node')
                if attached_node:
                    session.run("""
                        MATCH (test:Test) WHERE test.unique_id = $test_id
                        MATCH (node) WHERE node.unique_id = $attached_node
                        MERGE (test)-[:TESTS]->(node)
                    """, test_id=test_id, attached_node=attached_node)
                    test_count += 1
            
            logger.info(f"Created {test_count} TESTS relationships")
    
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
        """Main method to load DBT data into Neo4j from files"""
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