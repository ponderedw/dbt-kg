import json
import logging
from typing import Dict, Any, List, Optional
from falkordb import FalkorDB
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DBTFalkorDBLoader:
    """Load DBT manifest and catalog data into FalkorDB as a knowledge graph"""
    
    def __init__(self, host: str = 'falkordb', port: int = 6379, graph_name: str = 'dbt_graph',
                 username: str = None, password: str = None):
        """Initialize FalkorDB connection"""
        self.db = FalkorDB(host=host, port=port, username=username,
                           password=password)
        self.graph_name = graph_name
        self.graph = self.db.select_graph(graph_name)
        
    def close(self):
        """Close FalkorDB connection"""
        if self.db:
            self.db.close()
    
    def clear_database(self):
        """Clear all nodes and relationships"""
        try:
            self.graph.query("MATCH (n) DELETE n")
            logger.info("Database cleared")
        except Exception as e:
            logger.warning(f"Database clear failed (may be empty): {e}")
    
    def create_constraints(self):
        """Create constraints and indexes for better performance"""
        constraints = [
            "CREATE INDEX FOR (m:Model) ON (m.unique_id)",
            "CREATE INDEX FOR (s:Source) ON (s.unique_id)",
            "CREATE INDEX FOR (t:Test) ON (t.unique_id)",
            "CREATE INDEX FOR (mac:Macro) ON (mac.unique_id)",
            "CREATE INDEX FOR (o:Operation) ON (o.unique_id)",
            "CREATE INDEX FOR (seed:Seed) ON (seed.unique_id)",
            "CREATE INDEX FOR (snap:Snapshot) ON (snap.unique_id)",
        ]
        
        for constraint in constraints:
            try:
                self.graph.query(constraint)
            except Exception as e:
                logger.warning(f"Index creation failed (may already exist): {e}")
        
        logger.info("Indexes created")
    
    def load_manifest_data_from_strings(self, manifest_str: str, catalog_str: Optional[str] = None):
        """Load manifest and optional catalog data from strings"""
        # Parse manifest JSON string
        manifest_data = json.loads(manifest_str)
        
        # Parse catalog JSON string if provided
        catalog_data = {}
        if catalog_str:
            catalog_data = json.loads(catalog_str)
        
        return manifest_data, catalog_data
    
    def load_manifest_data(self, manifest_path: str, catalog_path: str = None):
        """Load manifest and optional catalog data from file paths"""
        # Load manifest
        with open(manifest_path, 'r') as f:
            manifest_data = json.load(f)
        
        # Load catalog if provided
        catalog_data = {}
        if catalog_path and Path(catalog_path).exists():
            with open(catalog_path, 'r') as f:
                catalog_data = json.load(f)
        
        return manifest_data, catalog_data
    
    def _escape_string(self, value):
        """Escape string values for FalkorDB queries"""
        if value is None:
            return None
        if isinstance(value, str):
            # Replace all problematic characters
            escaped = (value
                      .replace('\\', '\\\\')  # Escape backslashes first
                      .replace("'", "\\'")    # Escape single quotes
                      .replace('"', '\\"')    # Escape double quotes  
                      .replace('\n', '\\n')   # Escape newlines
                      .replace('\r', '\\r')   # Escape carriage returns
                      .replace('\t', '\\t')   # Escape tabs
                      .replace('\b', '\\b')   # Escape backspaces
                      .replace('\f', '\\f'))  # Escape form feeds
            return escaped
        return value
    
    def _format_property_value(self, key, value):
        """Format property value for query string"""
        if value is None:
            return None  # Will be filtered out
        elif isinstance(value, str):
            return f"{key}: '{self._escape_string(value)}'"
        elif isinstance(value, bool):
            return f"{key}: {str(value).lower()}"
        elif isinstance(value, (int, float)):
            return f"{key}: {value}"
        elif isinstance(value, (list, dict)):
            # Convert lists and dicts to JSON strings
            json_str = json.dumps(value)
            return f"{key}: '{self._escape_string(json_str)}'"
        else:
            # Convert other types to string and escape
            return f"{key}: '{self._escape_string(str(value))}'"
    
    def create_models(self, models: Dict[str, Any], catalog_nodes: Dict[str, Any] = None):
        """Create model nodes"""
        for model_id, model_data in models.items():
            # Extract basic properties
            properties = {
                'unique_id': self._escape_string(model_id),
                'name': self._escape_string(model_data.get('name', '')),
                'resource_type': self._escape_string(model_data.get('resource_type', '')),
                'package_name': self._escape_string(model_data.get('package_name', '')),
                'path': self._escape_string(model_data.get('path', '')),
                'original_file_path': self._escape_string(model_data.get('original_file_path', '')),
                'database': self._escape_string(model_data.get('database', '')),
                'schema': self._escape_string(model_data.get('schema', '')),
                'alias': self._escape_string(model_data.get('alias', '')),
                'materialized': self._escape_string(model_data.get('config', {}).get('materialized', '')),
                'description': self._escape_string(model_data.get('description', '')),
                'checksum': self._escape_string(model_data.get('checksum', {}).get('checksum', '')),
                'relation_name': self._escape_string(model_data.get('relation_name', '')),
                'language': self._escape_string(model_data.get('language', 'sql')),
            }
            
            # Add config details
            config = model_data.get('config', {})
            properties.update({
                'enabled': config.get('enabled', True),
                'tags': str(config.get('tags', [])),
                'meta': json.dumps(config.get('meta', {})),
                'access': self._escape_string(config.get('access', '')),
            })
            
            # Add catalog information if available
            if catalog_nodes and model_id in catalog_nodes:
                catalog_info = catalog_nodes[model_id]
                properties.update({
                    'table_type': self._escape_string(catalog_info.get('metadata', {}).get('type', '')),
                    'table_comment': self._escape_string(catalog_info.get('metadata', {}).get('comment', '')),
                    'owner': self._escape_string(catalog_info.get('metadata', {}).get('owner', '')),
                })
            
            # Build the query
            prop_strings = []
            for k, v in properties.items():
                formatted = self._format_property_value(k, v)
                if formatted is not None:
                    prop_strings.append(formatted)
            
            props_str = ", ".join(prop_strings)
            query = f"CREATE (m:Model {{{props_str}}})"
            
            try:
                self.graph.query(query)
            except Exception as e:
                logger.error(f"Error creating model {model_id}: {e}")
        
        logger.info(f"Created {len(models)} model nodes")
    
    def create_sources(self, sources: Dict[str, Any]):
        """Create source nodes with proper naming: source_name.identifier"""
        for source_id, source_data in sources.items():
            # Create full name as source_name.identifier
            source_name = source_data.get('source_name', '')
            identifier = source_data.get('identifier', source_data.get('name', ''))
            full_name = f"{source_name}.{identifier}" if source_name and identifier else identifier
            
            properties = {
                'unique_id': self._escape_string(source_id),
                'name': self._escape_string(full_name),
                'identifier': self._escape_string(identifier),
                'resource_type': self._escape_string(source_data.get('resource_type', '')),
                'package_name': self._escape_string(source_data.get('package_name', '')),
                'source_name': self._escape_string(source_name),
                'database': self._escape_string(source_data.get('database', '')),
                'schema': self._escape_string(source_data.get('schema', '')),
                'description': self._escape_string(source_data.get('description', '')),
                'loader': self._escape_string(source_data.get('loader', '')),
                'relation_name': self._escape_string(source_data.get('relation_name', '')),
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
            
            # Build the query
            prop_strings = []
            for k, v in properties.items():
                formatted = self._format_property_value(k, v)
                if formatted is not None:
                    prop_strings.append(formatted)
            
            props_str = ", ".join(prop_strings)
            query = f"CREATE (s:Source {{{props_str}}})"
            
            try:
                self.graph.query(query)
            except Exception as e:
                logger.error(f"Error creating source {source_id}: {e}")
        
        logger.info(f"Created {len(sources)} source nodes")
    
    def create_seeds(self, seeds: Dict[str, Any]):
        """Create seed nodes"""
        for seed_id, seed_data in seeds.items():
            properties = {
                'unique_id': self._escape_string(seed_id),
                'name': self._escape_string(seed_data.get('name', '')),
                'resource_type': self._escape_string(seed_data.get('resource_type', '')),
                'package_name': self._escape_string(seed_data.get('package_name', '')),
                'path': self._escape_string(seed_data.get('path', '')),
                'database': self._escape_string(seed_data.get('database', '')),
                'schema': self._escape_string(seed_data.get('schema', '')),
                'alias': self._escape_string(seed_data.get('alias', '')),
                'relation_name': self._escape_string(seed_data.get('relation_name', '')),
            }
            
            # Add config details
            config = seed_data.get('config', {})
            properties.update({
                'enabled': config.get('enabled', True),
                'tags': str(config.get('tags', [])),
                'materialized': self._escape_string(config.get('materialized', 'seed')),
                'delimiter': self._escape_string(config.get('delimiter', ',')),
            })
            
            # Build the query
            prop_strings = []
            for k, v in properties.items():
                formatted = self._format_property_value(k, v)
                if formatted is not None:
                    prop_strings.append(formatted)
            
            props_str = ", ".join(prop_strings)
            query = f"CREATE (seed:Seed {{{props_str}}})"
            
            try:
                self.graph.query(query)
            except Exception as e:
                logger.error(f"Error creating seed {seed_id}: {e}")
        
        logger.info(f"Created {len(seeds)} seed nodes")
    
    def create_snapshots(self, snapshots: Dict[str, Any]):
        """Create snapshot nodes"""
        for snapshot_id, snapshot_data in snapshots.items():
            properties = {
                'unique_id': self._escape_string(snapshot_id),
                'name': self._escape_string(snapshot_data.get('name', '')),
                'resource_type': self._escape_string(snapshot_data.get('resource_type', '')),
                'package_name': self._escape_string(snapshot_data.get('package_name', '')),
                'path': self._escape_string(snapshot_data.get('path', '')),
                'database': self._escape_string(snapshot_data.get('database', '')),
                'schema': self._escape_string(snapshot_data.get('schema', '')),
                'alias': self._escape_string(snapshot_data.get('alias', '')),
                'relation_name': self._escape_string(snapshot_data.get('relation_name', '')),
            }
            
            # Add snapshot-specific config
            config = snapshot_data.get('config', {})
            properties.update({
                'enabled': config.get('enabled', True),
                'tags': str(config.get('tags', [])),
                'materialized': self._escape_string(config.get('materialized', 'snapshot')),
                'strategy': self._escape_string(config.get('strategy', '')),
                'unique_key': self._escape_string(config.get('unique_key', '')),
                'updated_at': self._escape_string(config.get('updated_at', '')),
            })
            
            # Build the query
            prop_strings = []
            for k, v in properties.items():
                formatted = self._format_property_value(k, v)
                if formatted is not None:
                    prop_strings.append(formatted)
            
            props_str = ", ".join(prop_strings)
            query = f"CREATE (snap:Snapshot {{{props_str}}})"
            
            try:
                self.graph.query(query)
            except Exception as e:
                logger.error(f"Error creating snapshot {snapshot_id}: {e}")
        
        logger.info(f"Created {len(snapshots)} snapshot nodes")
    
    def create_tests(self, tests: Dict[str, Any]):
        """Create test nodes"""
        for test_id, test_data in tests.items():
            properties = {
                'unique_id': self._escape_string(test_id),
                'name': self._escape_string(test_data.get('name', '')),
                'resource_type': self._escape_string(test_data.get('resource_type', '')),
                'package_name': self._escape_string(test_data.get('package_name', '')),
                'path': self._escape_string(test_data.get('path', '')),
                'column_name': self._escape_string(test_data.get('column_name', '')),
                'language': self._escape_string(test_data.get('language', 'sql')),
            }
            
            # Add config and test metadata
            config = test_data.get('config', {})
            properties.update({
                'enabled': config.get('enabled', True),
                'tags': str(config.get('tags', [])),
                'severity': self._escape_string(config.get('severity', 'ERROR')),
            })
            
            test_metadata = test_data.get('test_metadata', {})
            if test_metadata:
                properties.update({
                    'test_name': self._escape_string(test_metadata.get('name', '')),
                    'test_kwargs': json.dumps(test_metadata.get('kwargs', {})),
                })
            
            # Build the query
            prop_strings = []
            for k, v in properties.items():
                formatted = self._format_property_value(k, v)
                if formatted is not None:
                    prop_strings.append(formatted)
            
            props_str = ", ".join(prop_strings)
            query = f"CREATE (t:Test {{{props_str}}})"
            
            try:
                self.graph.query(query)
            except Exception as e:
                logger.error(f"Error creating test {test_id}: {e}")
        
        logger.info(f"Created {len(tests)} test nodes")
    
    def create_macros(self, macros: Dict[str, Any]):
        """Create macro nodes"""
        for macro_id, macro_data in macros.items():
            properties = {
                'unique_id': self._escape_string(macro_id),
                'name': self._escape_string(macro_data.get('name', '')),
                'resource_type': self._escape_string(macro_data.get('resource_type', '')),
                'package_name': self._escape_string(macro_data.get('package_name', '')),
                'path': self._escape_string(macro_data.get('path', '')),
                'description': self._escape_string(macro_data.get('description', '')),
                'arguments': json.dumps(macro_data.get('arguments', [])),
            }
            
            # Build the query
            prop_strings = []
            for k, v in properties.items():
                formatted = self._format_property_value(k, v)
                if formatted is not None:
                    prop_strings.append(formatted)
            
            props_str = ", ".join(prop_strings)
            query = f"CREATE (mac:Macro {{{props_str}}})"
            
            try:
                self.graph.query(query)
            except Exception as e:
                logger.error(f"Error creating macro {macro_id}: {e}")
        
        logger.info(f"Created {len(macros)} macro nodes")
    
    def create_operations(self, operations: Dict[str, Any]):
        """Create operation nodes"""
        for op_id, op_data in operations.items():
            properties = {
                'unique_id': self._escape_string(op_id),
                'name': self._escape_string(op_data.get('name', '')),
                'resource_type': self._escape_string(op_data.get('resource_type', '')),
                'package_name': self._escape_string(op_data.get('package_name', '')),
                'path': self._escape_string(op_data.get('path', '')),
                'database': self._escape_string(op_data.get('database', '')),
                'schema': self._escape_string(op_data.get('schema', '')),
                'language': self._escape_string(op_data.get('language', 'sql')),
            }
            
            # Build the query
            prop_strings = []
            for k, v in properties.items():
                formatted = self._format_property_value(k, v)
                if formatted is not None:
                    prop_strings.append(formatted)
            
            props_str = ", ".join(prop_strings)
            query = f"CREATE (o:Operation {{{props_str}}})"
            
            try:
                self.graph.query(query)
            except Exception as e:
                logger.error(f"Error creating operation {op_id}: {e}")
        
        logger.info(f"Created {len(operations)} operation nodes")
    
    def create_dependencies(self, parent_map: Dict[str, List[str]], child_map: Dict[str, List[str]]):
        """Create dependency relationships using DEPENDS_ON for all types"""
        dependency_count = 0
        for child, parents in parent_map.items():
            for parent in parents:
                query = f"""
                    MATCH (parent) WHERE parent.unique_id = '{self._escape_string(parent)}'
                    MATCH (child) WHERE child.unique_id = '{self._escape_string(child)}'
                    CREATE (child)-[:DEPENDS_ON]->(parent)
                """
                try:
                    self.graph.query(query)
                    dependency_count += 1
                except Exception as e:
                    logger.error(f"Error creating dependency {child} -> {parent}: {e}")
        
        logger.info(f"Created {dependency_count} dependency relationships")
    
    def create_ref_relationships(self, nodes: Dict[str, Any]):
        """Create REFERENCES relationships between models"""
        ref_count = 0
        for node_id, node_data in nodes.items():
            refs = node_data.get('refs', [])
            for ref in refs:
                ref_name = ref.get('name') if isinstance(ref, dict) else ref
                if ref_name:
                    query = f"""
                        MATCH (referencing) WHERE referencing.unique_id = '{self._escape_string(node_id)}'
                        MATCH (referenced:Model) WHERE referenced.name = '{self._escape_string(ref_name)}'
                        CREATE (referencing)-[:REFERENCES]->(referenced)
                    """
                    try:
                        self.graph.query(query)
                        ref_count += 1
                    except Exception as e:
                        logger.error(f"Error creating reference {node_id} -> {ref_name}: {e}")
        
        logger.info(f"Created {ref_count} REFERENCES relationships")
    
    def create_source_relationships(self, nodes: Dict[str, Any]):
        """Create DEPENDS_ON relationships to sources"""
        source_dep_count = 0
        for node_id, node_data in nodes.items():
            sources = node_data.get('sources', [])
            for source in sources:
                if len(source) >= 2:
                    source_name, table_name = source[0], source[1]
                    full_source_name = f"{source_name}.{table_name}"
                    query = f"""
                        MATCH (node) WHERE node.unique_id = '{self._escape_string(node_id)}'
                        MATCH (source:Source) WHERE source.name = '{self._escape_string(full_source_name)}'
                        CREATE (node)-[:DEPENDS_ON]->(source)
                    """
                    try:
                        self.graph.query(query)
                        source_dep_count += 1
                    except Exception as e:
                        logger.error(f"Error creating source dependency {node_id} -> {full_source_name}: {e}")
        
        logger.info(f"Created {source_dep_count} DEPENDS_ON relationships to sources")
    
    def create_macro_relationships(self, nodes: Dict[str, Any]):
        """Create USES_MACRO relationships"""
        macro_count = 0
        for node_id, node_data in nodes.items():
            depends_on = node_data.get('depends_on', {})
            macros = depends_on.get('macros', [])
            for macro in macros:
                query = f"""
                    MATCH (node) WHERE node.unique_id = '{self._escape_string(node_id)}'
                    MATCH (macro:Macro) WHERE macro.unique_id = '{self._escape_string(macro)}'
                    CREATE (node)-[:USES_MACRO]->(macro)
                """
                try:
                    self.graph.query(query)
                    macro_count += 1
                except Exception as e:
                    logger.error(f"Error creating macro usage {node_id} -> {macro}: {e}")
        
        logger.info(f"Created {macro_count} USES_MACRO relationships")
    
    def create_test_relationships(self, tests: Dict[str, Any]):
        """Create TESTS relationships"""
        test_count = 0
        for test_id, test_data in tests.items():
            # Link tests to the nodes they test via attached_node
            attached_node = test_data.get('attached_node')
            if attached_node:
                query = f"""
                    MATCH (test:Test) WHERE test.unique_id = '{self._escape_string(test_id)}'
                    MATCH (node) WHERE node.unique_id = '{self._escape_string(attached_node)}'
                    CREATE (test)-[:TESTS]->(node)
                """
                try:
                    self.graph.query(query)
                    test_count += 1
                except Exception as e:
                    logger.error(f"Error creating test relationship {test_id} -> {attached_node}: {e}")
        
        logger.info(f"Created {test_count} TESTS relationships")
    
    def load_dbt_to_falkordb_from_strings(self, manifest_str: str, catalog_str: Optional[str] = None):
        """Main method to load DBT data into FalkorDB from string content"""
        logger.info("Starting DBT to FalkorDB load process from strings")
        
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
        
        logger.info("DBT to FalkorDB load process completed successfully")
    
    def load_dbt_to_falkordb(self, manifest_path: str, catalog_path: str = None):
        """Main method to load DBT data into FalkorDB from file paths"""
        logger.info("Starting DBT to FalkorDB load process")
        
        # Load data
        manifest_data, catalog_data = self.load_manifest_data(manifest_path, catalog_path)
        
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
        
        logger.info("DBT to FalkorDB load process completed successfully")
    
    def get_graph_stats(self):
        """Get statistics about the created graph"""
        try:
            # Count nodes by type
            node_result = self.graph.query("""
                MATCH (n)
                RETURN labels(n)[0] as node_type, count(n) as count
                ORDER BY count DESC
            """)
            
            # Count relationships by type
            rel_result = self.graph.query("""
                MATCH ()-[r]->()
                RETURN type(r) as relationship_type, count(r) as count
                ORDER BY count DESC
            """)
            
            print("\n=== Graph Statistics ===")
            print("\nNode counts:")
            for record in node_result.result_set:
                print(f"  {record[0]}: {record[1]}")
            
            print("\nRelationship counts:")
            for record in rel_result.result_set:
                print(f"  {record[0]}: {record[1]}")
                
        except Exception as e:
            logger.error(f"Error getting graph statistics: {e}")