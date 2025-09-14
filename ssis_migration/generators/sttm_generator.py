"""
STTM Generator - Creates Source-to-Target Mappings from SSIS packages
"""

import logging
from typing import List, Dict, Any, Optional
import re

from ..models import (
    SSISPackage, SourceTargetMapping, SSISDataType, 
    SSIS_TO_DATABRICKS_TYPE_MAPPING, DatabricksDataType
)


class STTMGenerator:
    """Generates Source-to-Target Mappings from parsed SSIS packages"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def generate_mappings(self, package: SSISPackage) -> List[SourceTargetMapping]:
        """
        Generate source-to-target mappings for a SSIS package
        
        Args:
            package: Parsed SSIS package
            
        Returns:
            List of SourceTargetMapping objects
        """
        self.logger.info(f"Generating mappings for package: {package.name}")
        
        mappings = []
        
        # Process each data flow in the package
        for data_flow in package.data_flows:
            flow_mappings = self._generate_dataflow_mappings(data_flow, package)
            mappings.extend(flow_mappings)
            
        self.logger.info(f"Generated {len(mappings)} mappings for package: {package.name}")
        return mappings
        
    def _generate_dataflow_mappings(self, data_flow, package: SSISPackage) -> List[SourceTargetMapping]:
        """Generate mappings for a single data flow"""
        mappings = []
        
        # Identify source and destination components
        sources = self._identify_source_components(data_flow.components)
        destinations = self._identify_destination_components(data_flow.components)
        transformations = self._identify_transformation_components(data_flow.components)
        
        # For each destination, trace back to sources
        for dest in destinations:
            dest_mappings = self._trace_mappings_to_destination(
                dest, sources, transformations, data_flow.paths, package
            )
            mappings.extend(dest_mappings)
            
        return mappings
        
    def _identify_source_components(self, components: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Identify source components (OLE DB Source, Excel Source, etc.)"""
        source_types = [
            'Microsoft.OLEDBSource',
            'Microsoft.ExcelSource', 
            'Microsoft.FlatFileSource',
            'Microsoft.XMLSource',
            'Microsoft.SqlServerCompactSource'
        ]
        
        sources = []
        for component in components:
            if any(source_type in component.get('componentClassID', '') for source_type in source_types):
                sources.append(component)
                
        return sources
        
    def _identify_destination_components(self, components: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Identify destination components"""
        dest_types = [
            'Microsoft.OLEDBDestination',
            'Microsoft.SqlServerDestination',
            'Microsoft.FlatFileDestination'
        ]
        
        destinations = []
        for component in components:
            if any(dest_type in component.get('componentClassID', '') for dest_type in dest_types):
                destinations.append(component)
                
        return destinations
        
    def _identify_transformation_components(self, components: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Identify transformation components"""
        transform_types = [
            'Microsoft.DerivedColumn',
            'Microsoft.ConditionalSplit',
            'Microsoft.Lookup',
            'Microsoft.Aggregate',
            'Microsoft.DataConversion',
            'Microsoft.Sort',
            'Microsoft.UnionAll',
            'Microsoft.Merge'
        ]
        
        transformations = []
        for component in components:
            if any(transform_type in component.get('componentClassID', '') for transform_type in transform_types):
                transformations.append(component)
                
        return transformations
        
    def _trace_mappings_to_destination(self, destination: Dict[str, Any], sources: List[Dict[str, Any]], 
                                     transformations: List[Dict[str, Any]], paths: List[Dict[str, Any]],
                                     package: SSISPackage) -> List[SourceTargetMapping]:
        """Trace column mappings from sources to a destination"""
        mappings = []
        
        # Get destination table name
        dest_table = self._extract_destination_table(destination)
        
        # Get destination columns
        dest_columns = self._extract_destination_columns(destination)
        
        # For each destination column, trace back to source
        for dest_col in dest_columns:
            source_mapping = self._trace_column_lineage(
                dest_col, destination, sources, transformations, paths
            )
            
            if source_mapping:
                # Convert to SourceTargetMapping
                mapping = SourceTargetMapping(
                    source_table=source_mapping.get('source_table'),
                    source_file=source_mapping.get('source_file'),
                    source_column=source_mapping.get('source_column', ''),
                    target_table=dest_table,
                    target_column=dest_col.get('name', ''),
                    data_type=self._convert_data_type(dest_col.get('dataType', '')),
                    transformation_rule=source_mapping.get('transformation_rule'),
                    validation_name=source_mapping.get('validation_name'),
                    transformation_name=source_mapping.get('transformation_name'),
                    is_derived=source_mapping.get('is_derived', False)
                )
                mappings.append(mapping)
                
        return mappings
        
    def _extract_destination_table(self, destination: Dict[str, Any]) -> str:
        """Extract destination table name from component"""
        # Look for table name in properties
        properties = destination.get('properties', {})
        
        # Common property names for table/destination
        table_props = ['OpenRowset', 'TableOrViewName', 'AccessMode']
        
        for prop_name in table_props:
            if prop_name in properties:
                table_name = properties[prop_name]
                if table_name and table_name != '':
                    return self._clean_table_name(table_name)
                    
        # Fallback to component name
        return destination.get('name', 'Unknown_Table')
        
    def _extract_destination_columns(self, destination: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract destination columns from component"""
        columns = []
        
        # Look in inputs (destination components have inputs, not outputs)
        for input_def in destination.get('inputs', []):
            columns.extend(input_def.get('columns', []))
            
        return columns
        
    def _trace_column_lineage(self, dest_column: Dict[str, Any], destination: Dict[str, Any],
                            sources: List[Dict[str, Any]], transformations: List[Dict[str, Any]],
                            paths: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Trace a destination column back to its source"""
        
        # Start with the destination column lineage ID
        lineage_id = dest_column.get('lineageId', '')
        if not lineage_id:
            return None
            
        # Build component graph from paths
        component_graph = self._build_component_graph(paths)
        
        # Find the path to destination
        dest_id = destination.get('id', '')
        upstream_components = self._get_upstream_components(dest_id, component_graph)
        
        # Trace through transformations
        current_lineage = lineage_id
        transformation_rules = []
        transformation_names = []
        is_derived = False
        
        for comp_id in reversed(upstream_components):
            # Find component by ID
            component = self._find_component_by_id(comp_id, transformations + sources)
            if not component:
                continue
                
            # Check if this is a transformation that affects our column
            if 'Microsoft.DerivedColumn' in component.get('componentClassID', ''):
                # Check if this derived column affects our lineage
                derived_info = self._check_derived_column(current_lineage, component)
                if derived_info:
                    transformation_rules.append(derived_info['expression'])
                    transformation_names.append('Derived Column')
                    current_lineage = derived_info.get('source_lineage', current_lineage)
                    is_derived = True
                    
            elif 'Microsoft.DataConversion' in component.get('componentClassID', ''):
                # Data conversion transformation
                conversion_info = self._check_data_conversion(current_lineage, component)
                if conversion_info:
                    transformation_rules.append(f"Convert to {conversion_info['target_type']}")
                    transformation_names.append('Data Conversion')
                    current_lineage = conversion_info.get('source_lineage', current_lineage)
                    
            elif 'Microsoft.Lookup' in component.get('componentClassID', ''):
                # Lookup transformation
                lookup_info = self._check_lookup_transformation(current_lineage, component)
                if lookup_info:
                    transformation_rules.append(f"Lookup: {lookup_info['lookup_table']}")
                    transformation_names.append('Lookup')
                    
        # Find the source component and column
        source_info = self._find_source_column(current_lineage, sources)
        
        if source_info:
            return {
                'source_table': source_info.get('source_table'),
                'source_file': source_info.get('source_file'),
                'source_column': source_info.get('source_column'),
                'transformation_rule': ' | '.join(transformation_rules) if transformation_rules else None,
                'transformation_name': ' | '.join(transformation_names) if transformation_names else None,
                'validation_name': None,  # TODO: Extract validation rules
                'is_derived': is_derived
            }
            
        return None
        
    def _build_component_graph(self, paths: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Build a graph of component connections"""
        graph = {}
        
        for path in paths:
            start_id = path.get('startId', '')
            end_id = path.get('endId', '')
            
            if end_id not in graph:
                graph[end_id] = []
            graph[end_id].append(start_id)
            
        return graph
        
    def _get_upstream_components(self, component_id: str, graph: Dict[str, List[str]]) -> List[str]:
        """Get all upstream components for a given component"""
        upstream = []
        visited = set()
        
        def traverse(comp_id):
            if comp_id in visited:
                return
            visited.add(comp_id)
            
            if comp_id in graph:
                for upstream_id in graph[comp_id]:
                    upstream.append(upstream_id)
                    traverse(upstream_id)
                    
        traverse(component_id)
        return upstream
        
    def _find_component_by_id(self, comp_id: str, components: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find a component by its ID"""
        for component in components:
            if component.get('id') == comp_id:
                return component
        return None
        
    def _check_derived_column(self, lineage_id: str, component: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check if derived column component affects the given lineage"""
        # Look through output columns for matching lineage
        for output in component.get('outputs', []):
            for column in output.get('columns', []):
                if column.get('lineageId') == lineage_id:
                    return {
                        'expression': column.get('expression', ''),
                        'source_lineage': column.get('sourceLineageId', lineage_id)
                    }
        return None
        
    def _check_data_conversion(self, lineage_id: str, component: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check data conversion transformation"""
        for output in component.get('outputs', []):
            for column in output.get('columns', []):
                if column.get('lineageId') == lineage_id:
                    return {
                        'target_type': column.get('dataType', ''),
                        'source_lineage': column.get('sourceLineageId', lineage_id)
                    }
        return None
        
    def _check_lookup_transformation(self, lineage_id: str, component: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check lookup transformation"""
        properties = component.get('properties', {})
        lookup_table = properties.get('SqlCommand', '') or properties.get('OpenRowset', '')
        
        if lookup_table:
            return {
                'lookup_table': self._clean_table_name(lookup_table)
            }
        return None
        
    def _find_source_column(self, lineage_id: str, sources: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find the source column for a given lineage ID"""
        for source in sources:
            # Check outputs of source component
            for output in source.get('outputs', []):
                for column in output.get('columns', []):
                    if column.get('lineageId') == lineage_id:
                        source_info = self._extract_source_info(source)
                        return {
                            'source_table': source_info.get('table'),
                            'source_file': source_info.get('file'),
                            'source_column': column.get('name', '')
                        }
        return None
        
    def _extract_source_info(self, source: Dict[str, Any]) -> Dict[str, str]:
        """Extract source table/file information"""
        properties = source.get('properties', {})
        component_class = source.get('componentClassID', '')
        
        info = {'table': None, 'file': None}
        
        if 'ExcelSource' in component_class:
            # Excel source
            connection_string = properties.get('ConnectionString', '')
            sheet_name = properties.get('OpenRowset', '')
            if 'Data Source=' in connection_string:
                file_path = self._extract_file_path_from_connection(connection_string)
                info['file'] = file_path
            if sheet_name:
                info['table'] = sheet_name
                
        elif 'OLEDBSource' in component_class:
            # Database source
            table_name = properties.get('OpenRowset', '') or properties.get('SqlCommand', '')
            if table_name:
                info['table'] = self._clean_table_name(table_name)
                
        elif 'FlatFileSource' in component_class:
            # Flat file source
            connection_string = properties.get('ConnectionString', '')
            file_path = self._extract_file_path_from_connection(connection_string)
            info['file'] = file_path
            
        return info
        
    def _extract_file_path_from_connection(self, connection_string: str) -> str:
        """Extract file path from connection string"""
        # Look for Data Source= pattern
        match = re.search(r'Data Source=([^;]+)', connection_string)
        if match:
            file_path = match.group(1).strip()
            # Extract just the filename
            return file_path.split('\\')[-1] if '\\' in file_path else file_path.split('/')[-1]
        return ''
        
    def _clean_table_name(self, table_name: str) -> str:
        """Clean and normalize table name"""
        # Remove brackets, quotes, and extra whitespace
        cleaned = table_name.strip().replace('[', '').replace(']', '').replace('"', '').replace("'", '')
        
        # If it's a SQL command, try to extract table name
        if 'SELECT' in cleaned.upper() or 'FROM' in cleaned.upper():
            # Try to extract table name from SQL
            match = re.search(r'FROM\s+([^\s,]+)', cleaned, re.IGNORECASE)
            if match:
                return match.group(1).strip()
                
        return cleaned
        
    def _convert_data_type(self, ssis_type: str) -> str:
        """Convert SSIS data type to Databricks type"""
        if not ssis_type:
            return DatabricksDataType.STRING.value
            
        # Normalize the type string
        ssis_type_clean = ssis_type.lower().strip()
        
        # Try direct mapping first
        for ssis_enum in SSISDataType:
            if ssis_enum.value.lower() == ssis_type_clean:
                return SSIS_TO_DATABRICKS_TYPE_MAPPING[ssis_enum].value
                
        # Handle numeric types with length/precision
        if 'wstr' in ssis_type_clean or 'str' in ssis_type_clean:
            return DatabricksDataType.STRING.value
        elif 'i4' in ssis_type_clean or 'int' in ssis_type_clean:
            return DatabricksDataType.INTEGER.value
        elif 'r8' in ssis_type_clean or 'float' in ssis_type_clean or 'double' in ssis_type_clean:
            return DatabricksDataType.DOUBLE.value
        elif 'bool' in ssis_type_clean:
            return DatabricksDataType.BOOLEAN.value
        elif 'dt' in ssis_type_clean or 'timestamp' in ssis_type_clean:
            return DatabricksDataType.TIMESTAMP.value
            
        # Default to String for unknown types
        return DatabricksDataType.STRING.value