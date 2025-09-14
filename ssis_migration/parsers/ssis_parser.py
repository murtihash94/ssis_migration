"""
SSIS Package Parser - Extracts components from SSIS .dtsx, .conmgr, and .params files
"""

import xml.etree.ElementTree as ET
import xmltodict
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

from ..models import (
    SSISPackage, SSISConnection, SSISParameter, SSISExecutable, 
    SSISDataFlow, SSISDataType, SSIS_TO_DATABRICKS_TYPE_MAPPING
)


class SSISPackageParser:
    """Parser for SSIS packages and related files"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def parse_package(self, dtsx_file_path: str) -> SSISPackage:
        """
        Parse SSIS package from .dtsx file
        
        Args:
            dtsx_file_path: Path to .dtsx file
            
        Returns:
            SSISPackage object with parsed components
        """
        self.logger.info(f"Parsing SSIS package: {dtsx_file_path}")
        
        try:
            # Parse XML with namespace handling
            tree = ET.parse(dtsx_file_path)
            root = tree.getroot()
            
            # Register DTS namespace
            if 'DTS' not in ET._namespace_map:
                ET.register_namespace('DTS', 'www.microsoft.com/SqlServer/Dts')
            
            # Define namespace for easier access
            ns = {'DTS': 'www.microsoft.com/SqlServer/Dts'}
            
            # Extract package information
            package_name = Path(dtsx_file_path).stem
            
            # Parse components with namespace
            connections = self._parse_package_connections(root, ns)
            parameters = self._parse_package_parameters(root, ns)
            executables = self._parse_executables(root, ns)
            data_flows = self._parse_data_flows(root, ns)
            dependencies = self._parse_dependencies(root, ns)
            
            package = SSISPackage(
                name=package_name,
                file_path=dtsx_file_path,
                connections=connections,
                parameters=parameters,
                executables=executables,
                data_flows=data_flows,
                dependencies=dependencies
            )
            
            self.logger.info(f"Successfully parsed package: {package_name}")
            return package
            
        except Exception as e:
            self.logger.error(f"Failed to parse package {dtsx_file_path}: {str(e)}")
            raise
            
    def parse_connection_manager(self, conmgr_file_path: str) -> List[SSISConnection]:
        """
        Parse connection manager from .conmgr file
        
        Args:
            conmgr_file_path: Path to .conmgr file
            
        Returns:
            List of SSISConnection objects
        """
        self.logger.info(f"Parsing connection manager: {conmgr_file_path}")
        
        try:
            tree = ET.parse(conmgr_file_path)
            root = tree.getroot()
            
            # Extract connection information
            connection = SSISConnection(
                name=root.get('DTS:ObjectName', ''),
                connection_type=root.get('DTS:CreationName', ''),
                connection_string=self._extract_connection_string(root),
                object_name=root.get('DTS:ObjectName', ''),
                dts_id=root.get('DTS:DTSID', '')
            )
            
            return [connection]
            
        except Exception as e:
            self.logger.error(f"Failed to parse connection manager {conmgr_file_path}: {str(e)}")
            return []
            
    def parse_parameters(self, params_file_path: str) -> List[SSISParameter]:
        """
        Parse parameters from .params file
        
        Args:
            params_file_path: Path to .params file
            
        Returns:
            List of SSISParameter objects
        """
        self.logger.info(f"Parsing parameters: {params_file_path}")
        
        try:
            tree = ET.parse(params_file_path)
            root = tree.getroot()
            
            parameters = []
            # SSIS parameters XML structure may vary, handle basic case
            for param_elem in root.findall('.//Parameter'):
                parameter = SSISParameter(
                    name=param_elem.get('Name', ''),
                    data_type=param_elem.get('DataType', ''),
                    value=param_elem.get('Value', ''),
                    description=param_elem.get('Description', '')
                )
                parameters.append(parameter)
                
            return parameters
            
        except Exception as e:
            self.logger.error(f"Failed to parse parameters {params_file_path}: {str(e)}")
            return []
            
    def _parse_package_connections(self, root: ET.Element, ns: dict) -> List[SSISConnection]:
        """Parse connections within a package"""
        connections = []
        
        # Find ConnectionManagers section
        conn_managers = root.find('.//DTS:ConnectionManagers', ns)
        if conn_managers is None:
            return connections
            
        for conn_mgr in conn_managers.findall('.//DTS:ConnectionManager', ns):
            connection = SSISConnection(
                name=conn_mgr.get('{www.microsoft.com/SqlServer/Dts}ObjectName', ''),
                connection_type=conn_mgr.get('{www.microsoft.com/SqlServer/Dts}CreationName', ''),
                connection_string=self._extract_connection_string(conn_mgr, ns),
                object_name=conn_mgr.get('{www.microsoft.com/SqlServer/Dts}ObjectName', ''),
                dts_id=conn_mgr.get('{www.microsoft.com/SqlServer/Dts}DTSID', '')
            )
            connections.append(connection)
            
        return connections
        
    def _parse_package_parameters(self, root: ET.Element) -> List[SSISParameter]:
        """Parse parameters within a package"""
        parameters = []
        
        # Find Variables or Parameters sections
        variables = root.find('.//DTS:Variables')
        if variables is not None:
            for var in variables.findall('.//DTS:Variable'):
                parameter = SSISParameter(
                    name=var.get('DTS:ObjectName', ''),
                    data_type=var.get('DTS:DataType', ''),
                    value=self._extract_variable_value(var),
                    description=var.get('DTS:Description', '')
                )
                parameters.append(parameter)
                
        return parameters
        
    def _parse_executables(self, root: ET.Element) -> List[SSISExecutable]:
        """Parse executable components"""
        executables = []
        
        # Find Executables section
        exec_section = root.find('.//DTS:Executables')
        if exec_section is None:
            return executables
            
        for executable in exec_section.findall('.//DTS:Executable'):
            exec_obj = SSISExecutable(
                ref_id=executable.get('DTS:refId', ''),
                name=executable.get('DTS:ObjectName', ''),
                creation_name=executable.get('DTS:CreationName', ''),
                description=executable.get('DTS:Description', ''),
                dts_id=executable.get('DTS:DTSID', ''),
                executable_type=executable.get('DTS:ExecutableType', '')
            )
            executables.append(exec_obj)
            
        return executables
        
    def _parse_data_flows(self, root: ET.Element) -> List[SSISDataFlow]:
        """Parse data flow tasks"""
        data_flows = []
        
        # Find data flow tasks in executables
        for executable in root.findall('.//DTS:Executable'):
            if executable.get('DTS:CreationName') == 'Microsoft.Pipeline':
                # This is a data flow task
                data_flow = self._parse_single_data_flow(executable)
                if data_flow:
                    data_flows.append(data_flow)
                    
        return data_flows
        
    def _parse_single_data_flow(self, executable: ET.Element) -> Optional[SSISDataFlow]:
        """Parse a single data flow task"""
        try:
            name = executable.get('DTS:ObjectName', '')
            
            # Find pipeline components
            components = []
            paths = []
            
            # Look for ObjectData containing pipeline information
            object_data = executable.find('.//DTS:ObjectData')
            if object_data is not None:
                # Parse pipeline XML within ObjectData
                pipeline_elem = object_data.find('.//pipeline')
                if pipeline_elem is not None:
                    components = self._parse_pipeline_components(pipeline_elem)
                    paths = self._parse_pipeline_paths(pipeline_elem)
                    
            return SSISDataFlow(
                name=name,
                components=components,
                paths=paths
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to parse data flow: {str(e)}")
            return None
            
    def _parse_pipeline_components(self, pipeline_elem: ET.Element) -> List[Dict[str, Any]]:
        """Parse pipeline components from data flow"""
        components = []
        
        for component in pipeline_elem.findall('.//component'):
            comp_info = {
                'id': component.get('id', ''),
                'name': component.get('name', ''),
                'componentClassID': component.get('componentClassID', ''),
                'contactInfo': component.get('contactInfo', ''),
                'description': component.get('description', ''),
                'properties': self._parse_component_properties(component),
                'inputs': self._parse_component_inputs(component),
                'outputs': self._parse_component_outputs(component)
            }
            components.append(comp_info)
            
        return components
        
    def _parse_component_properties(self, component: ET.Element) -> Dict[str, Any]:
        """Parse component properties"""
        properties = {}
        
        props_elem = component.find('.//properties')
        if props_elem is not None:
            for prop in props_elem.findall('.//property'):
                prop_name = prop.get('name', '')
                prop_value = prop.text or prop.get('value', '')
                properties[prop_name] = prop_value
                
        return properties
        
    def _parse_component_inputs(self, component: ET.Element) -> List[Dict[str, Any]]:
        """Parse component inputs"""
        inputs = []
        
        inputs_elem = component.find('.//inputs')
        if inputs_elem is not None:
            for input_elem in inputs_elem.findall('.//input'):
                input_info = {
                    'id': input_elem.get('id', ''),
                    'name': input_elem.get('name', ''),
                    'columns': self._parse_input_columns(input_elem)
                }
                inputs.append(input_info)
                
        return inputs
        
    def _parse_component_outputs(self, component: ET.Element) -> List[Dict[str, Any]]:
        """Parse component outputs"""
        outputs = []
        
        outputs_elem = component.find('.//outputs')
        if outputs_elem is not None:
            for output_elem in outputs_elem.findall('.//output'):
                output_info = {
                    'id': output_elem.get('id', ''),
                    'name': output_elem.get('name', ''),
                    'columns': self._parse_output_columns(output_elem)
                }
                outputs.append(output_info)
                
        return outputs
        
    def _parse_input_columns(self, input_elem: ET.Element) -> List[Dict[str, Any]]:
        """Parse input columns"""
        columns = []
        
        cols_elem = input_elem.find('.//inputColumns')
        if cols_elem is not None:
            for col in cols_elem.findall('.//inputColumn'):
                col_info = {
                    'id': col.get('id', ''),
                    'name': col.get('name', ''),
                    'lineageId': col.get('lineageId', ''),
                    'dataType': col.get('dataType', ''),
                    'length': col.get('length', ''),
                    'precision': col.get('precision', ''),
                    'scale': col.get('scale', '')
                }
                columns.append(col_info)
                
        return columns
        
    def _parse_output_columns(self, output_elem: ET.Element) -> List[Dict[str, Any]]:
        """Parse output columns"""
        columns = []
        
        cols_elem = output_elem.find('.//outputColumns')
        if cols_elem is not None:
            for col in cols_elem.findall('.//outputColumn'):
                col_info = {
                    'id': col.get('id', ''),
                    'name': col.get('name', ''),
                    'lineageId': col.get('lineageId', ''),
                    'dataType': col.get('dataType', ''),
                    'length': col.get('length', ''),
                    'precision': col.get('precision', ''),
                    'scale': col.get('scale', ''),
                    'expression': self._extract_column_expression(col)
                }
                columns.append(col_info)
                
        return columns
        
    def _parse_pipeline_paths(self, pipeline_elem: ET.Element) -> List[Dict[str, Any]]:
        """Parse pipeline paths (data flow connections)"""
        paths = []
        
        paths_elem = pipeline_elem.find('.//paths')
        if paths_elem is not None:
            for path in paths_elem.findall('.//path'):
                path_info = {
                    'id': path.get('id', ''),
                    'name': path.get('name', ''),
                    'startId': path.get('startId', ''),
                    'endId': path.get('endId', '')
                }
                paths.append(path_info)
                
        return paths
        
    def _parse_dependencies(self, root: ET.Element) -> List[Dict[str, str]]:
        """Parse precedence constraints (dependencies)"""
        dependencies = []
        
        # Find PrecedenceConstraints
        for constraint in root.findall('.//DTS:PrecedenceConstraint'):
            dep_info = {
                'from': constraint.get('DTS:From', ''),
                'to': constraint.get('DTS:To', ''),
                'logicalAnd': constraint.get('DTS:LogicalAnd', 'True'),
                'value': constraint.get('DTS:Value', 'Success')
            }
            dependencies.append(dep_info)
            
        return dependencies
        
    def _extract_connection_string(self, element: ET.Element) -> str:
        """Extract connection string from connection manager element"""
        conn_string = ''
        
        # Look in ObjectData
        object_data = element.find('.//DTS:ObjectData')
        if object_data is not None:
            conn_mgr = object_data.find('.//DTS:ConnectionManager')
            if conn_mgr is not None:
                conn_string = conn_mgr.get('DTS:ConnectionString', '')
                
        return conn_string
        
    def _extract_variable_value(self, variable: ET.Element) -> str:
        """Extract variable value"""
        # Look for VariableValue element
        var_value = variable.find('.//DTS:VariableValue')
        if var_value is not None:
            return var_value.text or ''
        return ''
        
    def _extract_column_expression(self, column: ET.Element) -> str:
        """Extract expression from derived column"""
        # Look for expression in properties
        props = column.find('.//properties')
        if props is not None:
            for prop in props.findall('.//property'):
                if prop.get('name', '').lower() == 'expression':
                    return prop.text or prop.get('value', '')
        return ''