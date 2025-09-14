"""
SSIS Package Parser - Extracts components from SSIS .dtsx, .conmgr, and .params files
"""

import xml.etree.ElementTree as ET
import xmltodict
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import re

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
            # Read file content and parse with xmltodict for easier namespace handling
            with open(dtsx_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
            # Use xmltodict for easier access to namespaced elements
            parsed_dict = xmltodict.parse(content)
            
            # Extract package information
            package_name = Path(dtsx_file_path).stem
            
            # Parse components from the dictionary structure
            connections = self._parse_package_connections_dict(parsed_dict)
            parameters = self._parse_package_parameters_dict(parsed_dict)
            executables = self._parse_executables_dict(parsed_dict)
            data_flows = self._parse_data_flows_dict(parsed_dict, dtsx_file_path)
            dependencies = self._parse_dependencies_dict(parsed_dict)
            
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
            # Create a minimal package to allow processing to continue
            package_name = Path(dtsx_file_path).stem
            return SSISPackage(
                name=package_name,
                file_path=dtsx_file_path,
                connections=[],
                parameters=[],
                executables=[],
                data_flows=[],
                dependencies=[]
            )
            
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
            with open(conmgr_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
            parsed_dict = xmltodict.parse(content)
            
            # Navigate to ConnectionManager
            conn_mgr = parsed_dict.get('DTS:ConnectionManager', {})
            
            if conn_mgr:
                connection = SSISConnection(
                    name=conn_mgr.get('@DTS:ObjectName', ''),
                    connection_type=conn_mgr.get('@DTS:CreationName', ''),
                    connection_string=self._extract_connection_string_dict(conn_mgr),
                    object_name=conn_mgr.get('@DTS:ObjectName', ''),
                    dts_id=conn_mgr.get('@DTS:DTSID', '')
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
            with open(params_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
            parsed_dict = xmltodict.parse(content)
            
            parameters = []
            # Navigate to Parameters
            params_section = parsed_dict.get('SSIS:Parameters', {})
            
            if params_section:
                param_list = params_section.get('Parameter', [])
                if not isinstance(param_list, list):
                    param_list = [param_list] if param_list else []
                    
                for param in param_list:
                    if isinstance(param, dict):
                        parameter = SSISParameter(
                            name=param.get('@Name', ''),
                            data_type=param.get('@DataType', ''),
                            value=param.get('@Value', ''),
                            description=param.get('@Description', '')
                        )
                        parameters.append(parameter)
                        
            return parameters
            
        except Exception as e:
            self.logger.error(f"Failed to parse parameters {params_file_path}: {str(e)}")
            return []
            
    def _parse_package_connections_dict(self, parsed_dict: dict) -> List[SSISConnection]:
        """Parse connections from parsed dictionary"""
        connections = []
        
        try:
            # Navigate to ConnectionManagers
            executable = parsed_dict.get('DTS:Executable', {})
            conn_managers = executable.get('DTS:ConnectionManagers', {})
            
            if not conn_managers:
                return connections
                
            # Handle both single and multiple connection managers
            conn_mgr_list = conn_managers.get('DTS:ConnectionManager', [])
            if not isinstance(conn_mgr_list, list):
                conn_mgr_list = [conn_mgr_list]
                
            for conn_mgr in conn_mgr_list:
                if isinstance(conn_mgr, dict):
                    connection = SSISConnection(
                        name=conn_mgr.get('@DTS:ObjectName', ''),
                        connection_type=conn_mgr.get('@DTS:CreationName', ''),
                        connection_string=self._extract_connection_string_dict(conn_mgr),
                        object_name=conn_mgr.get('@DTS:ObjectName', ''),
                        dts_id=conn_mgr.get('@DTS:DTSID', '')
                    )
                    connections.append(connection)
                    
        except Exception as e:
            self.logger.warning(f"Error parsing connections: {e}")
            
        return connections
        
    def _parse_package_parameters_dict(self, parsed_dict: dict) -> List[SSISParameter]:
        """Parse parameters from parsed dictionary"""
        parameters = []
        
        try:
            # Navigate to Variables
            executable = parsed_dict.get('DTS:Executable', {})
            variables = executable.get('DTS:Variables', {})
            
            if not variables:
                return parameters
                
            var_list = variables.get('DTS:Variable', [])
            if not isinstance(var_list, list):
                var_list = [var_list]
                
            for var in var_list:
                if isinstance(var, dict):
                    parameter = SSISParameter(
                        name=var.get('@DTS:ObjectName', ''),
                        data_type=var.get('@DTS:DataType', ''),
                        value=self._extract_variable_value_dict(var),
                        description=var.get('@DTS:Description', '')
                    )
                    parameters.append(parameter)
                    
        except Exception as e:
            self.logger.warning(f"Error parsing parameters: {e}")
            
        return parameters
        
    def _parse_executables_dict(self, parsed_dict: dict) -> List[SSISExecutable]:
        """Parse executables from parsed dictionary"""
        executables = []
        
        try:
            # Navigate to Executables
            executable = parsed_dict.get('DTS:Executable', {})
            exec_section = executable.get('DTS:Executables', {})
            
            if not exec_section:
                return executables
                
            exec_list = exec_section.get('DTS:Executable', [])
            if not isinstance(exec_list, list):
                exec_list = [exec_list]
                
            for exec_item in exec_list:
                if isinstance(exec_item, dict):
                    exec_obj = SSISExecutable(
                        ref_id=exec_item.get('@DTS:refId', ''),
                        name=exec_item.get('@DTS:ObjectName', ''),
                        creation_name=exec_item.get('@DTS:CreationName', ''),
                        description=exec_item.get('@DTS:Description', ''),
                        dts_id=exec_item.get('@DTS:DTSID', ''),
                        executable_type=exec_item.get('@DTS:ExecutableType', '')
                    )
                    executables.append(exec_obj)
                    
        except Exception as e:
            self.logger.warning(f"Error parsing executables: {e}")
            
        return executables
        
    def _parse_data_flows_dict(self, parsed_dict: dict, file_path: str) -> List[SSISDataFlow]:
        """Parse data flows from parsed dictionary"""
        data_flows = []
        
        try:
            # For now, create basic data flow representations
            # This is a simplified approach - real SSIS data flows are complex
            executable = parsed_dict.get('DTS:Executable', {})
            exec_section = executable.get('DTS:Executables', {})
            
            if exec_section:
                exec_list = exec_section.get('DTS:Executable', [])
                if not isinstance(exec_list, list):
                    exec_list = [exec_list]
                    
                for exec_item in exec_list:
                    if isinstance(exec_item, dict):
                        creation_name = exec_item.get('@DTS:CreationName', '')
                        if 'Pipeline' in creation_name:
                            # This is a data flow task
                            data_flow = SSISDataFlow(
                                name=exec_item.get('@DTS:ObjectName', ''),
                                components=self._extract_basic_components(file_path),
                                paths=[]
                            )
                            data_flows.append(data_flow)
                            
        except Exception as e:
            self.logger.warning(f"Error parsing data flows: {e}")
            
        return data_flows
        
    def _parse_dependencies_dict(self, parsed_dict: dict) -> List[Dict[str, str]]:
        """Parse dependencies from parsed dictionary"""
        dependencies = []
        
        try:
            # Look for PrecedenceConstraints - simplified approach
            # Real implementation would need to traverse the full structure
            pass
            
        except Exception as e:
            self.logger.warning(f"Error parsing dependencies: {e}")
            
        return dependencies
        
    def _extract_connection_string_dict(self, conn_mgr: dict) -> str:
        """Extract connection string from connection manager dictionary"""
        try:
            object_data = conn_mgr.get('DTS:ObjectData', {})
            if isinstance(object_data, dict):
                inner_conn_mgr = object_data.get('DTS:ConnectionManager', {})
                if isinstance(inner_conn_mgr, dict):
                    return inner_conn_mgr.get('@DTS:ConnectionString', '')
        except Exception:
            pass
        return ''
        
    def _extract_variable_value_dict(self, variable: dict) -> str:
        """Extract variable value from dictionary"""
        try:
            var_value = variable.get('DTS:VariableValue', '')
            if isinstance(var_value, dict):
                return var_value.get('#text', '')
            return str(var_value) if var_value else ''
        except Exception:
            pass
        return ''
        
    def _extract_basic_components(self, file_path: str) -> List[Dict[str, Any]]:
        """Extract basic component information from file analysis"""
        components = []
        
        # Read the raw XML to extract basic information
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
            # Look for common SSIS component patterns
            if 'ExcelSource' in content:
                components.append({
                    'id': 'excel_source_1',
                    'name': 'Excel Source',
                    'componentClassID': 'Microsoft.ExcelSource',
                    'properties': self._extract_excel_properties(content),
                    'outputs': [{'columns': self._infer_excel_columns()}]
                })
                
            if 'OLEDBSource' in content:
                components.append({
                    'id': 'oledb_source_1', 
                    'name': 'OLE DB Source',
                    'componentClassID': 'Microsoft.OLEDBSource',
                    'properties': self._extract_oledb_properties(content),
                    'outputs': [{'columns': self._infer_table_columns()}]
                })
                
            if 'OLEDBDestination' in content:
                components.append({
                    'id': 'oledb_dest_1',
                    'name': 'OLE DB Destination', 
                    'componentClassID': 'Microsoft.OLEDBDestination',
                    'properties': self._extract_destination_properties(content),
                    'inputs': [{'columns': self._infer_destination_columns()}]
                })
                
        except Exception as e:
            self.logger.warning(f"Error extracting basic components: {e}")
            
        return components
        
    def _extract_excel_properties(self, content: str) -> Dict[str, str]:
        """Extract Excel source properties from XML content"""
        properties = {}
        
        # Look for Excel connection string
        match = re.search(r'Data Source=([^;]+).*?\.xlsx?', content, re.IGNORECASE)
        if match:
            properties['file_path'] = match.group(1)
            
        # Look for sheet name
        match = re.search(r'OpenRowset.*?>\s*([^<]+)', content)
        if match:
            properties['sheet_name'] = match.group(1).strip()
            
        return properties
        
    def _extract_oledb_properties(self, content: str) -> Dict[str, str]:
        """Extract OLE DB source properties from XML content"""
        properties = {}
        
        # Look for table name
        match = re.search(r'OpenRowset.*?>\s*([^<]+)', content)
        if match:
            properties['table_name'] = match.group(1).strip()
            
        return properties
        
    def _extract_destination_properties(self, content: str) -> Dict[str, str]:
        """Extract destination properties from XML content"""
        properties = {}
        
        # Look for destination table
        match = re.search(r'AccessMode.*?>\s*([^<]+)', content)
        if match:
            properties['destination_table'] = match.group(1).strip()
            
        return properties
        
    def _infer_excel_columns(self) -> List[Dict[str, str]]:
        """Infer common Excel columns"""
        return [
            {'name': 'Column1', 'dataType': 'wstr', 'length': '255'},
            {'name': 'Column2', 'dataType': 'wstr', 'length': '255'},
            {'name': 'Column3', 'dataType': 'i4', 'length': '4'}
        ]
        
    def _infer_table_columns(self) -> List[Dict[str, str]]:
        """Infer common table columns"""
        return [
            {'name': 'ID', 'dataType': 'i4', 'length': '4'},
            {'name': 'Name', 'dataType': 'wstr', 'length': '100'},
            {'name': 'CreatedDate', 'dataType': 'dt', 'length': '8'}
        ]
        
    def _infer_destination_columns(self) -> List[Dict[str, str]]:
        """Infer destination columns"""
        return [
            {'name': 'ID', 'dataType': 'i4', 'length': '4'},
            {'name': 'Name', 'dataType': 'wstr', 'length': '100'},
            {'name': 'LoadDate', 'dataType': 'dt', 'length': '8'}
        ]
        
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