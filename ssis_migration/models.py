"""
Data types and mappings for SSIS to Databricks migration.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from enum import Enum


class SSISDataType(Enum):
    """SSIS data types"""
    WSTR = "wstr"
    I4 = "i4"
    DT = "dt"
    R8 = "r8"
    BOOL = "bool"
    UI1 = "ui1"
    UI2 = "ui2"
    UI4 = "ui4"
    I1 = "i1"
    I2 = "i2"
    I8 = "i8"
    R4 = "r4"
    CY = "cy"
    DATE = "date"
    DBTIMESTAMP = "dbtimestamp"
    DECIMAL = "decimal"
    NUMERIC = "numeric"
    BYTES = "bytes"
    STR = "str"


class DatabricksDataType(Enum):
    """Databricks/Spark data types"""
    STRING = "String"
    INTEGER = "Integer"
    LONG = "Long"
    DOUBLE = "Double"
    FLOAT = "Float"
    BOOLEAN = "Boolean"
    TIMESTAMP = "Timestamp"
    DATE = "Date"
    DECIMAL = "Decimal"
    BINARY = "Binary"
    BYTE = "Byte"
    SHORT = "Short"


# SSIS to Databricks data type mapping
SSIS_TO_DATABRICKS_TYPE_MAPPING: Dict[SSISDataType, DatabricksDataType] = {
    SSISDataType.WSTR: DatabricksDataType.STRING,
    SSISDataType.STR: DatabricksDataType.STRING,
    SSISDataType.I4: DatabricksDataType.INTEGER,
    SSISDataType.DT: DatabricksDataType.TIMESTAMP,
    SSISDataType.DBTIMESTAMP: DatabricksDataType.TIMESTAMP,
    SSISDataType.DATE: DatabricksDataType.DATE,
    SSISDataType.R8: DatabricksDataType.DOUBLE,
    SSISDataType.R4: DatabricksDataType.FLOAT,
    SSISDataType.BOOL: DatabricksDataType.BOOLEAN,
    SSISDataType.UI1: DatabricksDataType.BYTE,
    SSISDataType.I1: DatabricksDataType.BYTE,
    SSISDataType.UI2: DatabricksDataType.SHORT,
    SSISDataType.I2: DatabricksDataType.SHORT,
    SSISDataType.UI4: DatabricksDataType.LONG,
    SSISDataType.I8: DatabricksDataType.LONG,
    SSISDataType.CY: DatabricksDataType.DECIMAL,
    SSISDataType.DECIMAL: DatabricksDataType.DECIMAL,
    SSISDataType.NUMERIC: DatabricksDataType.DECIMAL,
    SSISDataType.BYTES: DatabricksDataType.BINARY,
}


@dataclass
class SSISConnection:
    """SSIS Connection Manager representation"""
    name: str
    connection_type: str
    connection_string: str
    object_name: str
    dts_id: str


@dataclass
class SSISParameter:
    """SSIS Parameter representation"""
    name: str
    data_type: str
    value: Any
    description: Optional[str] = None


@dataclass
class SSISExecutable:
    """SSIS Executable component representation"""
    ref_id: str
    name: str
    creation_name: str
    description: Optional[str]
    dts_id: str
    executable_type: str
    
    
@dataclass
class SSISDataFlow:
    """SSIS Data Flow representation"""
    name: str
    components: List[Dict[str, Any]]
    paths: List[Dict[str, Any]]
    

@dataclass
class SourceTargetMapping:
    """Source to Target Mapping representation"""
    source_table: Optional[str]
    source_file: Optional[str] 
    source_column: str
    target_table: str
    target_column: str
    data_type: str
    transformation_rule: Optional[str]
    validation_name: Optional[str]
    transformation_name: Optional[str]
    is_derived: bool = False
    

@dataclass
class SSISPackage:
    """Complete SSIS Package representation"""
    name: str
    file_path: str
    connections: List[SSISConnection]
    parameters: List[SSISParameter]
    executables: List[SSISExecutable]
    data_flows: List[SSISDataFlow]
    dependencies: List[Dict[str, str]]