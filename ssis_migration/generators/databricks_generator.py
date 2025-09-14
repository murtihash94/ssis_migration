"""
Databricks Workflow Generator - Creates Databricks assets from STTM and SSIS packages
"""

import logging
from typing import List, Dict, Any
from pathlib import Path
import yaml
from jinja2 import Template

from ..models import SSISPackage, SourceTargetMapping


class DatabricksWorkflowGenerator:
    """Generates Databricks workflows, notebooks, and configurations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def generate_dlt_pipeline(self, mappings: List[SourceTargetMapping], 
                            packages: List[SSISPackage]) -> str:
        """
        Generate DLT pipeline notebook with Bronze/Silver/Gold layers
        
        Args:
            mappings: Source-target mappings
            packages: SSIS packages
            
        Returns:
            DLT pipeline notebook content
        """
        self.logger.info("Generating DLT pipeline notebook")
        
        template = Template(self._get_dlt_pipeline_template())
        
        # Group mappings by target table
        tables_mappings = {}
        for mapping in mappings:
            table = mapping.target_table
            if table not in tables_mappings:
                tables_mappings[table] = []
            tables_mappings[table].append(mapping)
            
        # Identify data sources
        data_sources = self._identify_data_sources(mappings)
        
        # Generate bronze layer functions
        bronze_functions = self._generate_bronze_layer_functions(data_sources)
        
        # Generate silver layer functions
        silver_functions = self._generate_silver_layer_functions(tables_mappings)
        
        # Generate gold layer functions
        gold_functions = self._generate_gold_layer_functions(tables_mappings)
        
        content = template.render(
            bronze_functions=bronze_functions,
            silver_functions=silver_functions,
            gold_functions=gold_functions,
            data_sources=data_sources,
            tables=list(tables_mappings.keys())
        )
        
        return content
        
    def generate_workflow(self, packages: List[SSISPackage]) -> str:
        """
        Generate Databricks Asset Bundle workflow YAML
        
        Args:
            packages: SSIS packages
            
        Returns:
            Workflow YAML content
        """
        self.logger.info("Generating Databricks workflow")
        
        # Create workflow structure
        workflow = {
            'resources': {
                'jobs': {
                    'ssis_migration_job': {
                        'name': 'SSIS Migration Job',
                        'tasks': []
                    }
                },
                'pipelines': {
                    'ssis_dlt_pipeline': {
                        'name': 'SSIS DLT Pipeline',
                        'target': 'dev',
                        'libraries': [
                            {'notebook': {'path': './notebooks/dlt_pipeline'}}
                        ],
                        'configuration': {
                            'pipeline.progress_reporting': 'true'
                        }
                    }
                }
            }
        }
        
        # Add data extraction tasks
        task_order = 1
        for package in packages:
            if self._has_excel_sources(package):
                workflow['resources']['jobs']['ssis_migration_job']['tasks'].append({
                    'task_key': f'extract_excel_{package.name.lower().replace(" ", "_").replace("-", "_")}',
                    'depends_on': [],
                    'notebook_task': {
                        'notebook_path': f'./notebooks/data_extraction/extract_{package.name.lower().replace(" ", "_").replace("-", "_")}'
                    },
                    'cluster': {
                        'num_workers': 1,
                        'spark_version': '13.3.x-scala2.12',
                        'node_type_id': 'i3.xlarge'
                    }
                })
                task_order += 1
                
        # Add DLT pipeline task
        dependencies = [task['task_key'] for task in workflow['resources']['jobs']['ssis_migration_job']['tasks']]
        workflow['resources']['jobs']['ssis_migration_job']['tasks'].append({
            'task_key': 'run_dlt_pipeline',
            'depends_on': [{'task_key': dep} for dep in dependencies],
            'pipeline_task': {
                'pipeline_id': '${resources.pipelines.ssis_dlt_pipeline.id}'
            }
        })
        
        return yaml.dump(workflow, default_flow_style=False, sort_keys=False)
        
    def generate_extraction_notebooks(self, packages: List[SSISPackage]) -> Dict[str, str]:
        """
        Generate data extraction notebooks for Excel and other file sources
        
        Args:
            packages: SSIS packages
            
        Returns:
            Dictionary of notebook names and their content
        """
        self.logger.info("Generating data extraction notebooks")
        
        notebooks = {}
        
        for package in packages:
            # Generate Excel extraction notebook if needed
            excel_sources = self._get_excel_sources(package)
            if excel_sources:
                notebook_name = f"extract_{package.name.lower().replace(' ', '_').replace('-', '_')}"
                content = self._generate_excel_extraction_notebook(excel_sources, package)
                notebooks[notebook_name] = content
                
        return notebooks
        
    def generate_config_files(self, packages: List[SSISPackage]) -> Dict[str, str]:
        """
        Generate configuration files for connections and parameters
        
        Args:
            packages: SSIS packages
            
        Returns:
            Dictionary of config file names and their content
        """
        self.logger.info("Generating configuration files")
        
        config_files = {}
        
        # Generate connections configuration
        connections_config = self._generate_connections_config(packages)
        config_files['connections'] = yaml.dump(connections_config, default_flow_style=False)
        
        # Generate parameters configuration
        parameters_config = self._generate_parameters_config(packages)
        config_files['parameters'] = yaml.dump(parameters_config, default_flow_style=False)
        
        return config_files
        
    def _identify_data_sources(self, mappings: List[SourceTargetMapping]) -> List[Dict[str, str]]:
        """Identify unique data sources from mappings"""
        sources = {}
        
        for mapping in mappings:
            if mapping.source_file:
                # File source
                source_key = f"file_{mapping.source_file}"
                sources[source_key] = {
                    'type': 'file',
                    'name': mapping.source_file,
                    'path': f'/path/to/{mapping.source_file}',
                    'format': self._infer_file_format(mapping.source_file)
                }
            elif mapping.source_table:
                # Table source
                source_key = f"table_{mapping.source_table}"
                sources[source_key] = {
                    'type': 'table',
                    'name': mapping.source_table,
                    'database': 'source_db'
                }
                
        return list(sources.values())
        
    def _infer_file_format(self, filename: str) -> str:
        """Infer file format from filename"""
        if filename.lower().endswith('.xlsx') or filename.lower().endswith('.xls'):
            return 'excel'
        elif filename.lower().endswith('.csv'):
            return 'csv'
        elif filename.lower().endswith('.parquet'):
            return 'parquet'
        else:
            return 'delimited'
            
    def _generate_bronze_layer_functions(self, data_sources: List[Dict[str, str]]) -> List[str]:
        """Generate bronze layer ingestion functions"""
        functions = []
        
        for source in data_sources:
            if source['type'] == 'file':
                if source['format'] == 'excel':
                    # Excel files need special handling
                    function = f"""
@dlt.table(
    name="bronze_{source['name'].replace('.', '_').replace('-', '_').lower()}",
    comment="Bronze layer for {source['name']}"
)
def bronze_{source['name'].replace('.', '_').replace('-', '_').lower()}():
    return spark.read.table("extracted_{source['name'].replace('.', '_').replace('-', '_').lower()}")
"""
                else:
                    function = f"""
@dlt.table(
    name="bronze_{source['name'].replace('.', '_').replace('-', '_').lower()}",
    comment="Bronze layer for {source['name']}"
)
def bronze_{source['name'].replace('.', '_').replace('-', '_').lower()}():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "{source['format']}")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("{source['path']}")
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
"""
            else:  # table source
                function = f"""
@dlt.table(
    name="bronze_{source['name'].lower()}",
    comment="Bronze layer for {source['name']}"
)
def bronze_{source['name'].lower()}():
    return (
        spark.readStream
        .format("delta")
        .table("{source['database']}.{source['name']}")
        .withColumn("_ingestion_time", current_timestamp())
    )
"""
            functions.append(function)
            
        return functions
        
    def _generate_silver_layer_functions(self, tables_mappings: Dict[str, List[SourceTargetMapping]]) -> List[str]:
        """Generate silver layer transformation functions"""
        functions = []
        
        for table_name, mappings in tables_mappings.items():
            # Group mappings by source
            source_mappings = {}
            for mapping in mappings:
                source_key = mapping.source_file or mapping.source_table or 'unknown'
                if source_key not in source_mappings:
                    source_mappings[source_key] = []
                source_mappings[source_key].append(mapping)
                
            # Generate transformation logic
            transformations = []
            for source_key, source_maps in source_mappings.items():
                bronze_table = f"bronze_{source_key.replace('.', '_').replace('-', '_').lower()}"
                
                # Build column transformations
                col_transforms = []
                for mapping in source_maps:
                    if mapping.transformation_rule:
                        # Apply transformation
                        col_expr = f"{mapping.transformation_rule} as {mapping.target_column}"
                    else:
                        # Direct mapping
                        col_expr = f"{mapping.source_column} as {mapping.target_column}"
                    col_transforms.append(col_expr)
                    
                if col_transforms:
                    transform_sql = f"""
        .select({', '.join(col_transforms)})"""
                    transformations.append(f"        dlt.read('{bronze_table}'){transform_sql}")
                    
            function = f"""
@dlt.table(
    name="silver_{table_name.lower()}",
    comment="Silver layer for {table_name}",
    table_properties={{"delta.autoOptimize.optimizeWrite": "true"}}
)
@dlt.expect_all({self._generate_data_quality_rules(mappings)})
def silver_{table_name.lower()}():
    return (
{chr(10).join(transformations) if transformations else "        dlt.read('bronze_unknown')"}
        .withColumn("_processed_time", current_timestamp())
    )
"""
            functions.append(function)
            
        return functions
        
    def _generate_gold_layer_functions(self, tables_mappings: Dict[str, List[SourceTargetMapping]]) -> List[str]:
        """Generate gold layer functions with SCD Type-2 support"""
        functions = []
        
        for table_name, mappings in tables_mappings.items():
            # Determine if this is a dimension table (has SCD requirements)
            is_dimension = 'dim' in table_name.lower()
            
            if is_dimension:
                # Generate SCD Type-2 dimension
                function = f"""
@dlt.table(
    name="gold_{table_name.lower()}",
    comment="Gold layer dimension table with SCD Type-2"
)
def gold_{table_name.lower()}():
    return (
        dlt.read("silver_{table_name.lower()}")
        .withColumn("effective_start_date", current_timestamp())
        .withColumn("effective_end_date", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
        .withColumn("surrogate_key", monotonically_increasing_id())
    )

# SCD Type-2 merge function for {table_name}
def apply_scd_type2_{table_name.lower()}():
    # This function would contain the SCD Type-2 merge logic
    # Implementation depends on business keys and changing attributes
    pass
"""
            else:
                # Generate fact table
                function = f"""
@dlt.table(
    name="gold_{table_name.lower()}",
    comment="Gold layer fact table"
)
def gold_{table_name.lower()}():
    return (
        dlt.read("silver_{table_name.lower()}")
        .withColumn("load_date", current_date())
        .withColumn("last_updated", current_timestamp())
    )
"""
            functions.append(function)
            
        return functions
        
    def _generate_data_quality_rules(self, mappings: List[SourceTargetMapping]) -> Dict[str, str]:
        """Generate data quality rules for expect_all"""
        rules = {}
        
        for mapping in mappings:
            col_name = mapping.target_column
            data_type = mapping.data_type.lower()
            
            # Add basic not null rules for key columns
            if any(keyword in col_name.lower() for keyword in ['id', 'key', '_key']):
                rules[f"{col_name}_not_null"] = f"{col_name} IS NOT NULL"
                
            # Add data type validation rules
            if data_type == 'integer':
                rules[f"{col_name}_is_integer"] = f"CAST({col_name} AS INTEGER) IS NOT NULL"
            elif data_type in ['double', 'decimal']:
                rules[f"{col_name}_is_numeric"] = f"CAST({col_name} AS DOUBLE) IS NOT NULL"
            elif data_type == 'timestamp':
                rules[f"{col_name}_is_timestamp"] = f"CAST({col_name} AS TIMESTAMP) IS NOT NULL"
                
        return rules
        
    def _has_excel_sources(self, package: SSISPackage) -> bool:
        """Check if package has Excel data sources"""
        for data_flow in package.data_flows:
            for component in data_flow.components:
                if 'ExcelSource' in component.get('componentClassID', ''):
                    return True
        return False
        
    def _get_excel_sources(self, package: SSISPackage) -> List[Dict[str, Any]]:
        """Get Excel source components from package"""
        excel_sources = []
        
        for data_flow in package.data_flows:
            for component in data_flow.components:
                if 'ExcelSource' in component.get('componentClassID', ''):
                    excel_sources.append(component)
                    
        return excel_sources
        
    def _generate_excel_extraction_notebook(self, excel_sources: List[Dict[str, Any]], 
                                          package: SSISPackage) -> str:
        """Generate notebook for Excel data extraction"""
        template = Template("""
# Databricks notebook source
# MAGIC %md
# MAGIC # Excel Data Extraction Notebook
# MAGIC 
# MAGIC This notebook extracts data from Excel files and saves them as Delta tables
# MAGIC for ingestion into the DLT pipeline.
# MAGIC 
# MAGIC **Package**: {{ package_name }}

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Excel File Extraction Functions

{% for source in excel_sources %}
# COMMAND ----------

def extract_{{ source.name | replace(" ", "_") | replace("-", "_") | lower }}():
    \"\"\"Extract data from {{ source.file_path }}\"\"\"
    
    # Read Excel file using Pandas (for complex Excel files)
    pandas_df = pd.read_excel("{{ source.file_path }}", sheet_name="{{ source.sheet_name }}")
    
    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df)
    
    # Add metadata columns
    df_with_metadata = (
        spark_df
        .withColumn("_source_file", lit("{{ source.file_path }}"))
        .withColumn("_extraction_time", current_timestamp())
        .withColumn("_sheet_name", lit("{{ source.sheet_name }}"))
    )
    
    # Write to Delta table
    (
        df_with_metadata
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("extracted_{{ source.name | replace(".", "_") | replace("-", "_") | lower }}")
    )
    
    print(f"Extracted {df_with_metadata.count()} rows from {{ source.file_path }}")
    return df_with_metadata

{% endfor %}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Extractions

{% for source in excel_sources %}
# Extract {{ source.name }}
extract_{{ source.name | replace(" ", "_") | replace("-", "_") | lower }}()

{% endfor %}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation and Summary

# Show extracted data summary
{% for source in excel_sources %}
print("=== {{ source.name }} ===")
spark.table("extracted_{{ source.name | replace(".", "_") | replace("-", "_") | lower }}").describe().show()
print()

{% endfor %}
""")
        
        # Prepare source information
        sources_info = []
        for source in excel_sources:
            properties = source.get('properties', {})
            connection_string = properties.get('ConnectionString', '')
            
            # Extract file path and sheet name
            file_path = self._extract_excel_file_path(connection_string)
            sheet_name = properties.get('OpenRowset', 'Sheet1')
            
            sources_info.append({
                'name': source.get('name', 'Unknown'),
                'file_path': file_path,
                'sheet_name': sheet_name
            })
            
        return template.render(
            package_name=package.name,
            excel_sources=sources_info
        )
        
    def _extract_excel_file_path(self, connection_string: str) -> str:
        """Extract Excel file path from connection string"""
        import re
        match = re.search(r'Data Source=([^;]+)', connection_string)
        if match:
            return match.group(1).strip()
        return '/path/to/excel/file.xlsx'
        
    def _generate_connections_config(self, packages: List[SSISPackage]) -> Dict[str, Any]:
        """Generate connections configuration"""
        connections = {}
        
        for package in packages:
            for connection in package.connections:
                conn_name = connection.name.replace(' ', '_').lower()
                
                if 'OLEDB' in connection.connection_type:
                    # Database connection
                    connections[conn_name] = {
                        'type': 'jdbc',
                        'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                        'url': self._convert_oledb_to_jdbc_url(connection.connection_string),
                        'properties': {
                            'user': '${secrets/scope/username}',
                            'password': '${secrets/scope/password}'
                        }
                    }
                elif 'EXCEL' in connection.connection_type:
                    # Excel connection (converted to file path)
                    connections[conn_name] = {
                        'type': 'file',
                        'format': 'excel',
                        'path': self._extract_excel_file_path(connection.connection_string)
                    }
                    
        return {'connections': connections}
        
    def _generate_parameters_config(self, packages: List[SSISPackage]) -> Dict[str, Any]:
        """Generate parameters configuration"""
        parameters = {}
        
        for package in packages:
            for param in package.parameters:
                if param.name:
                    parameters[param.name] = {
                        'value': param.value,
                        'type': param.data_type,
                        'description': param.description
                    }
                    
        return {'parameters': parameters}
        
    def _convert_oledb_to_jdbc_url(self, oledb_conn_string: str) -> str:
        """Convert OLE DB connection string to JDBC URL"""
        import re
        
        # Extract server and database
        server_match = re.search(r'Data Source=([^;]+)', oledb_conn_string)
        db_match = re.search(r'Initial Catalog=([^;]+)', oledb_conn_string)
        
        server = server_match.group(1) if server_match else 'localhost'
        database = db_match.group(1) if db_match else 'master'
        
        return f"jdbc:sqlserver://{server}:1433;databaseName={database}"
        
    def _get_dlt_pipeline_template(self) -> str:
        """Get the DLT pipeline template"""
        return """
# Databricks notebook source
# MAGIC %md
# MAGIC # SSIS to Databricks DLT Pipeline
# MAGIC 
# MAGIC This Delta Live Tables pipeline implements the bronze-silver-gold architecture
# MAGIC migrated from SSIS packages.

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Data Ingestion

{% for function in bronze_functions %}
{{ function }}
{% endfor %}

# COMMAND ----------

# MAGIC %md  
# MAGIC ## Silver Layer - Cleaned and Transformed Data

{% for function in silver_functions %}
{{ function }}
{% endfor %}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Business-Ready Data

{% for function in gold_functions %}
{{ function }}
{% endfor %}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality and Monitoring

@dlt.table(
    name="data_quality_metrics",
    comment="Data quality metrics for monitoring"
)
def data_quality_metrics():
    return spark.sql('''
        SELECT 
            table_name,
            COUNT(*) as row_count,
            COUNT(DISTINCT *) as distinct_count,
            current_timestamp() as measurement_time
        FROM (
            {% for table in tables %}
            SELECT '{{ table }}' as table_name, * FROM LIVE.silver_{{ table.lower() }}
            {% if not loop.last %}UNION ALL{% endif %}
            {% endfor %}
        )
        GROUP BY table_name
    ''')
"""