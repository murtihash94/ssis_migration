"""
SSIS Migration Agent - Main orchestrator for the three-module migration framework
"""

import os
import logging
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd

from ..models import SSISPackage, SourceTargetMapping
from ..parsers.ssis_parser import SSISPackageParser
from ..generators.sttm_generator import STTMGenerator
from ..generators.databricks_generator import DatabricksWorkflowGenerator
from ..generators.dag_generator import DAGGenerator
from ..validators.sttm_validator import STTMValidator


class SSISMigrationAgent:
    """
    Main migration agent that orchestrates the three-module framework:
    1. Source Code → Source-Target Mapping (STTM)
    2. GenAI Generated STTM → Validated STTM
    3. Validated STTM → Databricks Workflows
    """
    
    def __init__(self, project_path: str, output_path: str, validate_mappings: bool = True):
        """
        Initialize the migration agent
        
        Args:
            project_path: Path to SSIS project directory
            output_path: Path for generated Databricks assets
            validate_mappings: Whether to run validation on generated mappings
        """
        self.project_path = Path(project_path)
        self.output_path = Path(output_path)
        self.validate_mappings = validate_mappings
        
        # Initialize components
        self.parser = SSISPackageParser()
        self.sttm_generator = STTMGenerator()
        self.validator = STTMValidator() if validate_mappings else None
        self.databricks_generator = DatabricksWorkflowGenerator()
        self.dag_generator = DAGGenerator()
        
        # Setup logging
        self._setup_logging()
        
        # Migration state
        self.packages: List[SSISPackage] = []
        self.mappings: List[SourceTargetMapping] = []
        
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.output_path / 'migration.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def migrate_project(self, package_filter: Optional[str] = None) -> Dict[str, str]:
        """
        Execute full migration pipeline
        
        Args:
            package_filter: Optional package name filter (e.g., "ODS - Customers.dtsx")
            
        Returns:
            Dictionary with paths to generated files
        """
        self.logger.info("Starting SSIS to Databricks migration")
        
        try:
            # Create output directories
            self._create_output_directories()
            
            # Module 1: Parse SSIS packages and generate STTM
            self.logger.info("Module 1: Parsing SSIS packages and generating STTM")
            self._parse_ssis_packages(package_filter)
            self._generate_sttm()
            
            # Module 2: Validate STTM (if enabled)
            if self.validate_mappings:
                self.logger.info("Module 2: Validating STTM")
                self._validate_sttm()
            
            # Module 3: Generate Databricks assets
            self.logger.info("Module 3: Generating Databricks workflows")
            output_files = self._generate_databricks_assets()
            
            # Module 4: Generate DAG visualization
            self.logger.info("Module 4: Generating DAG visualization")
            dag_files = self._generate_dag_visualization()
            output_files.update(dag_files)
            
            self.logger.info("Migration completed successfully")
            return output_files
            
        except Exception as e:
            self.logger.error(f"Migration failed: {str(e)}")
            raise
            
    def generate_sttm_only(self, package_filter: Optional[str] = None) -> str:
        """
        Generate only STTM without Databricks assets
        
        Args:
            package_filter: Optional package name filter
            
        Returns:
            Path to generated STTM CSV file
        """
        self.logger.info("Generating STTM only")
        
        # Create output directories
        self._create_output_directories()
        
        # Parse packages and generate STTM
        self._parse_ssis_packages(package_filter)
        self._generate_sttm()
        
        # Save STTM to CSV
        sttm_path = self.output_path / "mappings" / "source_target_mapping.csv"
        self._save_sttm_to_csv(sttm_path)
        
        self.logger.info(f"STTM saved to: {sttm_path}")
        return str(sttm_path)
        
    def _create_output_directories(self):
        """Create necessary output directories"""
        dirs = ["mappings", "notebooks", "notebooks/dlt_pipeline", 
                "notebooks/data_extraction", "notebooks/post_processing",
                "workflows", "config"]
        
        for dir_name in dirs:
            (self.output_path / dir_name).mkdir(parents=True, exist_ok=True)
            
    def _parse_ssis_packages(self, package_filter: Optional[str] = None):
        """Parse SSIS packages from project directory"""
        self.logger.info(f"Parsing SSIS packages from: {self.project_path}")
        
        # Find DTSX files
        dtsx_files = list(self.project_path.glob("*.dtsx"))
        
        if package_filter:
            dtsx_files = [f for f in dtsx_files if f.name == package_filter]
            
        if not dtsx_files:
            raise ValueError(f"No DTSX files found in {self.project_path}")
            
        self.logger.info(f"Found {len(dtsx_files)} DTSX files")
        
        # Parse each package
        for dtsx_file in dtsx_files:
            self.logger.info(f"Parsing package: {dtsx_file.name}")
            package = self.parser.parse_package(str(dtsx_file))
            self.packages.append(package)
            
        # Also parse connection managers and parameters
        self._parse_connection_managers()
        self._parse_project_parameters()
        
    def _parse_connection_managers(self):
        """Parse connection manager files"""
        conmgr_files = list(self.project_path.glob("*.conmgr"))
        self.logger.info(f"Found {len(conmgr_files)} connection manager files")
        
        for conmgr_file in conmgr_files:
            connections = self.parser.parse_connection_manager(str(conmgr_file))
            # Add connections to all packages (they are project-level)
            for package in self.packages:
                package.connections.extend(connections)
                
    def _parse_project_parameters(self):
        """Parse project parameter files"""
        param_files = list(self.project_path.glob("*.params"))
        if param_files:
            self.logger.info(f"Found {len(param_files)} parameter files")
            for param_file in param_files:
                parameters = self.parser.parse_parameters(str(param_file))
                # Add parameters to all packages
                for package in self.packages:
                    package.parameters.extend(parameters)
                    
    def _generate_sttm(self):
        """Generate Source-Target Mappings from parsed packages"""
        self.logger.info("Generating source-target mappings")
        
        all_mappings = []
        for package in self.packages:
            package_mappings = self.sttm_generator.generate_mappings(package)
            all_mappings.extend(package_mappings)
            
        self.mappings = all_mappings
        self.logger.info(f"Generated {len(self.mappings)} mappings")
        
    def _validate_sttm(self):
        """Validate generated mappings"""
        if not self.validator:
            return
            
        self.logger.info("Validating source-target mappings")
        validation_results = self.validator.validate_mappings(self.mappings)
        
        # Log validation issues
        for result in validation_results:
            if result.get('issues'):
                self.logger.warning(f"Validation issues found: {result}")
                
    def _generate_databricks_assets(self) -> Dict[str, str]:
        """Generate Databricks workflows and notebooks"""
        output_files = {}
        
        # Generate DLT Pipeline
        dlt_notebook_path = self.output_path / "notebooks" / "dlt_pipeline.py"
        dlt_content = self.databricks_generator.generate_dlt_pipeline(self.mappings, self.packages)
        dlt_notebook_path.write_text(dlt_content)
        output_files['dlt_pipeline'] = str(dlt_notebook_path)
        
        # Generate Databricks Workflow
        workflow_path = self.output_path / "workflows" / "databricks_workflow.yml"
        workflow_content = self.databricks_generator.generate_workflow(self.packages)
        workflow_path.write_text(workflow_content)
        output_files['workflow'] = str(workflow_path)
        
        # Generate data extraction notebooks
        extraction_notebooks = self.databricks_generator.generate_extraction_notebooks(self.packages)
        for notebook_name, content in extraction_notebooks.items():
            notebook_path = self.output_path / "notebooks" / "data_extraction" / f"{notebook_name}.py"
            notebook_path.write_text(content)
            output_files[f'extraction_{notebook_name}'] = str(notebook_path)
            
        # Generate configuration files
        config_files = self.databricks_generator.generate_config_files(self.packages)
        for config_name, content in config_files.items():
            config_path = self.output_path / "config" / f"{config_name}.yaml"
            config_path.write_text(content)
            output_files[f'config_{config_name}'] = str(config_path)
            
        # Save STTM to CSV
        sttm_path = self.output_path / "mappings" / "source_target_mapping.csv"
        self._save_sttm_to_csv(sttm_path)
        output_files['sttm'] = str(sttm_path)
        
        return output_files
    
    def _generate_dag_visualization(self) -> Dict[str, str]:
        """Generate DAG visualization files"""
        output_files = {}
        
        # Generate DAG data
        dag_data = self.dag_generator.generate_dag_data(self.packages)
        
        # Save DAG data as JSON
        dag_json_path = self.output_path / "dag" / "dag_data.json"
        dag_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.dag_generator.save_dag_data(dag_data, dag_json_path.parent)
        output_files['dag_data'] = str(dag_json_path)
        
        # Generate DAG HTML visualization
        dag_html_content = self.dag_generator.generate_dag_html(dag_data)
        dag_html_path = self.output_path / "dag" / "dag_visualization.html"
        dag_html_path.write_text(dag_html_content)
        output_files['dag_html'] = str(dag_html_path)
        
        self.logger.info(f"Generated DAG with {dag_data['stats']['total_nodes']} nodes and {dag_data['stats']['total_edges']} edges")
        
        return output_files
        
    def _save_sttm_to_csv(self, file_path: Path):
        """Save STTM to CSV file"""
        if not self.mappings:
            self.logger.warning("No mappings to save")
            return
            
        # Convert mappings to DataFrame
        mapping_dicts = []
        for mapping in self.mappings:
            mapping_dicts.append({
                'source_table': mapping.source_table,
                'source_file': mapping.source_file,
                'source_column': mapping.source_column,
                'target_table': mapping.target_table,
                'target_column': mapping.target_column,
                'data_type': mapping.data_type,
                'transformation_rule': mapping.transformation_rule,
                'validation_name': mapping.validation_name,
                'transformation_name': mapping.transformation_name,
                'is_derived': mapping.is_derived
            })
            
        df = pd.DataFrame(mapping_dicts)
        df.to_csv(file_path, index=False)
        self.logger.info(f"Saved {len(mapping_dicts)} mappings to {file_path}")
        
    def get_migration_summary(self) -> Dict[str, int]:
        """Get summary statistics of the migration"""
        return {
            'packages_parsed': len(self.packages),
            'mappings_generated': len(self.mappings),
            'connections_found': sum(len(pkg.connections) for pkg in self.packages),
            'executables_found': sum(len(pkg.executables) for pkg in self.packages),
            'data_flows_found': sum(len(pkg.data_flows) for pkg in self.packages)
        }