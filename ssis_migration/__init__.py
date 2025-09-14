"""
SSIS to Databricks Migration Agent

A comprehensive framework for migrating SSIS packages to Databricks workflows.
"""

__version__ = "1.0.0"
__author__ = "SSIS Migration Team"

from .core.migration_agent import SSISMigrationAgent
from .parsers.ssis_parser import SSISPackageParser
from .generators.sttm_generator import STTMGenerator
from .generators.databricks_generator import DatabricksWorkflowGenerator

__all__ = [
    "SSISMigrationAgent",
    "SSISPackageParser", 
    "STTMGenerator",
    "DatabricksWorkflowGenerator"
]