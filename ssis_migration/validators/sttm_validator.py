"""
STTM Validator - Validates generated Source-to-Target Mappings
"""

import logging
from typing import List, Dict, Any
import re

from ..models import SourceTargetMapping


class STTMValidator:
    """Validates Source-to-Target Mappings and flags areas needing human review"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def validate_mappings(self, mappings: List[SourceTargetMapping]) -> List[Dict[str, Any]]:
        """
        Validate a list of source-target mappings
        
        Args:
            mappings: List of SourceTargetMapping objects
            
        Returns:
            List of validation results with issues flagged
        """
        self.logger.info(f"Validating {len(mappings)} mappings")
        
        validation_results = []
        
        for i, mapping in enumerate(mappings):
            result = self._validate_single_mapping(mapping, i)
            if result.get('issues'):
                validation_results.append(result)
                
        self.logger.info(f"Found validation issues in {len(validation_results)} mappings")
        return validation_results
        
    def _validate_single_mapping(self, mapping: SourceTargetMapping, index: int) -> Dict[str, Any]:
        """Validate a single mapping"""
        issues = []
        warnings = []
        
        # Check for missing source information
        if not mapping.source_table and not mapping.source_file:
            issues.append("Missing both source_table and source_file")
            
        if not mapping.source_column:
            issues.append("Missing source_column")
            
        # Check for missing target information
        if not mapping.target_table:
            issues.append("Missing target_table")
            
        if not mapping.target_column:
            issues.append("Missing target_column")
            
        # Check for suspicious data type mappings
        data_type_issues = self._validate_data_type(mapping)
        issues.extend(data_type_issues)
        
        # Check for complex transformation rules that need review
        transformation_warnings = self._validate_transformation_rules(mapping)
        warnings.extend(transformation_warnings)
        
        # Check for naming conventions
        naming_warnings = self._validate_naming_conventions(mapping)
        warnings.extend(naming_warnings)
        
        # Check for derived columns without expressions
        if mapping.is_derived and not mapping.transformation_rule:
            warnings.append("Derived column without transformation rule")
            
        result = {
            'mapping_index': index,
            'source_table': mapping.source_table,
            'source_column': mapping.source_column,
            'target_table': mapping.target_table,
            'target_column': mapping.target_column,
            'issues': issues,
            'warnings': warnings,
            'requires_review': len(issues) > 0 or len(warnings) > 2
        }
        
        return result
        
    def _validate_data_type(self, mapping: SourceTargetMapping) -> List[str]:
        """Validate data type mapping"""
        issues = []
        
        if not mapping.data_type:
            issues.append("Missing data_type")
            return issues
            
        # Check for potentially problematic type conversions
        data_type = mapping.data_type.lower()
        source_col = mapping.source_column.lower()
        
        # Date/time columns should have appropriate types
        if any(keyword in source_col for keyword in ['date', 'time', 'created', 'modified', 'updated']):
            if data_type not in ['timestamp', 'date']:
                issues.append(f"Date/time column '{mapping.source_column}' mapped to non-temporal type '{mapping.data_type}'")
                
        # ID columns should be numeric or string
        if any(keyword in source_col for keyword in ['id', '_key', 'key_']):
            if data_type not in ['integer', 'long', 'string']:
                issues.append(f"ID column '{mapping.source_column}' mapped to unexpected type '{mapping.data_type}'")
                
        # Amount/currency columns should be numeric
        if any(keyword in source_col for keyword in ['amount', 'price', 'cost', 'value', 'total']):
            if data_type not in ['double', 'decimal', 'float']:
                issues.append(f"Amount column '{mapping.source_column}' mapped to non-numeric type '{mapping.data_type}'")
                
        return issues
        
    def _validate_transformation_rules(self, mapping: SourceTargetMapping) -> List[str]:
        """Validate transformation rules"""
        warnings = []
        
        if not mapping.transformation_rule:
            return warnings
            
        rule = mapping.transformation_rule.lower()
        
        # Flag complex expressions that may need manual review
        complex_patterns = [
            r'case\s+when',  # CASE statements
            r'substring\s*\(',  # String functions
            r'datepart\s*\(',  # Date functions
            r'cast\s*\(',  # Type casting
            r'convert\s*\(',  # Type conversion
            r'isnull\s*\(',  # NULL handling
            r'coalesce\s*\(',  # NULL coalescing
        ]
        
        for pattern in complex_patterns:
            if re.search(pattern, rule):
                warnings.append(f"Complex transformation rule may need review: {mapping.transformation_rule}")
                break
                
        # Flag SQL functions that may not have direct Spark equivalents
        sql_functions = ['newid()', 'getdate()', 'sysdatetime()', 'user_name()', 'suser_name()']
        for func in sql_functions:
            if func in rule:
                warnings.append(f"SQL function '{func}' in transformation rule may need Spark equivalent")
                
        return warnings
        
    def _validate_naming_conventions(self, mapping: SourceTargetMapping) -> List[str]:
        """Validate naming conventions"""
        warnings = []
        
        # Check for reserved keywords
        spark_reserved_keywords = [
            'select', 'from', 'where', 'group', 'order', 'having', 'union', 'join',
            'inner', 'outer', 'left', 'right', 'on', 'as', 'distinct', 'all',
            'and', 'or', 'not', 'in', 'exists', 'between', 'like', 'is', 'null',
            'true', 'false', 'case', 'when', 'then', 'else', 'end'
        ]
        
        target_col_lower = mapping.target_column.lower()
        if target_col_lower in spark_reserved_keywords:
            warnings.append(f"Target column '{mapping.target_column}' is a reserved keyword")
            
        # Check for problematic characters
        if re.search(r'[^a-zA-Z0-9_]', mapping.target_column):
            warnings.append(f"Target column '{mapping.target_column}' contains special characters")
            
        # Check for very long names
        if len(mapping.target_column) > 100:
            warnings.append(f"Target column name is very long: '{mapping.target_column}'")
            
        return warnings
        
    def generate_validation_report(self, validation_results: List[Dict[str, Any]]) -> str:
        """Generate a formatted validation report"""
        report_lines = [
            "SSIS to Databricks Migration - STTM Validation Report",
            "=" * 60,
            ""
        ]
        
        if not validation_results:
            report_lines.extend([
                "✅ All mappings passed validation!",
                "No issues found that require human review.",
                ""
            ])
            return "\n".join(report_lines)
            
        # Summary
        total_issues = sum(len(result['issues']) for result in validation_results)
        total_warnings = sum(len(result['warnings']) for result in validation_results)
        mappings_need_review = sum(1 for result in validation_results if result['requires_review'])
        
        report_lines.extend([
            f"📊 Summary:",
            f"   • {len(validation_results)} mappings with validation findings",
            f"   • {total_issues} critical issues",
            f"   • {total_warnings} warnings",
            f"   • {mappings_need_review} mappings require human review",
            ""
        ])
        
        # Detailed findings
        if total_issues > 0:
            report_lines.extend([
                "🚨 Critical Issues (require immediate attention):",
                ""
            ])
            
            for result in validation_results:
                if result['issues']:
                    report_lines.extend([
                        f"Mapping #{result['mapping_index']}:",
                        f"   Source: {result['source_table']}.{result['source_column']}",
                        f"   Target: {result['target_table']}.{result['target_column']}",
                        "   Issues:"
                    ])
                    
                    for issue in result['issues']:
                        report_lines.append(f"     • {issue}")
                    report_lines.append("")
                    
        if total_warnings > 0:
            report_lines.extend([
                "⚠️  Warnings (recommended for review):",
                ""
            ])
            
            for result in validation_results:
                if result['warnings']:
                    report_lines.extend([
                        f"Mapping #{result['mapping_index']}:",
                        f"   Source: {result['source_table']}.{result['source_column']}",
                        f"   Target: {result['target_table']}.{result['target_column']}",
                        "   Warnings:"
                    ])
                    
                    for warning in result['warnings']:
                        report_lines.append(f"     • {warning}")
                    report_lines.append("")
                    
        # Recommendations
        report_lines.extend([
            "🔧 Recommendations:",
            "   • Review all critical issues before proceeding with migration",
            "   • Consider manual verification for mappings with multiple warnings",
            "   • Test data type conversions in a development environment",
            "   • Validate complex transformation rules against business requirements",
            ""
        ])
        
        return "\n".join(report_lines)