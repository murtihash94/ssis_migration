"""
Command Line Interface for SSIS Migration Agent
"""

import click
import logging
import os
from pathlib import Path

from .core.migration_agent import SSISMigrationAgent
from .validators.sttm_validator import STTMValidator


@click.command()
@click.option('--project-path', '-p', default='.', 
              help='Path to SSIS project directory (default: current directory)')
@click.option('--output-path', '-o', default='./databricks_output',
              help='Output path for generated Databricks assets')
@click.option('--package', 
              help='Specific package to migrate (e.g., "ODS - Customers.dtsx")')
@click.option('--sttm-only', is_flag=True,
              help='Generate only STTM without Databricks assets')
@click.option('--validate/--no-validate', default=True,
              help='Run validation on generated mappings')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose logging')
@click.option('--output-format', type=click.Choice(['csv', 'json', 'xlsx']), default='csv',
              help='Output format for STTM file')
def main(project_path, output_path, package, sttm_only, validate, verbose, output_format):
    """
    SSIS to Databricks Migration Agent
    
    Migrate SSIS packages to Databricks workflows with comprehensive
    source-to-target mappings and automated asset generation.
    """
    
    # Setup logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    logger = logging.getLogger(__name__)
    logger.info("SSIS to Databricks Migration Agent started")
    
    try:
        # Validate input paths
        project_path = Path(project_path).resolve()
        if not project_path.exists():
            raise click.ClickException(f"Project path does not exist: {project_path}")
            
        output_path = Path(output_path).resolve()
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize migration agent
        agent = SSISMigrationAgent(
            project_path=str(project_path),
            output_path=str(output_path),
            validate_mappings=validate
        )
        
        if sttm_only:
            # Generate only STTM
            logger.info("Generating Source-to-Target Mappings only")
            sttm_file = agent.generate_sttm_only(package_filter=package)
            
            # Convert to different format if requested
            if output_format != 'csv':
                _convert_sttm_format(sttm_file, output_format)
                
            click.echo(f"✅ STTM generated successfully: {sttm_file}")
            
        else:
            # Full migration
            logger.info("Starting full migration process")
            output_files = agent.migrate_project(package_filter=package)
            
            # Display results
            click.echo("✅ Migration completed successfully!")
            click.echo("\nGenerated files:")
            for file_type, file_path in output_files.items():
                click.echo(f"  • {file_type}: {file_path}")
                
        # Display migration summary
        summary = agent.get_migration_summary()
        click.echo(f"\n📊 Migration Summary:")
        click.echo(f"  • Packages processed: {summary['packages_parsed']}")
        click.echo(f"  • Mappings generated: {summary['mappings_generated']}")
        click.echo(f"  • Connections found: {summary['connections_found']}")
        click.echo(f"  • Executables found: {summary['executables_found']}")
        click.echo(f"  • Data flows found: {summary['data_flows_found']}")
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise click.ClickException(f"Migration failed: {str(e)}")


@click.command()
@click.option('--sttm-file', '-f', required=True,
              help='Path to STTM CSV file to validate')
@click.option('--output-report', '-o',
              help='Path to save validation report (optional)')
def validate_sttm(sttm_file, output_report):
    """Validate an existing STTM file"""
    
    import pandas as pd
    from .models import SourceTargetMapping
    
    logger = logging.getLogger(__name__)
    logger.info(f"Validating STTM file: {sttm_file}")
    
    try:
        # Load STTM file
        df = pd.read_csv(sttm_file)
        
        # Convert to SourceTargetMapping objects
        mappings = []
        for _, row in df.iterrows():
            mapping = SourceTargetMapping(
                source_table=row.get('source_table'),
                source_file=row.get('source_file'),
                source_column=row.get('source_column', ''),
                target_table=row.get('target_table', ''),
                target_column=row.get('target_column', ''),
                data_type=row.get('data_type', ''),
                transformation_rule=row.get('transformation_rule'),
                validation_name=row.get('validation_name'),
                transformation_name=row.get('transformation_name'),
                is_derived=bool(row.get('is_derived', False))
            )
            mappings.append(mapping)
            
        # Validate mappings
        validator = STTMValidator()
        validation_results = validator.validate_mappings(mappings)
        
        # Generate report
        report = validator.generate_validation_report(validation_results)
        
        # Display or save report
        if output_report:
            Path(output_report).write_text(report)
            click.echo(f"✅ Validation report saved to: {output_report}")
        else:
            click.echo(report)
            
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        raise click.ClickException(f"Validation failed: {str(e)}")


@click.group()
def cli():
    """SSIS to Databricks Migration Agent CLI"""
    pass


def _convert_sttm_format(csv_file: str, target_format: str):
    """Convert STTM CSV to different format"""
    import pandas as pd
    
    df = pd.read_csv(csv_file)
    base_path = Path(csv_file).parent / Path(csv_file).stem
    
    if target_format == 'json':
        output_file = f"{base_path}.json"
        df.to_json(output_file, orient='records', indent=2)
    elif target_format == 'xlsx':
        output_file = f"{base_path}.xlsx"
        df.to_excel(output_file, index=False, sheet_name='STTM')
        
    return output_file


# Add commands to CLI group
cli.add_command(main, name='migrate')
cli.add_command(validate_sttm, name='validate')


if __name__ == '__main__':
    cli()