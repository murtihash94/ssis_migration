# SSIS to Databricks Migration Agent

A comprehensive framework for migrating SSIS packages to Databricks workflows with 60-80% efficiency gain in ETL code conversion.

## Features

- **Three-Module Migration Framework**:
  1. Source Code → Source-Target Mapping (STTM)
  2. GenAI Generated STTM → Validated STTM
  3. Validated STTM → Databricks Workflows

- **SSIS Component Support**:
  - `.dtsx` files (SSIS packages)
  - `.conmgr` files (Connection configurations)
  - `.params` files (Project parameters)
  - `.databases` files (Database configurations)

- **Databricks Output**:
  - DLT Pipeline notebooks (Bronze/Silver/Gold layers)
  - Databricks Asset Bundle workflows
  - Data extraction notebooks
  - Configuration files

- **🆕 Web Application**:
  - User-friendly web interface
  - Drag-and-drop file upload
  - Real-time migration progress
  - Tabbed results display
  - Download generated files

## Installation

```bash
pip install -r requirements.txt
python setup.py install
```

## Usage

### Option 1: Web Application (Recommended)

Start the web application for an easy-to-use interface:

```bash
python start_webapp.py
```

Then open your browser and navigate to `http://localhost:5000`

Features:
- Upload SSIS files via drag-and-drop interface
- View migration progress in real-time
- Browse results in organized tabs
- Download individual files or complete migration package

### Option 2: Command Line Interface

```bash
# Migrate entire SSIS project
ssis-migrate --project-path . --output-path ./databricks_output

# Migrate specific package
ssis-migrate --package "ODS - Customers.dtsx" --output-path ./output

# Generate only STTM
ssis-migrate --project-path . --sttm-only --output ./mappings.csv
```

## Migration Process

1. **Parse SSIS Files**: Extract components from .dtsx, .conmgr, .params files
2. **Generate STTM**: Create source-to-target mappings
3. **Validate Mappings**: Flag areas needing human review
4. **Generate Databricks Assets**: Create notebooks, workflows, and configurations

## Output Structure

```
databricks_output/
├── mappings/
│   └── source_target_mapping.csv
├── notebooks/
│   ├── dlt_pipeline.py
│   ├── data_extraction/
│   └── post_processing/
├── workflows/
│   └── databricks_workflow.yml
└── config/
    ├── connections.yaml
    └── parameters.yaml
```

## Success Metrics

- **60-80% efficiency gain** in ETL code conversion
- **50-60% overall improvement** in migration efficiency
- **95%+ completeness** of SSIS components mapped