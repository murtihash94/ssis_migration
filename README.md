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

- **🆕 Dependencies DAG Visualization**:
  - Interactive Directed Acyclic Graph showing package dependencies
  - Visual representation of control flow, data flow, and package dependencies  
  - Color-coded nodes for different component types
  - Statistics and legend for easy interpretation

- **🆕 Web Application**:
  - User-friendly web interface
  - Drag-and-drop file upload
  - Real-time migration progress
  - Tabbed results display with DAG visualization
  - Download generated files

## Installation

```bash
pip install -r requirements.txt
python setup.py install
```

## Quick Start

### Option 1: Web Application (Recommended) 🌐

The web application provides an intuitive interface for SSIS migration with drag-and-drop file upload and interactive results viewing.

#### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

#### 2. Start the Web Application
```bash
# Using the convenience script (recommended)
python start_webapp.py

# Or run directly
python web_app.py
```

#### 3. Access the Application
Open your browser and navigate to: **http://localhost:8080**

> **Port Configuration**: The application runs on port 8080 by default. To use a different port, set the `FLASK_PORT` environment variable:
> ```bash
> FLASK_PORT=3000 python start_webapp.py
> ```

#### 4. Upload and Process SSIS Files
- **Supported file types**: `.dtsx`, `.conmgr`, `.params`, `.database`, `.dtproj`
- **File size limit**: 16MB per file
- **Multiple files**: Upload entire SSIS projects at once

**Step-by-step process:**
1. Click "Select SSIS Files" or drag files onto the upload area
2. Select your SSIS package files from your project directory
3. Click "Start Migration" to begin the conversion process
4. Wait for processing to complete (progress is shown in real-time)

#### 5. View and Download Results
After processing, you'll see a tabbed interface with:

- **📊 Source-Target Mappings**: Detailed field mappings in table format
- **🐍 DLT Pipeline Code**: Generated Python notebooks with Bronze-Silver-Gold architecture
- **⚙️ Workflow YAML**: Databricks Asset Bundle configuration
- **📁 Generated Files**: Complete list of output files with download links
- **🔗 DAG Visualization**: Interactive dependency graphs showing package relationships

**Download options:**
- Download individual files by clicking file names
- Download complete migration package as ZIP file
- Copy code snippets directly from the interface

#### 6. Advanced Features

**Code Editing**: You can edit transformation code directly in the web interface:
- Navigate to the DLT Pipeline tab
- Edit the code in the text area
- Changes are automatically saved and can be downloaded

**Session Management**: Each migration creates a unique session that persists until server restart:
- Results are accessible via unique session URLs
- Multiple migrations can run simultaneously
- Session data includes all generated files and logs

### Option 2: Command Line Interface

```bash
# Migrate entire SSIS project
ssis-migrate --project-path . --output-path ./databricks_output

# Migrate specific package
ssis-migrate --package "ODS - Customers.dtsx" --output-path ./output

# Generate only STTM
ssis-migrate --project-path . --sttm-only --output ./mappings.csv
```

## Web Application Features

### User Interface
- **Modern responsive design** that works on desktop and mobile devices
- **Drag-and-drop file upload** with visual feedback
- **Real-time progress indicators** during migration processing
- **Tabbed results interface** for organized data viewing
- **Interactive DAG visualization** showing package dependencies
- **Code editor** with syntax highlighting for reviewing and editing generated code

### Technical Capabilities
- **Multi-file processing**: Handle complete SSIS projects with multiple package types
- **Session-based architecture**: Each migration gets a unique session ID for result tracking
- **Automatic directory creation**: Upload and output directories are created automatically
- **Error handling**: Comprehensive error messages and logging for troubleshooting
- **File size management**: 16MB upload limit with clear error messages
- **Security features**: Secure filename handling and path validation

### API Endpoints
The web application exposes several RESTful endpoints:

- `GET /` - Main upload interface
- `POST /upload` - Handle file upload and migration processing
- `GET /results/<session_id>` - View migration results in tabbed interface
- `GET /download/<session_id>` - Download complete results as ZIP file
- `GET /download/<session_id>/<file_path>` - Download specific generated file
- `POST /update_transformation_code/<session_id>` - Update transformation code for objects

### Application Structure
```
ssis_migration/
├── web_app.py              # Flask application main file
├── start_webapp.py         # Convenience script to start the application
├── templates/
│   ├── index.html          # Upload interface with drag-and-drop
│   └── results.html        # Results display with tabs and DAG visualization
├── static/
│   ├── css/style.css       # Custom styling and responsive design
│   └── js/app.js           # JavaScript for file upload and interactions
├── uploads/                # Temporary upload storage (created automatically)
└── migration_outputs/      # Migration results storage (created automatically)
    └── <session_id>/       # Individual session directories
        ├── mappings/       # Source-target mapping files
        ├── notebooks/      # Generated DLT pipeline notebooks
        ├── workflows/      # Databricks workflow configurations
        ├── config/         # Connection and parameter configurations
        └── dag/            # DAG visualization files
```

## Installation and Setup

### Prerequisites
- **Python 3.7+** (Python 3.8+ recommended)
- **Web browser** (Chrome, Firefox, Safari, or Edge)
- **Minimum 1GB RAM** for processing large SSIS projects
- **Storage space** for uploaded files and generated outputs

### Installation Steps

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd ssis_migration
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify installation**:
   ```bash
   python start_webapp.py
   ```

### Environment Configuration

You can customize the application behavior using environment variables:

- `FLASK_PORT`: Set the port number (default: 8080)
  ```bash
  export FLASK_PORT=3000
  python start_webapp.py
  ```

- `FLASK_HOST`: Set the host address (default: 0.0.0.0)
  ```bash
  export FLASK_HOST=127.0.0.1
  python web_app.py
  ```

- `FLASK_DEBUG`: Enable/disable debug mode (default: True)
  ```bash
  export FLASK_DEBUG=False
  python web_app.py
  ```

## Migration Process

1. **Parse SSIS Files**: Extract components from .dtsx, .conmgr, .params files
2. **Generate STTM**: Create source-to-target mappings
3. **Validate Mappings**: Flag areas needing human review
4. **Generate Databricks Assets**: Create notebooks, workflows, and configurations
5. **Generate DAG Visualization**: Create interactive dependency graphs

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
├── config/
│   ├── connections.yaml
│   └── parameters.yaml
└── dag/
    ├── dag_data.json
    └── dag_visualization.html
```

## Troubleshooting

### Common Issues and Solutions

#### Web Application Won't Start
```bash
# Check if dependencies are installed
python -c "import flask, pandas, xmltodict; print('Dependencies OK')"

# If missing dependencies:
pip install -r requirements.txt

# Check if port is already in use
FLASK_PORT=8081 python start_webapp.py
```

#### Upload Errors
- **File too large**: Maximum file size is 16MB per file
- **Unsupported file type**: Only `.dtsx`, `.conmgr`, `.params`, `.database`, `.dtproj` files are supported
- **Permission errors**: Ensure the application has write access to the `uploads/` and `migration_outputs/` directories

#### Processing Errors
- Check the migration log in the results interface
- Verify SSIS files are not corrupted
- Ensure all required connection managers and parameters are included

#### Browser Issues
- Clear browser cache and cookies
- Try a different browser (Chrome, Firefox recommended)
- Disable browser extensions that might interfere
- Check browser console for JavaScript errors

### Debug Mode
To enable verbose logging:
```bash
export FLASK_DEBUG=True
python web_app.py
```

### Log Files
- **Application logs**: Check console output where the web app is running
- **Migration logs**: Available in each session's results interface
- **Error logs**: Displayed in the web interface and console

## Production Deployment

### Security Considerations
⚠️ **Important**: The default configuration is for development only. For production use:

#### 1. Use a Production WSGI Server
```bash
# Install Gunicorn
pip install gunicorn

# Run with Gunicorn
gunicorn -w 4 -b 0.0.0.0:8080 web_app:app
```

#### 2. Implement Security Measures
- **Authentication**: Add user authentication and authorization
- **HTTPS**: Use SSL/TLS encryption for data transmission
- **File validation**: Implement stricter file upload validation
- **Input sanitization**: Validate and sanitize all user inputs
- **Session security**: Configure secure session management
- **Rate limiting**: Implement request rate limiting

#### 3. Environment Configuration
```bash
# Production environment variables
export FLASK_ENV=production
export FLASK_DEBUG=False
export SECRET_KEY="your-production-secret-key"
```

#### 4. Storage Configuration
- **Secure file storage**: Use secure cloud storage or encrypted local storage
- **Backup strategy**: Implement regular backups of migration results
- **Cleanup policies**: Set up automatic cleanup of old upload and output files

#### 5. Monitoring and Logging
- **Application monitoring**: Set up health checks and performance monitoring
- **Error tracking**: Implement error tracking and alerting
- **Audit logging**: Log all user actions and system events

### Docker Deployment
Create a `Dockerfile` for containerized deployment:
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8080

CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8080", "web_app:app"]
```

## Success Metrics

- **60-80% efficiency gain** in ETL code conversion
- **50-60% overall improvement** in migration efficiency
- **95%+ completeness** of SSIS components mapped

## Quick Reference

### Start Web Application
```bash
python start_webapp.py          # Default port 8080
FLASK_PORT=3000 python start_webapp.py  # Custom port
```

### Access Web Interface
- **Default URL**: http://localhost:8080
- **Custom port**: http://localhost:YOUR_PORT

### Supported File Types
- `.dtsx` - SSIS packages
- `.conmgr` - Connection managers
- `.params` - Project parameters
- `.database` - Database configurations
- `.dtproj` - Project files

### Command Line Usage
```bash
ssis-migrate --project-path . --output-path ./output  # Full migration
ssis-migrate --package "file.dtsx" --output ./output  # Single package
ssis-migrate --project-path . --sttm-only --output ./mappings.csv  # STTM only
```

### Common Environment Variables
```bash
FLASK_PORT=8080     # Web server port
FLASK_DEBUG=True    # Enable debug mode
FLASK_HOST=0.0.0.0  # Server host address
```