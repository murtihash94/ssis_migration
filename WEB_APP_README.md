# SSIS to Databricks Migration Web Application

This web application provides a user-friendly interface for migrating SSIS packages to Databricks workflows.

## Features

- **File Upload Interface**: Upload multiple SSIS files (.dtsx, .conmgr, .params, .database, .dtproj)
- **Migration Processing**: Automatically processes uploaded files using the SSIS migration agent
- **Results Display**: View migration results in organized tabs:
  - Source-Target Mappings table
  - Generated DLT Pipeline code
  - Databricks Workflow YAML
  - List of generated files with download links
- **Download Options**: Download individual files or complete migration results as ZIP

## Quick Start

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Web Application**:
   ```bash
   python web_app.py
   ```

3. **Access the Application**:
   Open your browser and navigate to `http://localhost:5000`

4. **Upload SSIS Files**:
   - Click "Select SSIS Files" and choose your SSIS package files
   - Supported file types: .dtsx, .conmgr, .params, .database, .dtproj
   - Click "Start Migration" to begin the conversion process

5. **View Results**:
   - After processing, click "View Results" to see the migration output
   - Browse through different tabs to see mappings, generated code, and files
   - Download individual files or the complete results package

## Application Structure

```
/
├── web_app.py              # Flask application main file
├── templates/
│   ├── index.html          # Upload interface
│   └── results.html        # Results display with tabs
├── static/
│   ├── css/style.css       # Custom styling
│   └── js/app.js           # JavaScript functionality
├── uploads/                # Temporary upload storage (created automatically)
└── migration_outputs/      # Migration results storage (created automatically)
```

## API Endpoints

- `GET /` - Main upload interface
- `POST /upload` - Handle file upload and migration processing
- `GET /results/<session_id>` - View migration results
- `GET /download/<session_id>` - Download complete results as ZIP
- `GET /download/<session_id>/<file_path>` - Download specific file

## Migration Process

1. **File Upload**: Users upload SSIS package files via the web interface
2. **Processing**: The application uses the existing SSIS migration agent to:
   - Parse SSIS packages (.dtsx files)
   - Extract connection managers (.conmgr files)
   - Process parameters (.params files)
   - Generate source-target mappings (STTM)
   - Create Databricks DLT pipelines and workflows
3. **Results**: Generated files are organized and displayed in a tabbed interface
4. **Download**: Users can download individual files or the complete migration package

## Generated Output

The migration process creates:

- **Source-Target Mappings**: CSV file with detailed field mappings
- **DLT Pipeline**: Python notebook implementing Bronze-Silver-Gold architecture
- **Workflow Configuration**: YAML file for Databricks Asset Bundle
- **Configuration Files**: Connection and parameter configurations
- **Migration Log**: Detailed processing log for troubleshooting

## Requirements

- Python 3.7+
- Flask 2.3+
- All dependencies from the main SSIS migration tool

## Security Note

This is a development server implementation. For production use:
- Use a proper WSGI server (e.g., Gunicorn)
- Implement proper file upload validation
- Add authentication and authorization
- Configure proper session management
- Set up secure file storage