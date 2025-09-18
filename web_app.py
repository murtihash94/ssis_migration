"""
Flask Web Application for SSIS Migration Tool
Provides a web interface for uploading SSIS packages and viewing migration results
"""

import os
import json
import logging
import tempfile
import zipfile
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for
from werkzeug.utils import secure_filename
import shutil

from ssis_migration.core.migration_agent import SSISMigrationAgent

app = Flask(__name__)
app.secret_key = 'ssis_migration_secret_key_12345'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Configure upload folder
UPLOAD_FOLDER = Path('uploads')
UPLOAD_FOLDER.mkdir(exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Configure output folder
OUTPUT_FOLDER = Path('migration_outputs')
OUTPUT_FOLDER.mkdir(exist_ok=True)
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER

# Allowed file extensions
ALLOWED_EXTENSIONS = {'.dtsx', '.conmgr', '.params', '.database', '.dtproj'}

def allowed_file(filename):
    """Check if uploaded file has allowed extension"""
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    """Main page with upload interface"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle file uploads and process migration"""
    
    if 'files' not in request.files:
        return jsonify({'error': 'No files provided'}), 400
    
    files = request.files.getlist('files')
    
    if not files or all(f.filename == '' for f in files):
        return jsonify({'error': 'No files selected'}), 400
    
    # Create unique session directory
    session_id = os.urandom(16).hex()
    session_upload_dir = UPLOAD_FOLDER / session_id
    session_output_dir = OUTPUT_FOLDER / session_id
    
    session_upload_dir.mkdir(exist_ok=True)
    session_output_dir.mkdir(exist_ok=True)
    
    try:
        # Save uploaded files
        uploaded_files = []
        for file in files:
            if file and file.filename and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file_path = session_upload_dir / filename
                file.save(str(file_path))
                uploaded_files.append(filename)
        
        if not uploaded_files:
            return jsonify({'error': 'No valid SSIS files uploaded. Please upload .dtsx, .conmgr, .params, .database, or .dtproj files.'}), 400
        
        # Run migration
        app.logger.info(f"Starting migration for session {session_id}")
        agent = SSISMigrationAgent(
            project_path=str(session_upload_dir),
            output_path=str(session_output_dir),
            validate_mappings=True
        )
        
        # Execute migration
        output_files = agent.migrate_project()
        
        # Get migration summary
        summary = agent.get_migration_summary()
        
        # Prepare response data
        response_data = {
            'session_id': session_id,
            'uploaded_files': uploaded_files,
            'output_files': output_files,
            'summary': summary,
            'status': 'success'
        }
        
        app.logger.info(f"Migration completed successfully for session {session_id}")
        return jsonify(response_data)
        
    except Exception as e:
        app.logger.error(f"Migration failed for session {session_id}: {str(e)}")
        return jsonify({
            'error': f'Migration failed: {str(e)}',
            'session_id': session_id,
            'status': 'error'
        }), 500

@app.route('/results/<session_id>')
def view_results(session_id):
    """View migration results in tabbed interface"""
    session_output_dir = OUTPUT_FOLDER / session_id
    
    if not session_output_dir.exists():
        flash('Session not found or results expired')
        return redirect(url_for('index'))
    
    # Load migration results
    results_data = {}
    
    try:
        # Load STTM mappings if available
        sttm_file = session_output_dir / "mappings" / "source_target_mapping.csv"
        if sttm_file.exists():
            import pandas as pd
            df = pd.read_csv(sttm_file)
            results_data['sttm'] = df.to_html(classes='table table-striped table-hover', table_id='sttm-table')
            results_data['sttm_count'] = len(df)
        
        # Load generated files
        generated_files = []
        for file_path in session_output_dir.rglob('*'):
            if file_path.is_file() and file_path.name != 'migration.log':
                relative_path = file_path.relative_to(session_output_dir)
                generated_files.append({
                    'name': file_path.name,
                    'path': str(relative_path),
                    'size': file_path.stat().st_size,
                    'type': file_path.suffix or 'file'
                })
        
        results_data['generated_files'] = generated_files
        
        # Load migration log
        log_file = session_output_dir / 'migration.log'
        if log_file.exists():
            results_data['log'] = log_file.read_text()
        
        # Load sample generated code (DLT pipeline)
        dlt_file = session_output_dir / "notebooks" / "dlt_pipeline.py"
        if dlt_file.exists():
            results_data['dlt_code'] = dlt_file.read_text()
        
        # Load workflow YAML
        workflow_file = session_output_dir / "workflows" / "databricks_workflow.yml"
        if workflow_file.exists():
            results_data['workflow_yaml'] = workflow_file.read_text()
        
        # Load DAG visualization data
        dag_json_file = session_output_dir / "dag" / "dag_data.json"
        if dag_json_file.exists():
            import json
            with open(dag_json_file, 'r') as f:
                results_data['dag_data'] = json.load(f)
        
        # Load DAG HTML
        dag_html_file = session_output_dir / "dag" / "dag_visualization.html"
        if dag_html_file.exists():
            results_data['dag_html'] = dag_html_file.read_text()
            
    except Exception as e:
        app.logger.error(f"Error loading results for session {session_id}: {str(e)}")
        flash(f'Error loading results: {str(e)}')
        return redirect(url_for('index'))
    
    return render_template('results.html', session_id=session_id, results=results_data)

@app.route('/download/<session_id>')
def download_results(session_id):
    """Download all migration results as ZIP file"""
    session_output_dir = OUTPUT_FOLDER / session_id
    
    if not session_output_dir.exists():
        return jsonify({'error': 'Session not found'}), 404
    
    # Create ZIP file
    zip_path = OUTPUT_FOLDER / f"{session_id}_migration_results.zip"
    
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in session_output_dir.rglob('*'):
                if file_path.is_file():
                    arcname = file_path.relative_to(session_output_dir)
                    zipf.write(file_path, arcname)
        
        return send_file(
            zip_path,
            as_attachment=True,
            download_name=f'ssis_migration_results_{session_id}.zip',
            mimetype='application/zip'
        )
        
    except Exception as e:
        app.logger.error(f"Error creating download for session {session_id}: {str(e)}")
        return jsonify({'error': 'Failed to create download'}), 500

@app.route('/download/<session_id>/<path:file_path>')
def download_file(session_id, file_path):
    """Download specific generated file"""
    session_output_dir = OUTPUT_FOLDER / session_id
    target_file = session_output_dir / file_path
    
    if not target_file.exists() or not target_file.is_file():
        return jsonify({'error': 'File not found'}), 404
    
    try:
        return send_file(
            target_file,
            as_attachment=True,
            download_name=target_file.name
        )
    except Exception as e:
        app.logger.error(f"Error downloading file {file_path} for session {session_id}: {str(e)}")
        return jsonify({'error': 'Failed to download file'}), 500

@app.errorhandler(413)
def too_large(e):
    """Handle file too large error"""
    return jsonify({'error': 'File too large. Maximum size is 16MB.'}), 413

if __name__ == '__main__':
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Get port from environment variable, default to 8080 to avoid common conflicts
    port = int(os.environ.get('FLASK_PORT', 8080))
    
    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port=port)