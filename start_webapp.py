#!/usr/bin/env python3
"""
Quick start script for SSIS Migration Web Application
"""

import sys
import subprocess
import os
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are installed"""
    try:
        import flask
        import pandas
        import xmltodict
        print("✅ All dependencies are installed")
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Please run: pip install -r requirements.txt")
        return False

def main():
    """Main function to start the web application"""
    print("🚀 SSIS to Databricks Migration Web Application")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not Path("web_app.py").exists():
        print("❌ Please run this script from the ssis_migration directory")
        sys.exit(1)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Get port from environment variable, default to 8080 to avoid common conflicts
    port = int(os.environ.get('FLASK_PORT', 8080))
    
    print("📂 Starting web application...")
    print(f"🌐 Open your browser and navigate to: http://localhost:{port}")
    print("🛑 Press Ctrl+C to stop the server")
    print(f"💡 To use a different port, set FLASK_PORT environment variable (e.g., FLASK_PORT=3000)")
    print("-" * 50)
    
    try:
        # Run the web application
        subprocess.run([sys.executable, "web_app.py"], check=True)
    except KeyboardInterrupt:
        print("\n👋 Web application stopped")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error starting web application: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()