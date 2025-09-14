from setuptools import setup, find_packages

setup(
    name="ssis-migration-agent",
    version="1.0.0",
    description="SSIS to Databricks Migration Agent",
    author="SSIS Migration Team",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.5.0",
        "xmltodict>=0.13.0",
        "pyyaml>=6.0",
        "jinja2>=3.1.0",
        "click>=8.1.0",
        "pathlib>=1.0.1",
        "dataclasses>=0.6",
        "typing-extensions>=4.0.0",
    ],
    entry_points={
        'console_scripts': [
            'ssis-migrate=ssis_migration.cli:main',
        ],
    },
    python_requires=">=3.8",
)