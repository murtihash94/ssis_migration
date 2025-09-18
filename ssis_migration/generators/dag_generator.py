"""
DAG Generator - Creates Directed Acyclic Graph visualizations from SSIS package dependencies
"""

import logging
import json
from typing import List, Dict, Any, Set
from pathlib import Path

from ..models import SSISPackage


class DAGGenerator:
    """Generates DAG visualizations from SSIS package dependencies"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def generate_dag_data(self, packages: List[SSISPackage]) -> Dict[str, Any]:
        """
        Generate DAG data structure for visualization
        
        Args:
            packages: List of parsed SSIS packages
            
        Returns:
            Dictionary containing nodes and edges for DAG visualization
        """
        self.logger.info("Generating DAG data from SSIS packages")
        
        nodes = []
        edges = []
        node_ids = set()
        
        # Process each package
        for package in packages:
            # Add package as a root node
            package_node_id = f"package_{package.name}"
            if package_node_id not in node_ids:
                nodes.append({
                    'id': package_node_id,
                    'label': package.name,
                    'type': 'package',
                    'shape': 'box',
                    'color': '#4a90e2',
                    'size': 20
                })
                node_ids.add(package_node_id)
            
            # Add executable nodes
            for executable in package.executables:
                exec_node_id = f"exec_{executable.ref_id}"
                if exec_node_id not in node_ids:
                    nodes.append({
                        'id': exec_node_id,
                        'label': executable.name or executable.ref_id,
                        'type': 'executable',
                        'creation_name': executable.creation_name,
                        'shape': self._get_executable_shape(executable.creation_name),
                        'color': self._get_executable_color(executable.creation_name),
                        'size': 15,
                        'package': package.name
                    })
                    node_ids.add(exec_node_id)
                
                # Link executable to package
                edges.append({
                    'from': package_node_id,
                    'to': exec_node_id,
                    'type': 'contains',
                    'color': '#cccccc',
                    'width': 1
                })
            
            # Add data flow component nodes
            for data_flow in package.data_flows:
                df_node_id = f"dataflow_{data_flow.name}"
                if df_node_id not in node_ids:
                    nodes.append({
                        'id': df_node_id,
                        'label': data_flow.name,
                        'type': 'data_flow',
                        'shape': 'diamond',
                        'color': '#f39c12',
                        'size': 18,
                        'package': package.name
                    })
                    node_ids.add(df_node_id)
                
                # Add data flow components
                for component in data_flow.components:
                    comp_node_id = f"comp_{component.get('id', component.get('name', ''))}"
                    if comp_node_id not in node_ids:
                        nodes.append({
                            'id': comp_node_id,
                            'label': component.get('name', component.get('id', '')),
                            'type': 'component',
                            'component_class': component.get('componentClassID', ''),
                            'shape': self._get_component_shape(component.get('componentClassID', '')),
                            'color': self._get_component_color(component.get('componentClassID', '')),
                            'size': 12,
                            'data_flow': data_flow.name,
                            'package': package.name
                        })
                        node_ids.add(comp_node_id)
                    
                    # Link component to data flow
                    edges.append({
                        'from': df_node_id,
                        'to': comp_node_id,
                        'type': 'contains',
                        'color': '#cccccc',
                        'width': 1
                    })
                
                # Add data flow paths
                for path in data_flow.paths:
                    start_id = f"comp_{path.get('startId', '')}"
                    end_id = f"comp_{path.get('endId', '')}"
                    
                    if start_id in node_ids and end_id in node_ids:
                        edges.append({
                            'from': start_id,
                            'to': end_id,
                            'type': 'data_flow',
                            'color': '#e74c3c',
                            'width': 2,
                            'arrows': 'to'
                        })
            
            # Add dependency edges
            for dependency in package.dependencies:
                dep_type = dependency.get('type', 'unknown')
                from_id = dependency.get('from', '')
                to_id = dependency.get('to', '')
                
                # Map dependency IDs to node IDs
                from_node_id = self._map_dependency_id_to_node_id(from_id, package.name)
                to_node_id = self._map_dependency_id_to_node_id(to_id, package.name)
                
                if from_node_id and to_node_id and from_node_id in node_ids and to_node_id in node_ids:
                    edges.append({
                        'from': from_node_id,
                        'to': to_node_id,
                        'type': dep_type,
                        'color': self._get_dependency_color(dep_type),
                        'width': self._get_dependency_width(dep_type),
                        'arrows': 'to',
                        'label': dependency.get('value', ''),
                        'title': f"{dep_type}: {dependency.get('value', '')}"
                    })
        
        # Calculate statistics
        stats = self._calculate_dag_stats(nodes, edges)
        
        dag_data = {
            'nodes': nodes,
            'edges': edges,
            'stats': stats,
            'layout': {
                'hierarchical': {
                    'enabled': True,
                    'direction': 'UD',  # Up-Down
                    'sortMethod': 'directed'
                }
            }
        }
        
        self.logger.info(f"Generated DAG with {len(nodes)} nodes and {len(edges)} edges")
        return dag_data
    
    def _get_executable_shape(self, creation_name: str) -> str:
        """Get shape for executable node based on creation name"""
        if 'Pipeline' in creation_name or 'DataFlow' in creation_name:
            return 'diamond'
        elif 'Script' in creation_name:
            return 'circle'
        elif 'Execute' in creation_name:
            return 'triangle'
        else:
            return 'box'
    
    def _get_executable_color(self, creation_name: str) -> str:
        """Get color for executable node based on creation name"""
        if 'Pipeline' in creation_name or 'DataFlow' in creation_name:
            return '#f39c12'  # Orange for data flows
        elif 'Script' in creation_name:
            return '#9b59b6'  # Purple for scripts
        elif 'Execute' in creation_name:
            return '#e74c3c'  # Red for execute tasks
        elif 'Bulk' in creation_name:
            return '#2ecc71'  # Green for bulk operations
        else:
            return '#7f8c8d'  # Gray for others
    
    def _get_component_shape(self, component_class: str) -> str:
        """Get shape for component node based on component class"""
        if 'Source' in component_class:
            return 'triangle'
        elif 'Destination' in component_class:
            return 'triangleDown'
        elif 'Transformation' in component_class:
            return 'diamond'
        else:
            return 'circle'
    
    def _get_component_color(self, component_class: str) -> str:
        """Get color for component node based on component class"""
        if 'Source' in component_class:
            return '#27ae60'  # Green for sources
        elif 'Destination' in component_class:
            return '#e74c3c'  # Red for destinations
        elif 'Transformation' in component_class:
            return '#3498db'  # Blue for transformations
        else:
            return '#95a5a6'  # Light gray for others
    
    def _get_dependency_color(self, dep_type: str) -> str:
        """Get color for dependency edge based on type"""
        if dep_type == 'control_flow':
            return '#2c3e50'  # Dark blue for control flow
        elif dep_type == 'data_flow':
            return '#e74c3c'  # Red for data flow
        elif dep_type == 'package_dependency':
            return '#8e44ad'  # Purple for package dependencies
        else:
            return '#7f8c8d'  # Gray for unknown
    
    def _get_dependency_width(self, dep_type: str) -> int:
        """Get width for dependency edge based on type"""
        if dep_type == 'control_flow':
            return 3
        elif dep_type == 'data_flow':
            return 2
        elif dep_type == 'package_dependency':
            return 4
        else:
            return 1
    
    def _map_dependency_id_to_node_id(self, dep_id: str, package_name: str) -> str:
        """Map dependency ID to node ID"""
        if not dep_id:
            return ''
        
        # Try different mapping strategies
        if dep_id.startswith('{') and dep_id.endswith('}'):
            # GUID format - likely an executable ref ID
            return f"exec_{dep_id}"
        elif '\\' in dep_id:
            # Path format - likely a component path
            return f"comp_{dep_id.split('\\')[-1]}"
        else:
            # Direct name - try as component first, then executable
            return f"comp_{dep_id}"
    
    def _calculate_dag_stats(self, nodes: List[Dict], edges: List[Dict]) -> Dict[str, Any]:
        """Calculate DAG statistics"""
        stats = {
            'total_nodes': len(nodes),
            'total_edges': len(edges),
            'node_types': {},
            'edge_types': {},
            'packages': set(),
            'max_depth': 0
        }
        
        # Count node types
        for node in nodes:
            node_type = node.get('type', 'unknown')
            stats['node_types'][node_type] = stats['node_types'].get(node_type, 0) + 1
            if 'package' in node:
                stats['packages'].add(node['package'])
        
        # Count edge types
        for edge in edges:
            edge_type = edge.get('type', 'unknown')
            stats['edge_types'][edge_type] = stats['edge_types'].get(edge_type, 0) + 1
        
        stats['packages'] = list(stats['packages'])
        
        return stats
    
    def generate_dag_html(self, dag_data: Dict[str, Any]) -> str:
        """
        Generate HTML for DAG visualization using vis.js
        
        Args:
            dag_data: DAG data structure
            
        Returns:
            HTML string for rendering DAG
        """
        html_template = """
        <div id="dag-network" style="width: 100%; height: 600px; border: 1px solid #ccc;"></div>
        
        <div class="dag-controls mt-3">
            <div class="row">
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header">
                            <h6><i class="fas fa-info-circle"></i> DAG Statistics</h6>
                        </div>
                        <div class="card-body">
                            <div class="row">
                                <div class="col-6">
                                    <small class="text-muted">Total Nodes:</small>
                                    <div class="fw-bold">{total_nodes}</div>
                                </div>
                                <div class="col-6">
                                    <small class="text-muted">Total Edges:</small>
                                    <div class="fw-bold">{total_edges}</div>
                                </div>
                            </div>
                            <div class="row mt-2">
                                <div class="col-12">
                                    <small class="text-muted">Packages:</small>
                                    <div class="fw-bold">{packages_count}</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header">
                            <h6><i class="fas fa-palette"></i> Legend</h6>
                        </div>
                        <div class="card-body">
                            <div class="d-flex flex-wrap gap-2">
                                <span class="badge" style="background-color: #4a90e2;">Package</span>
                                <span class="badge" style="background-color: #f39c12;">Data Flow</span>
                                <span class="badge" style="background-color: #27ae60;">Source</span>
                                <span class="badge" style="background-color: #e74c3c;">Destination</span>
                                <span class="badge" style="background-color: #3498db;">Transformation</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
        <script>
            // DAG data
            var nodes = new vis.DataSet({dag_nodes});
            var edges = new vis.DataSet({dag_edges});
            
            // Create network
            var container = document.getElementById('dag-network');
            var data = {{ nodes: nodes, edges: edges }};
            var options = {{
                layout: {{
                    hierarchical: {{
                        enabled: true,
                        direction: 'UD',
                        sortMethod: 'directed',
                        nodeSpacing: 150,
                        levelSeparation: 100
                    }}
                }},
                physics: {{
                    hierarchicalRepulsion: {{
                        centralGravity: 0.3,
                        springLength: 100,
                        springConstant: 0.01,
                        nodeDistance: 120,
                        damping: 0.09
                    }},
                    maxVelocity: 50,
                    solver: 'hierarchicalRepulsion',
                    timestep: 0.35,
                    stabilization: {{iterations: 150}}
                }},
                nodes: {{
                    font: {{
                        size: 12,
                        color: '#000000'
                    }},
                    margin: 10,
                    borderWidth: 2,
                    shadow: true
                }},
                edges: {{
                    font: {{
                        size: 10,
                        align: 'middle'
                    }},
                    arrows: {{
                        to: {{
                            enabled: true,
                            scaleFactor: 0.8
                        }}
                    }},
                    smooth: {{
                        type: 'cubicBezier',
                        forceDirection: 'vertical',
                        roundness: 0.4
                    }}
                }},
                interaction: {{
                    dragNodes: true,
                    dragView: true,
                    zoomView: true,
                    selectConnectedEdges: true,
                    hover: true,
                    tooltipDelay: 200
                }}
            }};
            
            var network = new vis.Network(container, data, options);
            
            // Add event listeners
            network.on('selectNode', function(params) {{
                if (params.nodes.length > 0) {{
                    var nodeId = params.nodes[0];
                    var node = nodes.get(nodeId);
                    console.log('Selected node:', node);
                }}
            }});
            
            network.on('hoverNode', function(params) {{
                var nodeId = params.node;
                var node = nodes.get(nodeId);
                var tooltip = 'Type: ' + node.type;
                if (node.creation_name) {{
                    tooltip += '\\nCreation: ' + node.creation_name;
                }}
                if (node.package) {{
                    tooltip += '\\nPackage: ' + node.package;
                }}
                // Note: vis.js doesn't have built-in tooltip, but we could add one
            }});
        </script>
        """
        
        # Format the template with data
        stats = dag_data['stats']
        formatted_html = html_template.format(
            dag_nodes=json.dumps(dag_data['nodes']),
            dag_edges=json.dumps(dag_data['edges']),
            total_nodes=stats['total_nodes'],
            total_edges=stats['total_edges'],
            packages_count=len(stats['packages'])
        )
        
        return formatted_html
    
    def save_dag_data(self, dag_data: Dict[str, Any], output_path: Path) -> str:
        """
        Save DAG data to JSON file
        
        Args:
            dag_data: DAG data structure
            output_path: Output directory path
            
        Returns:
            Path to saved JSON file
        """
        output_file = output_path / "dag_data.json"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(dag_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"DAG data saved to {output_file}")
            return str(output_file)
            
        except Exception as e:
            self.logger.error(f"Failed to save DAG data: {e}")
            raise