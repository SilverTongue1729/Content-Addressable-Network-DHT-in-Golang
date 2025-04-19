document.addEventListener('DOMContentLoaded', () => {
    const vizContainer = d3.select("#visualization");
    const width = 800;
    const height = 600;

    // Create SVG
    const svg = vizContainer.append("svg")
        .attr("width", width)
        .attr("height", height);

    // Create tooltip
    const tooltip = d3.select("body").append("div")
        .attr("class", "tooltip")
        .style("opacity", 0);

    // Create legend with load balancing indicators
    const legend = vizContainer.append("div")
        .attr("class", "legend")
        .html(`
            <div class="legend-item">
                <div class="legend-color" style="background-color: #4CAF50;"></div>
                <span>Active Node</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #F44336;"></div>
                <span>Failed Node</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #FF9800;"></div>
                <span>High Load Node</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #E91E63;"></div>
                <span>Balanced Node</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #2196F3;"></div>
                <span>Data Point</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #9C27B0;"></div>
                <span>Replica</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #FF5722;"></div>
                <span>Hot Key</span>
            </div>
            <div class="legend-item">
                <div class="legend-line"></div>
                <span>Routing Path</span>
            </div>
            <div class="legend-item">
                <div class="legend-line" style="border-top: 2px dashed #673AB7;"></div>
                <span>Zone Transfer</span>
            </div>
        `);

    // Create status indicators
    const statusIndicator = vizContainer.append("div")
        .attr("class", "status-indicator");

    // Define scales for mapping coordinates (assuming 2D for now, [0,1] space)
    const xScale = d3.scaleLinear().domain([0, 1]).range([0, width]);
    const yScale = d3.scaleLinear().domain([0, 1]).range([0, height]);

    // Color scale for heatmap
    const heatColorScale = d3.scaleSequential(d3.interpolateYlOrRd)
        .domain([0, 1]);

    // Operation log
    const operationLog = d3.select("#operation-log");
    const maxLogEntries = 10;
    let logEntries = [];
    
    // Load balancing metrics display
    const loadMetrics = d3.select("#load-metrics");

    // Function to format time
    const formatTime = (date) => {
        return new Date(date).toLocaleTimeString();
    };

    // Function to calculate node center
    const getNodeCenter = (node) => {
        return {
            x: xScale((node.zone[0] + node.zone[2]) / 2),
            y: yScale((node.zone[1] + node.zone[3]) / 2)
        };
    };

    // Function to update node select dropdown
    function updateNodeSelect(nodes) {
        const nodeSelect = document.getElementById('nodeSelect');
        const failBtn = document.getElementById('failNode');
        const recoverBtn = document.getElementById('recoverNode');
        
        // Clear existing options except the first one
        while (nodeSelect.options.length > 1) {
            nodeSelect.remove(1);
        }
        
        // Add nodes to dropdown
        nodes.forEach(node => {
            const option = document.createElement('option');
            option.value = node.id;
            option.textContent = `${node.id} (${node.failed ? 'Failed' : 'Active'})`;
            nodeSelect.appendChild(option);
        });
        
        // Update button states based on selection
        nodeSelect.addEventListener('change', () => {
            const selectedNode = nodes.find(n => n.id === nodeSelect.value);
            if (selectedNode) {
                failBtn.disabled = selectedNode.failed;
                recoverBtn.disabled = !selectedNode.failed;
            } else {
                failBtn.disabled = true;
                recoverBtn.disabled = true;
            }
        });
        
        // Initially disable both buttons
        failBtn.disabled = true;
        recoverBtn.disabled = true;
    }

    // Functions to fail/recover nodes
    async function failNode(nodeId) {
        try {
            const response = await fetch(`/api/nodes/${nodeId}/fail`, {
                method: 'POST'
            });
            if (response.ok) {
                addLogEntry(`Node ${nodeId} failed`, 'failure');
                fetchDataAndDraw();
            }
        } catch (error) {
            console.error('Error failing node:', error);
        }
    }

    async function recoverNode(nodeId) {
        try {
            const response = await fetch(`/api/nodes/${nodeId}/recover`, {
                method: 'POST'
            });
            if (response.ok) {
                addLogEntry(`Node ${nodeId} recovered`, 'recovery');
                fetchDataAndDraw();
            }
        } catch (error) {
            console.error('Error recovering node:', error);
        }
    }

    // Functions for data operations
    async function putData(key, value) {
        try {
            const response = await fetch('/api/data', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ key, value })
            });
            
            if (response.ok) {
                const result = await response.json();
                addLogEntry(`PUT ${key} (routed through ${result.path.join(' → ')})`, 'put');
                animateRoutingPath(result.path, result.coords);
                fetchDataAndDraw();
            }
        } catch (error) {
            console.error('Error putting data:', error);
        }
    }

    async function getData(key) {
        try {
            const response = await fetch(`/api/data/${encodeURIComponent(key)}`);
            if (response.ok) {
                const result = await response.json();
                if (result.exists) {
                    addLogEntry(`GET ${key}: ${result.value} (routed through ${result.path.join(' → ')})`, 'get');
                    animateRoutingPath(result.path, result.coords);
                } else {
                    addLogEntry(`GET ${key}: Not found`, 'not-found');
                }
                fetchDataAndDraw();
            }
        } catch (error) {
            console.error('Error getting data:', error);
        }
    }

    async function deleteData(key) {
        try {
            const response = await fetch(`/api/data/${encodeURIComponent(key)}`, {
                method: 'DELETE'
            });
            if (response.ok) {
                const result = await response.json();
                addLogEntry(`DELETE ${key} (routed through ${result.path.join(' → ')})`, 'delete');
                animateRoutingPath(result.path);
                fetchDataAndDraw();
            }
        } catch (error) {
            console.error('Error deleting data:', error);
        }
    }

    // Add log entry
    function addLogEntry(message, type) {
        const timestamp = new Date().toLocaleTimeString();
        logEntries.unshift({ timestamp, message, type });
        
        // Trim if we have too many entries
        if (logEntries.length > maxLogEntries) {
            logEntries = logEntries.slice(0, maxLogEntries);
        }
        
        updateLogDisplay();
        
        // Add to balance log if it's a balancing event
        if (type === 'balance' || type === 'zone-transfer') {
            const balanceLog = document.getElementById('balance-log');
            if (balanceLog) {
                const entryDiv = document.createElement('div');
                entryDiv.className = `log-entry ${type}`;
                entryDiv.innerHTML = `<span class="log-time">${timestamp}:</span> ${message}`;
                
                // Add to the top of the log
                if (balanceLog.firstChild) {
                    balanceLog.insertBefore(entryDiv, balanceLog.firstChild);
                } else {
                    balanceLog.appendChild(entryDiv);
                }
                
                // Trim the log if it gets too long
                while (balanceLog.children.length > maxLogEntries) {
                    balanceLog.removeChild(balanceLog.lastChild);
                }
            }
        }
    }

    // Update log display
    function updateLogDisplay() {
        operationLog.selectAll("*").remove();
        
        logEntries.forEach(entry => {
            operationLog.append("div")
                .attr("class", `log-entry ${entry.type}`)
                .html(`<span class="log-time">${entry.timestamp}</span> ${entry.message}`);
        });
    }

    // Animate routing path
    function animateRoutingPath(nodePath, coordPath) {
        if (!document.getElementById('showRouting').checked) return;
        
        // Get node states
        fetch('/api/state')
            .then(response => response.json())
            .then(data => {
                const nodes = data.nodes;
                
                // Create path segments between nodes
                const pathSegments = [];
                
                if (coordPath && coordPath.length > 0) {
                    // If we have exact coordinates, use those
                    for (let i = 0; i < nodePath.length - 1; i++) {
                        const sourceNode = nodes.find(n => n.id === nodePath[i]);
                        const targetNode = nodes.find(n => n.id === nodePath[i+1]);
                        
                        if (sourceNode && targetNode) {
                            const sourceCenter = getNodeCenter(sourceNode);
                            const targetCenter = getNodeCenter(targetNode);
                            
                            pathSegments.push({
                                source: sourceCenter,
                                target: targetCenter
                            });
                        }
                    }
                    
                    // Add final segment to the data point
                    const lastNode = nodes.find(n => n.id === nodePath[nodePath.length - 1]);
                    if (lastNode) {
                        const nodeCenter = getNodeCenter(lastNode);
                        const dataPoint = {
                            x: xScale(coordPath[0]),
                            y: yScale(coordPath[1])
                        };
                        
                        pathSegments.push({
                            source: nodeCenter,
                            target: dataPoint
                        });
                    }
                } else {
                    // Just create paths between nodes
                    for (let i = 0; i < nodePath.length - 1; i++) {
                        const sourceNode = nodes.find(n => n.id === nodePath[i]);
                        const targetNode = nodes.find(n => n.id === nodePath[i+1]);
                        
                        if (sourceNode && targetNode) {
                            const sourceCenter = getNodeCenter(sourceNode);
                            const targetCenter = getNodeCenter(targetNode);
                            
                            pathSegments.push({
                                source: sourceCenter,
                                target: targetCenter
                            });
                        }
                    }
                }
                
                // Draw and animate each path segment
                pathSegments.forEach((segment, i) => {
                    // Create path
                    const path = svg.append("path")
                        .attr("class", "routing-path")
                        .attr("d", `M${segment.source.x},${segment.source.y} L${segment.target.x},${segment.target.y}`)
                        .style("stroke-dasharray", function() { 
                            const length = this.getTotalLength();
                            return `${length} ${length}`;
                        })
                        .style("stroke-dashoffset", function() { 
                            return this.getTotalLength();
                        });
                    
                    // Animate path
                    path.transition()
                        .delay(i * 300) // Sequential animation
                        .duration(500)
                        .style("stroke-dashoffset", 0)
                        .transition()
                        .delay(2000) // Keep visible for 2 seconds
                        .duration(500)
                        .style("opacity", 0)
                        .remove();
                    
                    // Animate a dot moving along the path
                    const dot = svg.append("circle")
                        .attr("class", "routing-dot")
                        .attr("r", 5)
                        .attr("cx", segment.source.x)
                        .attr("cy", segment.source.y);
                    
                    dot.transition()
                        .delay(i * 300)
                        .duration(500)
                        .attrTween("cx", function() { 
                            return d3.interpolate(segment.source.x, segment.target.x);
                        })
                        .attrTween("cy", function() {
                            return d3.interpolate(segment.source.y, segment.target.y);
                        })
                        .transition()
                        .duration(100)
                        .remove();
                });
            });
    }

    // Set up button event listeners
    document.getElementById('failNode').addEventListener('click', () => {
        const nodeId = document.getElementById('nodeSelect').value;
        if (nodeId) {
            failNode(nodeId);
        }
    });

    document.getElementById('recoverNode').addEventListener('click', () => {
        const nodeId = document.getElementById('nodeSelect').value;
        if (nodeId) {
            recoverNode(nodeId);
        }
    });
    
    document.getElementById('putData').addEventListener('click', () => {
        const key = document.getElementById('dataKey').value;
        const value = document.getElementById('dataValue').value;
        if (key && value) {
            putData(key, value);
        }
    });
    
    document.getElementById('getData').addEventListener('click', () => {
        const key = document.getElementById('dataKey').value;
        if (key) {
            getData(key);
        }
    });
    
    document.getElementById('deleteData').addEventListener('click', () => {
        const key = document.getElementById('dataKey').value;
        if (key) {
            deleteData(key);
        }
    });
    
    // Set up visualization option event listeners
    document.getElementById('showKeyValuePairs').addEventListener('change', fetchDataAndDraw);
    document.getElementById('showReplicas').addEventListener('change', fetchDataAndDraw);
    document.getElementById('showRouting').addEventListener('change', fetchDataAndDraw);
    document.getElementById('showZones').addEventListener('change', fetchDataAndDraw);
    document.getElementById('showLoadHeatmap').addEventListener('change', fetchDataAndDraw);

    // Functions for load balancing operations
    async function balanceNetwork() {
        try {
            const response = await fetch('/api/loadbalance', {
                method: 'POST'
            });
            if (response.ok) {
                const result = await response.json();
                addLogEntry(`Network load balancing initiated`, 'balance');
                if (result.adjustments && result.adjustments.length > 0) {
                    result.adjustments.forEach(adj => {
                        animateZoneTransfer(adj.sourceNodeId, adj.targetNodeId, adj.oldZone, adj.newZone);
                    });
                }
                fetchDataAndDraw();
            }
        } catch (error) {
            console.error('Error balancing network:', error);
        }
    }

    async function getLoadBalancingMetrics() {
        try {
            const response = await fetch('/api/metrics/loadbalancing');
            if (response.ok) {
                return await response.json();
            }
            return null;
        } catch (error) {
            console.error('Error fetching load balancing metrics:', error);
            return null;
        }
    }

    // Function to animate zone transfer during load balancing
    function animateZoneTransfer(sourceNodeId, targetNodeId, oldZone, newZone) {
        const nodes = currentNetworkState.nodes;
        const sourceNode = nodes.find(n => n.id === sourceNodeId);
        const targetNode = nodes.find(n => n.id === targetNodeId);
        
        if (!sourceNode || !targetNode) return;
        
        const sourceCenter = getNodeCenter(sourceNode);
        const targetCenter = getNodeCenter(targetNode);
        
        // Create a dashed line path between source and target nodes
        const transferPath = svg.append("path")
            .attr("class", "zone-transfer-path")
            .attr("d", `M${sourceCenter.x},${sourceCenter.y} L${targetCenter.x},${targetCenter.y}`)
            .attr("stroke-dasharray", "5,5")
            .attr("stroke", "#673AB7")
            .attr("stroke-width", 2)
            .attr("fill", "none")
            .attr("opacity", 0);
            
        // Animate the path
        transferPath.transition()
            .duration(500)
            .attr("opacity", 1)
            .transition()
            .duration(2000)
            .attr("opacity", 0)
            .remove();
            
        // Display the zone transfer in the log
        addLogEntry(`Zone transfer from ${sourceNodeId} to ${targetNodeId}`, 'zone-transfer');
    }

    // Update the drawVisualization function to include load balancing visualization
    function drawVisualization(state) {
        // Clear previous drawings
        svg.selectAll("*").remove();
        
        // Check visualization options
        const showZones = document.getElementById('showZones').checked;
        const showKeyValues = document.getElementById('showKeyValuePairs').checked;
        const showReplicas = document.getElementById('showReplicas').checked;
        const showHeatmap = document.getElementById('showLoadHeatmap').checked;

        // --- Draw Load Heatmap if enabled ---
        if (showHeatmap) {
            const heatmapCells = [];
            const cellSize = 0.05; // Each cell is 5% of the space
            
            // Create a grid of cells
            for (let x = 0; x < 1; x += cellSize) {
                for (let y = 0; y < 1; y += cellSize) {
                    // Calculate load for this cell
                    const cellLoad = calculateCellLoad(state, x, y, cellSize);
                    
                    heatmapCells.push({
                        x: x,
                        y: y,
                        width: cellSize,
                        height: cellSize,
                        load: cellLoad
                    });
                }
            }
            
            // Draw heatmap cells
            svg.selectAll(".heatmap-cell")
                .data(heatmapCells)
                .enter()
                .append("rect")
                .attr("class", "heatmap-cell")
                .attr("x", d => xScale(d.x))
                .attr("y", d => yScale(d.y))
                .attr("width", d => xScale(d.x + d.width) - xScale(d.x))
                .attr("height", d => yScale(d.y + d.height) - yScale(d.y))
                .style("fill", d => heatColorScale(d.load))
                .style("opacity", 0.5);
        }

        // --- Draw Nodes and Zones ---
        if (showZones) {
            const nodes = svg.selectAll(".node")
                .data(state.nodes)
                .enter()
                .append("g")
                .attr("class", "node");
    
            // Draw node rectangles with recovery progress
            nodes.append("rect")
                .attr("class", d => {
                    let classes = ["node-rect"];
                    if (d.failed) classes.push("failed-node");
                    if (d.load > 0.7) classes.push("high-load");
                    if (d.recoveryProgress > 0 && d.recoveryProgress < 1) classes.push("recovering-node");
                    return classes.join(" ");
                })
                .attr("x", d => xScale(d.zone[0]))
                .attr("y", d => yScale(d.zone[1]))
                .attr("width", d => xScale(d.zone[2]) - xScale(d.zone[0]))
                .attr("height", d => yScale(d.zone[3]) - yScale(d.zone[1]))
                .style("stroke-dasharray", d => d.failed ? "5,5" : "none")
                .style("stroke-width", d => d.failed ? "2" : "1");
    
            // Draw recovery progress bar for recovering nodes
            nodes.filter(d => d.recoveryProgress > 0 && d.recoveryProgress < 1)
                .append("rect")
                .attr("class", "recovery-progress")
                .attr("x", d => xScale(d.zone[0]))
                .attr("y", d => yScale(d.zone[3]) + 5)
                .attr("width", d => (xScale(d.zone[2]) - xScale(d.zone[0])) * d.recoveryProgress)
                .attr("height", "3")
                .style("fill", "#4CAF50");
    
            // Draw node labels with recovery status
            nodes.append("text")
                .attr("class", "node-text")
                .attr("x", d => xScale((d.zone[0] + d.zone[2]) / 2))
                .attr("y", d => yScale((d.zone[1] + d.zone[3]) / 2))
                .attr("dy", "0.35em")
                .text(d => {
                    let status = d.id;
                    if (d.failed) status += " (FAILED)";
                    if (d.recoveryProgress > 0 && d.recoveryProgress < 1) {
                        status += ` (${Math.round(d.recoveryProgress * 100)}%)`;
                    }
                    return status;
                });
        }

        // --- Draw Data Points ---
        if (showKeyValues && state.data) {
            const dataPoints = svg.selectAll(".data-point")
                .data(state.data)
                .enter()
                .append("g")
                .attr("class", "data-point")
                .on("mouseover", function(event, d) {
                    tooltip.transition()
                        .duration(200)
                        .style("opacity", 0.9);
                    tooltip.html(`Key: ${d.key}<br>Value: ${d.value}<br>Node: ${d.nodeId}`)
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY - 28) + "px");
                })
                .on("mouseout", function() {
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                })
                .on("click", function(event, d) {
                    // Update the data panel with details
                    d3.select("#data-info").html(`
                        <p><strong>Key:</strong> ${d.key}</p>
                        <p><strong>Value:</strong> ${d.value}</p>
                        <p><strong>Node:</strong> ${d.nodeId}</p>
                        <p><strong>Coordinates:</strong> [${d.coords[0].toFixed(3)}, ${d.coords[1].toFixed(3)}]</p>
                        <p><strong>Replicas:</strong> ${d.replicaCount || 0}</p>
                    `);
                });
            
            dataPoints.append("circle")
                .attr("class", "data-point-circle")
                .attr("cx", d => xScale(d.coords[0]))
                .attr("cy", d => yScale(d.coords[1]))
                .attr("r", 5)
                .style("fill", "#2196F3");
                
            dataPoints.append("text")
                .attr("class", "data-point-text")
                .attr("x", d => xScale(d.coords[0]) + 8)
                .attr("y", d => yScale(d.coords[1]) - 8)
                .text(d => d.key);
        }
        
        // --- Draw Data Replicas ---
        if (showReplicas && state.replicas) {
            const replicas = svg.selectAll(".replica-point")
                .data(state.replicas)
                .enter()
                .append("g")
                .attr("class", "replica-point")
                .on("mouseover", function(event, d) {
                    tooltip.transition()
                        .duration(200)
                        .style("opacity", 0.9);
                    tooltip.html(`Replica<br>Key: ${d.key}<br>Node: ${d.nodeId}`)
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY - 28) + "px");
                })
                .on("mouseout", function() {
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                });
            
            replicas.append("circle")
                .attr("class", "replica-point-circle")
                .attr("cx", d => xScale(d.coords[0]))
                .attr("cy", d => yScale(d.coords[1]))
                .attr("r", 4)
                .style("fill", "#9C27B0")
                .style("stroke-dasharray", "2,2");
        }

        // --- Draw Replication in Progress ---
        state.replication.forEach(replication => {
            const sourceNode = state.nodes.find(n => n.id === replication.sourceNodeID);
            const targetNode = state.nodes.find(n => n.id === replication.targetNodeID);
            
            if (sourceNode && targetNode) {
                const sourceCenter = getNodeCenter(sourceNode);
                const targetCenter = getNodeCenter(targetNode);
                
                // Draw the replication path
                const path = svg.append("path")
                    .attr("class", "replication-path")
                    .attr("d", `M${sourceCenter.x},${sourceCenter.y} L${targetCenter.x},${targetCenter.y}`);

                // Animate a dot moving along the path
                const dot = svg.append("circle")
                    .attr("class", "replication-dot")
                    .attr("r", 4)
                    .attr("cx", sourceCenter.x)
                    .attr("cy", sourceCenter.y);

                function animateDot() {
                    dot.transition()
                        .duration(1000)
                        .attr("cx", targetCenter.x)
                        .attr("cy", targetCenter.y)
                        .transition()
                        .duration(0)
                        .attr("cx", sourceCenter.x)
                        .attr("cy", sourceCenter.y)
                        .on("end", animateDot);
                }

                animateDot();
            }
        });

        // Draw hot keys if enabled
        if (document.getElementById('showHotKeys').checked) {
            // Collect all hot keys from nodes
            let hotKeys = [];
            state.nodes.forEach(node => {
                if (node.hotKeys && node.hotKeys.length > 0) {
                    node.hotKeys.forEach(key => {
                        // Find the key in data
                        const keyData = state.data.find(d => d.key === key);
                        if (keyData) {
                            hotKeys.push({
                                key: key,
                                x: keyData.x,
                                y: keyData.y,
                                nodeId: node.id
                            });
                        }
                    });
                }
            });

            svg.selectAll(".hot-key")
                .data(hotKeys)
                .join("circle")
                .attr("class", "hot-key")
                .attr("cx", d => xScale(d.x))
                .attr("cy", d => yScale(d.y))
                .attr("r", 8)
                .attr("fill", "#FF5722")
                .attr("stroke", "#FFF")
                .attr("stroke-width", 2)
                .attr("opacity", 0.8)
                .on("mouseover", function(event, d) {
                    tooltip.transition()
                        .duration(200)
                        .style("opacity", .9);
                    tooltip.html(`<strong>Hot Key:</strong> ${d.key}<br><strong>Node:</strong> ${d.nodeId}`)
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY - 28) + "px");
                })
                .on("mouseout", function() {
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                });
        }

        // Update load metrics display
        updateLoadMetricsDisplay(state);
    }

    // Function to update load metrics display
    function updateLoadMetricsDisplay(state) {
        getLoadBalancingMetrics().then(metrics => {
            if (!metrics) return;
            
            loadMetrics.html(`
                <div class="metric">
                    <span class="metric-label">Zone Count:</span>
                    <span class="metric-value">${metrics.zoneCount}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Zone Variance:</span>
                    <span class="metric-value">${metrics.zoneVariance.toFixed(4)}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Load Variance:</span>
                    <span class="metric-value">${metrics.loadVariance.toFixed(4)}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Hot Keys:</span>
                    <span class="metric-value">${metrics.hotKeyCount}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Zone Transfers:</span>
                    <span class="metric-value">${metrics.recentTransfers}</span>
                </div>
            `);
        });
    }

    // Calculate load for a cell in the heatmap
    function calculateCellLoad(state, x, y, cellSize) {
        if (!state.data) return 0;
        
        // Count data points in this cell
        const pointsInCell = state.data.filter(d => {
            return d.coords[0] >= x && d.coords[0] < x + cellSize &&
                   d.coords[1] >= y && d.coords[1] < y + cellSize;
        });
        
        // Also count replicas if available
        let replicasInCell = 0;
        if (state.replicas) {
            replicasInCell = state.replicas.filter(d => {
                return d.coords[0] >= x && d.coords[0] < x + cellSize &&
                       d.coords[1] >= y && d.coords[1] < y + cellSize;
            }).length;
        }
        
        // Calculate load based on points and recent operations
        const cellLoad = (pointsInCell.length + replicasInCell * 0.5) / 
                        Math.max(1, state.data.length); // Normalize by total data points
        
        return Math.min(1, cellLoad * 5); // Scale for better visualization
    }

    // Attach event listeners for load balancing controls
    document.getElementById('balanceNetwork').addEventListener('click', async () => {
        try {
            const response = await fetch('/api/loadbalance', {
                method: 'POST'
            });
            if (response.ok) {
                const result = await response.json();
                addLogEntry(`Network load balancing initiated`, 'balance');
                if (result.adjustments && result.adjustments.length > 0) {
                    result.adjustments.forEach(adj => {
                        animateZoneTransfer(adj.sourceNodeId, adj.targetNodeId, adj.oldZone, adj.newZone);
                    });
                }
                fetchDataAndDraw();
            }
        } catch (error) {
            console.error('Error balancing network:', error);
        }
    });

    document.getElementById('simulateLoad').addEventListener('click', async () => {
        try {
            const response = await fetch('/api/simulate-load', {
                method: 'POST'
            });
            if (response.ok) {
                fetchDataAndDraw();
                addLogEntry(`Load simulation applied to network`, 'load');
            }
        } catch (error) {
            console.error('Error simulating load:', error);
        }
    });

    // Update threshold display when slider changes
    const loadThresholdSlider = document.getElementById('loadThreshold');
    const thresholdValueDisplay = document.getElementById('thresholdValue');
    
    if (loadThresholdSlider && thresholdValueDisplay) {
        loadThresholdSlider.addEventListener('input', () => {
            thresholdValueDisplay.textContent = `${loadThresholdSlider.value}%`;
        });
    }

    // Update visualization when checkboxes change
    document.querySelectorAll('.viz-controls input[type="checkbox"]').forEach(checkbox => {
        checkbox.addEventListener('change', fetchDataAndDraw);
    });

    // Initial fetch and draw
    let currentNetworkState = null;
    fetchDataAndDraw();

    // Function to fetch data and draw visualization
    function fetchDataAndDraw() {
        fetch('/api/state')
            .then(response => response.json())
            .then(state => {
                currentNetworkState = state;
                drawVisualization(state);
                updateNodeSelect(state.nodes);
                updateStatusIndicators(state);
                updateLoadMetricsDisplay(state);
            })
            .catch(error => console.error('Error fetching network state:', error));
    }

    function updateStatusIndicators(data) {
        // Clear previous status items
        statusIndicator.selectAll("*").remove();

        // Count active, failed, and replicating nodes
        const activeNodes = data.nodes.filter(n => !n.failed).length;
        const failedNodes = data.nodes.filter(n => n.failed).length;
        const replicatingNodes = data.replication.filter(r => r.status === "in_progress").length;
        const totalData = data.data ? data.data.length : 0;
        const totalReplicas = data.replicas ? data.replicas.length : 0;

        // Add status items
        statusIndicator.append("div")
            .attr("class", "status-item")
            .html(`
                <div class="status-dot status-active"></div>
                <span>Active Nodes: ${activeNodes}</span>
            `);

        statusIndicator.append("div")
            .attr("class", "status-item")
            .html(`
                <div class="status-dot status-failed"></div>
                <span>Failed Nodes: ${failedNodes}</span>
            `);

        statusIndicator.append("div")
            .attr("class", "status-item")
            .html(`
                <div class="status-dot status-replicating"></div>
                <span>Replicating: ${replicatingNodes}</span>
            `);
            
        statusIndicator.append("div")
            .attr("class", "status-item")
            .html(`
                <div class="status-dot status-data"></div>
                <span>Data Points: ${totalData}</span>
            `);
            
        statusIndicator.append("div")
            .attr("class", "status-item")
            .html(`
                <div class="status-dot status-replica"></div>
                <span>Replicas: ${totalReplicas}</span>
            `);
    }
}); 