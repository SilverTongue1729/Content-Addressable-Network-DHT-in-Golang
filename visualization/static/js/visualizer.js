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

    // Create legend
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
                <div class="legend-color" style="background-color: #2196F3;"></div>
                <span>Data Point</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: #9C27B0;"></div>
                <span>Replica</span>
            </div>
        `);

    // Create status indicators
    const statusIndicator = vizContainer.append("div")
        .attr("class", "status-indicator");

    // Define scales for mapping coordinates (assuming 2D for now, [0,1] space)
    const xScale = d3.scaleLinear().domain([0, 1]).range([0, width]);
    const yScale = d3.scaleLinear().domain([0, 1]).range([0, height]);

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

    // Function to fail a node
    async function failNode(nodeId) {
        try {
            const response = await fetch(`/api/nodes/${nodeId}/fail`, {
                method: 'POST'
            });
            if (response.ok) {
                fetchDataAndDraw();
            }
        } catch (error) {
            console.error('Error failing node:', error);
        }
    }

    // Function to recover a node
    async function recoverNode(nodeId) {
        try {
            const response = await fetch(`/api/nodes/${nodeId}/recover`, {
                method: 'POST'
            });
            if (response.ok) {
                fetchDataAndDraw();
            }
        } catch (error) {
            console.error('Error recovering node:', error);
        }
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

    function fetchDataAndDraw() {
        fetch('/api/state')
            .then(response => response.json())
            .then(data => {
                console.log("Received data:", data);
                drawVisualization(data);
                updateStatusIndicators(data);
                updateNodeSelect(data.nodes);
            })
            .catch(error => console.error('Error fetching state:', error));
    }

    function updateStatusIndicators(data) {
        // Clear previous status items
        statusIndicator.selectAll("*").remove();

        // Count active, failed, and replicating nodes
        const activeNodes = data.nodes.filter(n => !n.failed).length;
        const failedNodes = data.nodes.filter(n => n.failed).length;
        const replicatingNodes = data.replication.filter(r => r.status === "in_progress").length;

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
    }

    function drawVisualization(state) {
        // Clear previous drawings
        svg.selectAll("*").remove();

        // --- Draw Nodes and Zones ---
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
                    status += ` (RECOVERING ${Math.round(d.recoveryProgress * 100)}%)`;
                }
                return status;
            });

        // --- Draw Replication Paths with Animation ---
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

                // Add animated dot along the path
                const dot = svg.append("circle")
                    .attr("class", "replication-dot")
                    .attr("r", 4)
                    .attr("fill", "#9C27B0");

                // Animate the dot along the path
                const pathLength = path.node().getTotalLength();
                dot.attr("transform", `translate(${sourceCenter.x},${sourceCenter.y})`);

                function animateDot() {
                    dot.transition()
                        .duration(2000)
                        .ease(d3.easeLinear)
                        .attrTween("transform", function(d) {
                            return function(t) {
                                const p = path.node().getPointAtLength(t * pathLength);
                                return `translate(${p.x},${p.y})`;
                            };
                        })
                        .on("end", function() {
                            dot.attr("transform", `translate(${sourceCenter.x},${sourceCenter.y})`);
                            animateDot();
                        });
                }

                animateDot();
            }
        });

        // Update tooltips with more information
        nodes.select(".node-rect")
            .on("mouseover", function(event, d) {
                tooltip.transition()
                    .duration(200)
                    .style("opacity", .9);
                
                let status = d.failed ? "Failed" : "Active";
                if (d.recoveryProgress > 0 && d.recoveryProgress < 1) {
                    status = `Recovering (${Math.round(d.recoveryProgress * 100)}%)`;
                }
                
                let failureInfo = "";
                if (d.failed && d.failureTime) {
                    const failureDuration = Math.round((new Date() - new Date(d.failureTime)) / 1000);
                    failureInfo = `<br>Failed ${failureDuration} seconds ago`;
                }
                
                tooltip.html(`
                    Node: ${d.id}<br>
                    Status: ${status}<br>
                    Load: ${(d.load * 100).toFixed(1)}%<br>
                    Last Seen: ${formatTime(d.lastSeen)}${failureInfo}<br>
                    Replicas: ${d.replicas.join(", ") || "None"}
                `)
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 28) + "px");
            })
            .on("mouseout", function() {
                tooltip.transition()
                    .duration(500)
                    .style("opacity", 0);
            });

        // --- Draw Data Points ---
        svg.selectAll(".data-point-group")
            .data(state.data)
            .enter()
            .append("circle")
            .attr("class", d => d.isReplica ? "replica-point" : "data-point")
            .attr("cx", d => {
                const node = state.nodes.find(n => n.id === d.nodeId);
                if (!node || node.failed) return -10;
                const center = getNodeCenter(node);
                return center.x + (Math.random() * 20 - 10); // Add jitter
            })
            .attr("cy", d => {
                const node = state.nodes.find(n => n.id === d.nodeId);
                if (!node || node.failed) return -10;
                const center = getNodeCenter(node);
                return center.y + (Math.random() * 20 - 10); // Add jitter
            })
            .on("mouseover", function(event, d) {
                tooltip.transition()
                    .duration(200)
                    .style("opacity", .9);
                tooltip.html(`
                    Key: ${d.key}<br>
                    Value: ${d.value}<br>
                    Type: ${d.isReplica ? "Replica" : "Primary"}<br>
                    Node: ${d.nodeId}<br>
                    Created: ${formatTime(d.created)}<br>
                    Updated: ${formatTime(d.updated)}
                `)
                    .style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 28) + "px");
            })
            .on("mouseout", function() {
                tooltip.transition()
                    .duration(500)
                    .style("opacity", 0);
            });
    }

    // Initial draw
    fetchDataAndDraw();

    // Refresh data periodically
    setInterval(fetchDataAndDraw, 5000); // Refresh every 5 seconds
}); 