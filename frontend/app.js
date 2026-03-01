// Application State
const appState = {
    mode: 'discovery', // 'discovery' or 'verification'
    currentJobId: null,
    eventStream: null,
    files: new Map(), // Map to store selected files
    userId: CONFIG.API.userId,
    apiBaseUrl: CONFIG.API.baseUrl,
    graphInstance: null, // Store Vis.js network instance
};

// DOM Elements
const modeToggle = document.getElementById('modeToggle');
const contentArea = document.getElementById('contentArea');
let readyMessage = document.getElementById('readyMessage');
const inputForm = document.getElementById('inputForm');
const discoveryForm = document.getElementById('discoveryForm');
const verificationForm = document.getElementById('verificationForm');
const contentInput = document.getElementById('contentInput');
const fileInput = document.getElementById('fileInput');
const fileList = document.getElementById('fileList');
const statusMessage = document.getElementById('statusMessage');
const statusText = document.getElementById('statusText');
const entity1Input = document.getElementById('entity1Input');
const entity2Input = document.getElementById('entity2Input');
const clearBtn = document.getElementById('clearBtn');

// ============= MODE SWITCHING =============
modeToggle.addEventListener('change', () => {
    appState.mode = appState.mode === 'discovery' ? 'verification' : 'discovery';
    switchMode();
});

function switchMode() {
    // Stop any running SSE stream
    stopEventStream();
    
    // Completely clear content area
    contentArea.innerHTML = '';
    
    // Reset form and fields
    resetForm();
    
    // Clear all state
    appState.currentJobId = null;
    
    // Switch forms and UI based on mode
    if (appState.mode === 'discovery') {
        discoveryForm.classList.remove('hidden');
        verificationForm.classList.add('hidden');
        
        // Create discovery ready message
        const readyDiv = document.createElement('div');
        readyDiv.className = 'ready-message';
        readyDiv.id = 'readyMessage';
        readyDiv.innerHTML = `
            <div class="ready-icon">🔍</div>
            <h2>Ready for Discovery Mode</h2>
            <p>Upload documents and research topics to begin analysis</p>
        `;
        contentArea.appendChild(readyDiv);
        readyMessage = readyDiv;
        
    } else {
        discoveryForm.classList.add('hidden');
        verificationForm.classList.remove('hidden');
        
        // Create verification ready message
        const readyDiv = document.createElement('div');
        readyDiv.className = 'ready-message';
        readyDiv.id = 'readyMessage';
        readyDiv.innerHTML = `
            <div class="ready-icon">✓</div>
            <h2>Ready for Verification Mode</h2>
            <p>Enter two entities to verify their connection</p>
        `;
        contentArea.appendChild(readyDiv);
        readyMessage = readyDiv;
    }
    
    // Re-enable form
    enableForm();
}

// ============= FILE HANDLING =============
fileInput.addEventListener('change', (e) => {
    // Don't clear! Add to existing files instead
    const filesArray = Array.from(e.target.files);
    console.log(`📁 Adding ${filesArray.length} file(s):`, filesArray.map(f => f.name));
    
    filesArray.forEach((file) => {
        appState.files.set(file.name, file);
        addFileToList(file.name);
    });
    
    // Reset input value so same file can be selected again if needed
    fileInput.value = '';
    
    // Log total files
    console.log(`📦 Total files in queue: ${appState.files.size}`);
});

function addFileToList(fileName) {
    // Check if file already exists in list
    const existingItem = fileList.querySelector(`[data-filename="${fileName}"]`);
    if (existingItem) {
        console.log(`⚠️ File already in list: ${fileName}`);
        return;
    }
    
    const fileItem = document.createElement('div');
    fileItem.className = 'file-item';
    fileItem.setAttribute('data-filename', fileName);
    fileItem.innerHTML = `
        <span>${fileName}</span>
        <span class="file-remove" data-filename="${fileName}">✕</span>
    `;
    fileList.appendChild(fileItem);
    
    fileItem.querySelector('.file-remove').addEventListener('click', () => {
        appState.files.delete(fileName);
        fileItem.remove();
        console.log(`🗑️ Removed: ${fileName} (${appState.files.size} remaining)`);
    });
}

// ============= FORM HANDLING =============
inputForm.addEventListener('submit', handleFormSubmit);

async function handleFormSubmit(e) {
    e.preventDefault();
    
    if (appState.mode === 'discovery') {
        await handleDiscoverySubmit();
    } else {
        await handleVerificationSubmit();
    }
}

async function handleDiscoverySubmit() {
    const content = contentInput.value.trim();
    
    if (!content && appState.files.size === 0) {
        alert('Please enter text or select files');
        return;
    }
    
    // Create user input block
    addUserBlock(content, appState.files);
    
    const formData = new FormData();
    
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('📮 SUBMISSION DATA:');
    
    if (appState.currentJobId) {
        formData.append('job_id', appState.currentJobId);
        console.log(`  job_id: ${appState.currentJobId}`);
    }
    
    if (content) {
        formData.append('content', content);
        console.log(`  content: "${content}"`);
    }
    
    // Add all files to FormData
    const filesArray = Array.from(appState.files.values());
    console.log(`📤 FILES (${filesArray.length} total):`);
    
    filesArray.forEach((file, index) => {
        console.log(`  [${index + 1}] ${file.name} (${(file.size / 1024).toFixed(2)} KB)`);
        formData.append('files', file);
    });
    
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    
    try {
        disableForm('Discovery in progress...');
        
        const response = await fetch(`${appState.apiBaseUrl}/`, {
            method: 'POST',
            body: formData,
        });
        
        const data = await response.json();
        console.log('✅ Backend responded with job:', data.job_id);
        console.log('🔄 Workflow triggered:', data.workflow_triggered || 'None (pure conversation)');
        
        appState.currentJobId = data.job_id;
        
        contentInput.value = '';
        fileInput.value = '';
        appState.files.clear();
        fileList.innerHTML = '';
        
        // Clear ready message if it's the first submission
        if (!contentArea.querySelector('.event-block')) {
            readyMessage.style.display = 'none';
        }
        
        // If API response contains answer, display it in separate block
        if (data.answer) {
            addAnswerBlock(data.answer);
        }
        
        // Only start SSE stream if a workflow was triggered (not pure conversation)
        if (data.workflow_triggered) {
            // Workflow started (fetch, ingestion, extraction)
            startEventStream();
        } else {
            // Pure conversation - no workflow, just enable form
            console.log('✅ Conversational response complete');
            enableForm();
            statusMessage.classList.add('hidden');
        }
        
    } catch (error) {
        console.error('Error submitting discovery:', error);
        alert('Error submitting request. Please try again.');
        enableForm();
    }
}

async function handleVerificationSubmit() {
    const entity1 = entity1Input.value.trim();
    const entity2 = entity2Input.value.trim();
    
    if (!entity1 || !entity2) {
        alert('Please enter both entities');
        return;
    }
    
    // Create user input block for verification
    addVerificationUserBlock(entity1, entity2);
    
    try {
        disableForm('Verification in progress...');
        
        const formData = new FormData();
        formData.append('entity1', entity1);
        formData.append('entity2', entity2);
        
        const response = await fetch(`${appState.apiBaseUrl}/verify/`, {
            method: 'POST',
            body: formData,
        });
        
        const data = await response.json();
        console.log('✅ Backend responded with job:', data.job_id);
        console.log('🔄 Workflow triggered:', data.workflow_triggered || 'None (pure conversation)');
        
        appState.currentJobId = data.job_id;
        console.log('📌 Set currentJobId to:', appState.currentJobId);
        
        entity1Input.value = '';
        entity2Input.value = '';
        
        // Clear ready message
        if (!contentArea.querySelector('.event-block')) {
            readyMessage.style.display = 'none';
        }
        
        // If API response contains answer, display it in separate block
        if (data.answer) {
            addAnswerBlock(data.answer);
        }
        
        // Verification mode ALWAYS has a workflow - no conversational mode
        console.log('🚀 STARTING SSE STREAM FOR VERIFICATION (always has workflow)');
        startEventStream();
        
    } catch (error) {
        console.error('Error submitting verification:', error);
        alert('Error submitting request. Please try again.');
        enableForm();
    }
}

// ============= FORM STATE =============
function disableForm(message = 'System is running...') {
    discoveryForm.style.opacity = '0.5';
    verificationForm.style.opacity = '0.5';
    inputForm.style.pointerEvents = 'none';
    statusText.textContent = message;
    statusMessage.classList.remove('hidden');
}

function enableForm() {
    discoveryForm.style.opacity = '1';
    verificationForm.style.opacity = '1';
    inputForm.style.pointerEvents = 'auto';
    statusMessage.classList.add('hidden');
}

function resetForm() {
    // Clear all input fields
    contentInput.value = '';
    entity1Input.value = '';
    entity2Input.value = '';
    fileInput.value = '';
    
    // Clear file list
    appState.files.clear();
    fileList.innerHTML = '';
    
    // Reset form element
    inputForm.reset();
}

// ============= CLEAR BUTTON (VERIFICATION MODE) =============
clearBtn.addEventListener('click', () => {
    clearAllContent();
});

function clearAllContent() {
    // Completely clear content area
    contentArea.innerHTML = '';
    
    // Stop any SSE stream
    stopEventStream();
    
    // Reset state
    appState.currentJobId = null;
    resetForm();
    enableForm();
    
    // Show ready message based on current mode
    if (appState.mode === 'discovery') {
        const readyDiv = document.createElement('div');
        readyDiv.className = 'ready-message';
        readyDiv.id = 'readyMessage';
        readyDiv.innerHTML = `
            <div class="ready-icon">🔍</div>
            <h2>Ready for Discovery Mode</h2>
            <p>Upload documents and research topics to begin analysis</p>
        `;
        contentArea.appendChild(readyDiv);
        readyMessage = readyDiv;
    } else {
        const readyDiv = document.createElement('div');
        readyDiv.className = 'ready-message';
        readyDiv.id = 'readyMessage';
        readyDiv.innerHTML = `
            <div class="ready-icon">✓</div>
            <h2>Ready for Verification Mode</h2>
            <p>Enter two entities to verify their connection</p>
        `;
        contentArea.appendChild(readyDiv);
        readyMessage = readyDiv;
    }
}

// ============= SSE STREAMING =============
function startEventStream() {
    console.log('🔄 [startEventStream] Called!');
    stopEventStream();
    
    const eventUrl = `${appState.apiBaseUrl}/user/${appState.userId}/events/`;
    console.log('🔌 Connecting to event stream:', eventUrl);
    console.log('   Watching for job ID:', appState.currentJobId);
    appState.eventStream = new EventSource(eventUrl);
    
    appState.eventStream.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            console.log('📡 RAW SSE EVENT:', JSON.stringify(data));
            console.log('   Event Job ID:', data.job_id);
            console.log('   Current Job ID:', appState.currentJobId);
            console.log('   IDs Match?:', data.job_id === appState.currentJobId);
            
            // Only process events for current job
            if (data.job_id === appState.currentJobId) {
                console.log('✓ Job ID matches, processing event...');
                processSSEEvent(data);
            } else {
                console.log('✗ Job ID mismatch - Event ignored (expect: ' + appState.currentJobId + ', got: ' + data.job_id + ')');
            }
        } catch (error) {
            console.error('Error parsing SSE event:', error);
            console.log('Raw event data:', event.data);
        }
    };
    
    appState.eventStream.onerror = () => {
        console.error('EventStream error');
        appState.eventStream.close();
        enableForm();
    };
}

function stopEventStream() {
    if (appState.eventStream) {
        appState.eventStream.close();
        appState.eventStream = null;
    }
}

// ============= SSE EVENT PROCESSING =============
function processSSEEvent(eventData) {
    console.log('📥 SSE Event received:', eventData);
    
    // Check if form should be enabled (needs_more_input status)
    if (eventData.status === 'needs_more_input') {
        enableForm();
        statusMessage.classList.add('hidden');
    }
    
    // Check for insufficient signal in discovery mode - enable form for more input
    if (appState.mode === 'discovery' && 
        eventData.status === 'insufficientsignal' && 
        eventData.next_action === 'need_inputs') {
        console.log('⚠️ Insufficient signal detected in discovery mode. Enabling form for more input...');
        enableForm();
        statusMessage.classList.add('hidden');
    }
    
    // Check for completion
    if (eventData.next_action === 'halt_no_hypothesis' || eventData.next_action === null) {
        console.log('✅ Workflow complete. Adding final event block...');
        setTimeout(() => {
            enableForm();
            statusMessage.classList.add('hidden');
            stopEventStream();
        }, 1000);
    }
    
    // Add event block to content
    console.log('📊 Adding event block to UI...');
    addEventBlock(eventData);
}

function addEventBlock(eventData) {
    // Remove ready message on first event
    const existingReady = contentArea.querySelector('.ready-message');
    if (existingReady) {
        existingReady.remove();
    }
    
    const block = document.createElement('div');
    block.className = 'event-block';
    
    // Header
    const header = document.createElement('div');
    header.className = 'event-header';
    
    const phaseInfo = document.createElement('div');
    const statusIndicator = eventData.status ? 
        `<span class="status-indicator ${eventData.status === 'needs_more_input' ? 'warning' : 'success'}"></span>` : 
        `<span class="status-indicator running"></span>`;
    
    phaseInfo.innerHTML = `
        ${statusIndicator}
        <span class="event-phase">${eventData.phase}</span>
    `;
    
    const jobIdSpan = document.createElement('span');
    jobIdSpan.className = 'event-job-id';
    jobIdSpan.textContent = `Job #${eventData.job_id}`;
    
    header.appendChild(phaseInfo);
    header.appendChild(jobIdSpan);
    block.appendChild(header);
    
    // Body
    const body = document.createElement('div');
    body.className = 'event-body';
    
    // Explanation text
    if (eventData.explanation) {
        const explanation = document.createElement('p');
        explanation.className = 'event-text';
        explanation.textContent = eventData.explanation;
        body.appendChild(explanation);
    }
    
    // Result summary
    if (eventData.result) {
        const resultDiv = document.createElement('div');
        resultDiv.className = 'event-result';
        resultDiv.textContent = JSON.stringify(eventData.result, null, 2);
        body.appendChild(resultDiv);
    }
    
    // Graph (if present in payload with nodes and edges)
    if (eventData.payload && eventData.payload.graph && 
        eventData.payload.graph.nodes && eventData.payload.graph.nodes.length > 0 &&
        eventData.payload.graph.edges && eventData.payload.graph.edges.length > 0) {
        const graphDiv = createGraphElement(eventData.payload.graph);
        body.appendChild(graphDiv);
    }
    
    // Hypotheses table (if present in payload)
    if (eventData.payload && eventData.payload.top_k_hypotheses && eventData.payload.top_k_hypotheses.length > 0) {
        const table = createHypothesesTable(eventData.payload.top_k_hypotheses);
        body.appendChild(table);
    }
    
    // Papers list (if present)
    if (eventData.payload && eventData.payload.papers && eventData.payload.papers.length > 0) {
        const papersList = createPapersList(eventData.payload.papers);
        body.appendChild(papersList);
    }
    
    block.appendChild(body);
    
    contentArea.appendChild(block);
    console.log('✅ Event block added to DOM');
    console.log('   Phase:', eventData.phase);
    console.log('   Job:', eventData.job_id);
    console.log('   Status:', eventData.status);
    console.log('   Hypotheses:', eventData.payload?.top_k_hypotheses?.length || 0);
    console.log('   Papers:', eventData.payload?.papers?.length || 0);
    
    // Auto-scroll to bottom
    contentArea.scrollTop = contentArea.scrollHeight;
}

// ============= USER INPUT BLOCK =============
function addUserBlock(content, files) {
    // Remove ready message on first input
    const existingReady = contentArea.querySelector('.ready-message');
    if (existingReady) {
        existingReady.remove();
    }
    
    const block = document.createElement('div');
    block.className = 'event-block user-block';
    
    // Header
    const header = document.createElement('div');
    header.className = 'event-header';
    
    const phaseInfo = document.createElement('div');
    phaseInfo.innerHTML = `<span class="event-phase">Input</span>`;
    
    const userLabel = document.createElement('span');
    userLabel.className = 'user-label';
    userLabel.textContent = 'user';
    
    header.appendChild(phaseInfo);
    header.appendChild(userLabel);
    block.appendChild(header);
    
    // Body
    const body = document.createElement('div');
    body.className = 'event-body';
    
    // Content text
    if (content) {
        const contentDiv = document.createElement('p');
        contentDiv.className = 'event-text';
        contentDiv.textContent = content;
        body.appendChild(contentDiv);
    }
    
    // Files list
    if (files.size > 0) {
        const filesDiv = document.createElement('div');
        filesDiv.className = 'user-files';
        
        const filesHeading = document.createElement('div');
        filesHeading.style.fontSize = '12px';
        filesHeading.style.fontWeight = '600';
        filesHeading.style.color = 'var(--text-secondary)';
        filesHeading.style.marginBottom = '8px';
        filesHeading.textContent = `📎 ${files.size} file(s) attached`;
        filesDiv.appendChild(filesHeading);
        
        const filesList = document.createElement('div');
        filesList.style.fontSize = '12px';
        filesList.style.color = 'var(--text-primary)';
        
        files.forEach((file) => {
            const fileItem = document.createElement('div');
            fileItem.style.padding = '4px 0';
            fileItem.textContent = `• ${file.name} (${(file.size / 1024).toFixed(1)} KB)`;
            filesList.appendChild(fileItem);
        });
        
        filesDiv.appendChild(filesList);
        body.appendChild(filesDiv);
    }
    
    block.appendChild(body);
    contentArea.appendChild(block);
    
    // Auto-scroll to bottom
    contentArea.scrollTop = contentArea.scrollHeight;
}

function addVerificationUserBlock(entity1, entity2) {
    // Remove ready message on first input
    const existingReady = contentArea.querySelector('.ready-message');
    if (existingReady) {
        existingReady.remove();
    }
    
    const block = document.createElement('div');
    block.className = 'event-block user-block';
    
    // Header
    const header = document.createElement('div');
    header.className = 'event-header';
    
    const phaseInfo = document.createElement('div');
    phaseInfo.innerHTML = `<span class="event-phase">Input</span>`;
    
    const userLabel = document.createElement('span');
    userLabel.className = 'user-label';
    userLabel.textContent = 'user';
    
    header.appendChild(phaseInfo);
    header.appendChild(userLabel);
    block.appendChild(header);
    
    // Body
    const body = document.createElement('div');
    body.className = 'event-body';
    
    // Entity pair text
    const entitiesDiv = document.createElement('p');
    entitiesDiv.className = 'event-text';
    entitiesDiv.innerHTML = `<strong>${entity1}</strong> ↔ <strong>${entity2}</strong>`;
    body.appendChild(entitiesDiv);
    
    block.appendChild(body);
    contentArea.appendChild(block);
    
    // Auto-scroll to bottom
    contentArea.scrollTop = contentArea.scrollHeight;
}

function addAnswerBlock(answer) {
    // Remove ready message if present
    const existingReady = contentArea.querySelector('.ready-message');
    if (existingReady) {
        existingReady.remove();
    }
    
    const block = document.createElement('div');
    block.className = 'event-block answer-block';
    
    // Header
    const header = document.createElement('div');
    header.className = 'event-header';
    
    const phaseInfo = document.createElement('div');
    phaseInfo.innerHTML = `<span class="event-phase">Answer</span>`;
    
    const aiLabel = document.createElement('span');
    aiLabel.className = 'ai-label';
    aiLabel.textContent = 'System';
    
    header.appendChild(phaseInfo);
    header.appendChild(aiLabel);
    block.appendChild(header);
    
    // Body
    const body = document.createElement('div');
    body.className = 'event-body';
    
    // Answer text
    const answerDiv = document.createElement('p');
    answerDiv.className = 'answer-text';
    answerDiv.textContent = answer;
    body.appendChild(answerDiv);
    
    block.appendChild(body);
    contentArea.appendChild(block);
    
    // Auto-scroll to bottom
    contentArea.scrollTop = contentArea.scrollHeight;
}

// ============= GRAPH RENDERING =============
function createGraphElement(graphData) {
    const container = document.createElement('div');
    container.className = 'graph-container';
    
    const networkDiv = document.createElement('div');
    networkDiv.id = 'graphNetwork';
    networkDiv.style.width = '100%';
    networkDiv.style.height = CONFIG.UI.graph.height + 'px';
    container.appendChild(networkDiv);
    
    // Add legend
    const legend = document.createElement('div');
    legend.className = 'graph-legend';
    legend.innerHTML = `
        <div><strong>Nodes:</strong> ${graphData.nodes.length} | <strong>Edges:</strong> ${graphData.edges.length}</div>
        <div style="font-size: 11px; margin-top: 4px;">💡 Hover over nodes and edges to see details</div>
    `;
    container.appendChild(legend);
    
    // Prepare data for Vis.js
    const nodes = new vis.DataSet(graphData.nodes.map(node => ({
        id: node.id,
        label: node.label,
        title: node.label, // Tooltip
        color: { background: '#10a37f', border: '#0a8a69' },
        shape: 'box',
        margin: { top: 8, right: 8, bottom: 8, left: 8 },
    })));
    
    const edges = new vis.DataSet(graphData.edges.map(edge => ({
        from: edge.source,
        to: edge.target,
        label: edge.predicate,
        title: `${edge.source} ${edge.predicate} ${edge.target}`,
        arrows: 'to',
        smooth: { type: 'curvedCW' },
        color: { color: '#d1d5db', highlight: '#10a37f' },
        font: { size: 11 },
    })));
    
    const options = {
        physics: {
            enabled: CONFIG.UI.graph.physics,
            stabilization: { iterations: CONFIG.UI.graph.stabilizationIterations },
            barnesHut: {
                gravitationalConstant: -26000,
                centralGravity: 0.3,
                springLength: 200,
                springConstant: 0.018,
            },
        },
        interaction: {
            navigationButtons: false,
            keyboard: true,
        },
        nodes: {
            font: { size: 12 },
        },
    };
    
    // Render graph
    setTimeout(() => {
        try {
            appState.graphInstance = new vis.Network(networkDiv, { nodes, edges }, options);
            
            // Create info panel for hover details
            const infoPanel = document.createElement('div');
            infoPanel.id = 'graphInfoPanel';
            infoPanel.style.cssText = `
                position: absolute;
                background: rgba(0, 0, 0, 0.9);
                color: white;
                padding: 12px;
                border-radius: 6px;
                font-size: 12px;
                pointer-events: none;
                display: none;
                z-index: 100;
                max-width: 250px;
                white-space: normal;
                line-height: 1.4;
                box-shadow: 0 2px 8px rgba(0,0,0,0.3);
            `;
            container.appendChild(infoPanel);
            
            // Handle node hover
            appState.graphInstance.on('hoverNode', (params) => {
                const nodeId = params.node;
                const node = graphData.nodes.find(n => n.id === nodeId);
                if (node) {
                    infoPanel.innerHTML = `<strong>Entity:</strong><br>${node.label}`;
                    infoPanel.style.display = 'block';
                    infoPanel.style.left = params.event.pageX + 10 + 'px';
                    infoPanel.style.top = params.event.pageY + 10 + 'px';
                }
            });
            
            // Handle edge hover
            appState.graphInstance.on('hoverEdge', (params) => {
                const edgeIndex = params.edge;
                const edge = graphData.edges[edgeIndex];
                if (edge) {
                    const details = `
<strong>${edge.source}</strong><br>
<em style="color: #10a37f;">${edge.predicate}</em><br>
<strong>${edge.target}</strong><br><br>
<small>Hypotheses: ${edge.used_in_hypotheses?.length || 0}<br>
Triples: ${edge.triple_ids?.join(', ') || 'N/A'}</small>
                    `;
                    infoPanel.innerHTML = details;
                    infoPanel.style.display = 'block';
                    infoPanel.style.left = params.event.pageX + 10 + 'px';
                    infoPanel.style.top = params.event.pageY + 10 + 'px';
                }
            });
            
            // Hide info panel when not hovering
            appState.graphInstance.on('blurNode', () => {
                infoPanel.style.display = 'none';
            });
            
            appState.graphInstance.on('blurEdge', () => {
                infoPanel.style.display = 'none';
            });
            
        } catch (error) {
            console.error('Error rendering graph:', error);
        }
    }, 100);
    
    return container;
}


// ============= HYPOTHESES TABLE =============
function createHypothesesTable(hypotheses) {
    const container = document.createElement('div');
    
    const heading = document.createElement('h3');
    heading.style.fontSize = '14px';
    heading.style.marginBottom = '8px';
    heading.textContent = 'Top Hypotheses';
    container.appendChild(heading);
    
    const table = document.createElement('table');
    table.className = 'hypothesis-table';
    
    // Headers
    const thead = document.createElement('thead');
    thead.innerHTML = `
        <tr>
            <th>Source</th>
            <th>Target</th>
            <th>Intermediates</th>
        </tr>
    `;
    table.appendChild(thead);
    
    // Rows
    const tbody = document.createElement('tbody');
    hypotheses.slice(0, 10).forEach((hyp, index) => {
        const intermediates = hyp.intermediates && hyp.intermediates.length > 0 
            ? hyp.intermediates.join(' → ')
            : '—';
        
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${hyp.source || 'N/A'}</td>
            <td>${hyp.target || 'N/A'}</td>
            <td>${intermediates}</td>
        `;
        tbody.appendChild(row);
    });
    table.appendChild(tbody);
    
    container.appendChild(table);
    return container;
}

// ============= PAPERS LIST =============
function createPapersList(papers) {
    const container = document.createElement('div');
    
    const heading = document.createElement('h3');
    heading.style.fontSize = '14px';
    heading.style.marginBottom = '8px';
    heading.textContent = `Papers Retrieved (${papers.length})`;
    container.appendChild(heading);
    
    const list = document.createElement('div');
    list.style.fontSize = '13px';
    list.style.lineHeight = '1.6';
    
    papers.forEach((paper, index) => {
        const item = document.createElement('div');
        item.style.paddingBottom = '12px';
        item.style.marginBottom = '12px';
        item.style.borderBottom = '1px solid var(--border-color)';
        item.style.position = 'relative';
        item.style.cursor = 'pointer';
        
        const titleText = paper.title || 'Untitled';
        const impactScore = paper.impact_score || 0;
        const status = paper.abstract_only ? 'Abstract Only' : 'Full Text';
        const abstractSnippet = paper.abstract_snippet || 'No abstract available';
        
        item.innerHTML = `
            <div style="display: flex; justify-content: space-between; align-items: flex-start; gap: 10px;">
                <div style="flex: 1;">
                    <strong style="color: var(--text-primary); display: block; margin-bottom: 4px;">${index + 1}. ${titleText}</strong>
                    <small style="color: var(--text-secondary); display: block;">
                        Impact Score: ${impactScore} | Status: ${status}
                    </small>
                </div>
                ${paper.url ? `
                    <a href="${paper.url}" target="_blank" rel="noopener noreferrer" 
                       style="flex-shrink: 0; padding: 4px 8px; background: var(--primary-color); color: white; 
                       border-radius: 4px; text-decoration: none; font-size: 11px; font-weight: 600; 
                       white-space: nowrap; transition: background 0.2s;"
                       onmouseover="this.style.background='var(--primary-hover)'"
                       onmouseout="this.style.background='var(--primary-color)'">
                        Download
                    </a>
                ` : ''}
            </div>
        `;
        
        // Add tooltip for abstract on hover
        item.title = abstractSnippet;
        item.style.borderRadius = '4px';
        item.style.padding = '8px';
        item.style.transition = 'background 0.2s ease';
        
        item.addEventListener('mouseenter', () => {
            item.style.background = 'var(--bg-secondary)';
        });
        
        item.addEventListener('mouseleave', () => {
            item.style.background = 'transparent';
        });
        
        list.appendChild(item);
    });
    
    container.appendChild(list);
    return container;
}

// ============= INITIALIZATION =============
document.addEventListener('DOMContentLoaded', () => {
    // Set initial mode UI
    discoveryForm.classList.remove('hidden');
    verificationForm.classList.add('hidden');
    
    // Expose debug function in console
    window.debugFiles = () => {
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log('📁 FILES IN MEMORY:');
        console.log(`Total: ${appState.files.size} file(s)`);
        appState.files.forEach((file, name) => {
            console.log(`  • ${name} (${(file.size / 1024).toFixed(2)} KB)`);
        });
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    };
    
    console.log('✅ App loaded. Type debugFiles() in console to check files.');
});
