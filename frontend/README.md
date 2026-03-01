# Literature Review System - Frontend

A modern, interactive web-based frontend for the Literature Review System featuring dual-mode operation (Discovery & Verification), real-time event streaming, and interactive knowledge graph visualization.

## Features

### 🎯 Dual Mode Operation
- **Discovery Mode**: Upload documents, analyze relationships, explore hypotheses
- **Verification Mode**: Verify connections between specific entities

### 📊 Real-time Updates
- Server-Sent Events (SSE) streaming for live pipeline updates
- Automatic content streaming and scrolling
- Phase-by-phase visualization of the analysis process

### 🔗 Interactive Knowledge Graphs
- Powered by Vis.js library
- Click nodes and edges to view details
- Physics-based layout for optimal visualization
- Automatic stabilization for better readability

### 📈 Rich Data Visualization
- Event blocks with phase information and status indicators
- Results and metrics display
- Hypothesis tables with ranking
- Papers list with impact scores

### 💾 Smart Form Management
- File upload support (PDF, TXT, DOCX)
- Automatic job continuation with job ID tracking
- Form state management (enable/disable based on system status)
- Clear button for verification mode reset

## Project Structure

```
frontend/
├── index.html          # Main HTML structure
├── styles.css          # Complete styling with responsive design
├── app.js             # Core application logic
└── README.md          # This file
```

## Setup & Installation

### Prerequisites
- Modern web browser (Chrome, Firefox, Safari, Edge)
- Backend API running at `http://localhost:8000`
- Node.js (optional, for local server)

### Quick Start

1. **Using Python's built-in server** (for testing):
```bash
cd frontend
python -m http.server 8080
```
Visit: `http://localhost:8080`

2. **Using Node.js http-server**:
```bash
npm install -g http-server
http-server frontend
```

3. **Direct**: Simply open `index.html` in your browser (if backend is accessible)

## API Configuration

The frontend connects to your backend API. Configure the API base URL in `app.js`:

```javascript
apiBaseUrl: 'http://localhost:8000'
```

## Usage Guide

### Discovery Mode
1. **Enter Research Topic**: Type your question or research focus in the text area
2. **Upload Files** (Optional): Click the attachment icon to select documents (PDF, TXT, DOCX)
3. **Submit**: Click the send button
4. **Monitor Progress**: Watch real-time events stream in the content area
5. **Respond to Prompts**: When system needs more input, the form will automatically enable

### Verification Mode
1. **Toggle Mode**: Click the mode switch in the top-right corner
2. **Enter Entity 1**: Source entity (e.g., "Kiwi fruit")
3. **Enter Entity 2**: Target entity (e.g., "Cancer")
4. **Verify**: Click the check button
5. **Review Results**: View the connection analysis in the content area
6. **Clear**: Click the clear button to reset and start over

## UI Components

### Header
- Application title
- Fixed at top for easy access

### Mode Switch
- Toggle button in top-right corner
- Shows current mode (Discovery | Verification)
- Automatically clears content when switching

### Content Area
- Scrollable main section showing all events
- Event blocks with:
  - Phase information
  - Status indicators
  - Job ID reference
  - Detailed explanation from LLM
  - Result metrics
  - Interactive graphs (when applicable)
  - Hypothesis tables (when applicable)
  - Papers list (when applicable)

### Footer Form
- Dynamic form that changes based on mode
- Real-time file list management
- Form disables during processing with status message
- Auto-enables when system requires input

## Event Block Contents

Each SSE event creates a block containing:

1. **Header Section**
   - Status indicator (running/success/warning)
   - Phase name (CREATION, INGESTION, TRIPLES, GRAPH, etc.)
   - Job ID reference

2. **Body Section**
   - LLM-generated explanation
   - Result metrics and data
   - Graph visualization (if graph data exists)
   - Hypothesis table (if hypotheses exist)
   - Papers list (if papers retrieved)

3. **Divider**
   - Horizontal line separating events

## Interactive Graph Features

### Graph Visualization
- **Nodes**: Entities from the knowledge base (colored green)
- **Edges**: Relationships between entities (labeled with predicate type)
- **Layout**: Physics-based algorithm for optimal positioning
- **Navigation**: Zoom, pan, and interact with mouse/keyboard

### Interaction
- **Click Nodes**: View node details
- **Click Edges**: View relationship details with supporting evidence
- **Hover**: Tooltips appear with entity/relationship information
- **Physics**: Real-time visualization stabilization

## Status Indicators

- 🟢 **Running**: Phase currently in progress (yellow pulse)
- ✓ **Complete**: Phase completed successfully (green dot)
- ⚠️ **Needs Input**: System waiting for user response (orange)
- 🛑 **Halted**: Process completed or stopped (red)

## File Upload Support

- **Formats**: PDF, TXT, DOCX
- **Multiple Files**: Select and upload several documents at once
- **Management**: Remove files before submission
- **Progress**: File list shows selected files
- **Auto-clear**: List clears after successful submission

## Keyboard Shortcuts

- **Tab**: Navigate between form elements
- **Enter**: Submit form (when allowed)
- **Escape**: Clear mode switch panel (on some browsers)

## API Endpoints Used

### 1. Discovery Upload
```
POST /
Content-Type: multipart/form-data

Parameters:
- job_id (optional): Continue existing job
- content (optional): Text input
- files (optional): File upload(s)

Response:
{
  "job_id": 123,
  "status": "queued|conversational",
  "details": [...],
  "answer": "..."
}
```

### 2. Verification
```
POST /verify/
Content-Type: application/x-www-form-urlencoded

Parameters:
- entity1: First entity
- entity2: Second entity

Response:
{
  "job_id": 789,
  "status": "FETCH_QUEUED",
  "verification_id": 101,
  "message": "..."
}
```

### 3. Event Stream
```
GET /user/{user_id}/events/
Response-Type: text/event-stream

Returns: SSE events with job pipeline updates
```

## Styling & Customization

### Color Scheme
- Primary Color: `#10a37f` (Green)
- Background: `#ffffff` (White)
- Text: `#0d0d0d` (Dark)
- Border: `#d1d5db` (Gray)

### Dark Mode
Automatically enabled based on system preferences (supports `prefers-color-scheme`)

### Responsive Design
- Mobile-optimized layout
- Touch-friendly interface
- Responsive form and graph sizing
- Tablet and desktop support

## Troubleshooting

### Events not appearing
- Check backend URL in `app.js`
- Verify backend is running at `http://localhost:8000`
- Check browser console for errors (F12)
- Ensure SSE endpoint is accessible

### Graph not rendering
- Wait for physics stabilization (may take a few seconds)
- Check browser console for Vis.js errors
- Verify graph data is present in event payload
- Try zooming/panning to trigger redraw

### Form not responding
- Check if system status message indicates ongoing processing
- Ensure file sizes are reasonable (< 100MB recommended)
- Verify API endpoint accessibility
- Clear browser cache if issues persist

### CORS Errors
If you see CORS errors, your backend needs to enable CORS:
```python
# Backend should include:
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type
```

## Performance Notes

- Event streaming automatically scrolls to latest content
- Graph rendering uses Vis.js physics engine (computationally intensive)
- Large graphs (100+ nodes) may take time to stabilize
- Browser memory usage increases with many events (consider clearing for long sessions)

## Browser Support

- ✅ Chrome/Chromium 90+
- ✅ Firefox 88+
- ✅ Safari 14+
- ✅ Edge 90+

## Future Enhancements

- [ ] Export event history to JSON/CSV
- [ ] Search and filter events
- [ ] Graph analysis tools (centrality, clustering)
- [ ] Hypothesis export to PDF
- [ ] Collaborative features
- [ ] Theme customization UI

## License

TBD

## Support

For issues, questions, or feature requests, please refer to the main project documentation.
