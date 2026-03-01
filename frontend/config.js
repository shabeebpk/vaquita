/**
 * Configuration for the Literature Review Frontend
 * Customize settings here instead of modifying app.js
 */

const CONFIG = {
    // Backend API Configuration
    API: {
        // Main API base URL
        baseUrl: 'http://localhost:8000',
        
        // Endpoints (relative to baseUrl)
        endpoints: {
            discovery: '/',           // POST - Discovery upload
            verification: '/verify/', // POST - Verification
            events: '/user/{userId}/events/' // GET - SSE event stream
        },
        
        // User ID for event streaming
        userId: 1,
        
        // Request timeout (ms)
        timeout: 30000,
    },
    
    // UI Configuration
    UI: {
        // Auto-scroll to new events
        autoScroll: true,
        
        // Show detailed metrics in event blocks
        showMetrics: true,
        
        // Graph visualization settings
        graph: {
            // Physics simulation enabled
            physics: true,
            
            // Physics stabilization iterations
            stabilizationIterations: 200,
            
            // Graph height in pixels
            height: 400,
            
            // Auto-fit graph to viewport
            autoFit: true,
            
            // Show interaction hints
            showHints: true,
        },
        
        // Maximum events to display (0 = unlimited)
        maxEventsDisplay: 0,
    },
    
    // File Upload Configuration
    FILES: {
        // Allowed file extensions
        allowedExtensions: ['.pdf', '.txt', '.docx'],
        
        // Maximum file size in MB
        maxFileSize: 100,
        
        // Allow multiple files
        multipleFiles: true,
    },
    
    // Display Configuration
    DISPLAY: {
        // Theme: 'auto', 'light', 'dark'
        theme: 'auto',
        
        // Show phase icons
        showPhaseIcons: true,
        
        // Separator line between events
        showDividers: true,
        
        // Show job ID in blocks
        showJobId: true,
    },
    
    // SSE Configuration
    SSE: {
        // Reconnect on disconnect (ms)
        reconnectDelay: 3000,
        
        // Maximum reconnect attempts (0 = infinite)
        maxReconnectAttempts: 5,
        
        // Heartbeat check interval (ms)
        heartbeatInterval: 30000,
    },
    
    // Development/Debug
    DEBUG: {
        // Log SSE events to console
        logSSEEvents: false,
        
        // Log form submissions
        logSubmissions: false,
        
        // Show API responses
        logAPIResponses: false,
    },
};

// Helper function to get API endpoint URL
function getApiUrl(endpoint, params = {}) {
    let url = CONFIG.API.baseUrl + CONFIG.API.endpoints[endpoint];
    
    // Replace {userId} placeholder
    if (params.userId) {
        url = url.replace('{userId}', params.userId);
    }
    
    return url;
}

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { CONFIG, getApiUrl };
}
