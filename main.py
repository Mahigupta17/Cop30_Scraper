from flask import Flask, render_template_string, request, jsonify
import subprocess
import os
import threading
import time
from datetime import datetime
import csv
import io
import pandas as pd  # For Excel support

app = Flask(__name__)

# Track scraper status globally
scraper_status = {
    "running": False,
    "last_run": None,
    "message": "Ready",
    "events_found": 0
}

# Store configuration
scraper_config = {
    "urls": [],
    "format_columns": [],
    "sheet_url": "https://docs.google.com/spreadsheets/d/1tDFA7DIRm0b-9mbZby2lNyfgYJtGXSjOwl5ezN3CeME/edit"
}

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MVP Testing Scraper</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
        }
        .container { 
            background: white;
            padding: 40px;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            max-width: 700px;
            width: 100%;
        }
        h1 { 
            font-size: 32px;
            color: #333;
            margin-bottom: 10px;
            font-weight: 700;
            text-align: center;
        }
        .subtitle {
            color: #666;
            margin-bottom: 30px;
            font-size: 16px;
            text-align: center;
        }
        
        .section {
            background: #f8f9fa;
            border-radius: 12px;
            padding: 20px;
            margin: 20px 0;
            border-left: 4px solid #667eea;
        }
        
        .section-title {
            font-size: 18px;
            font-weight: 600;
            color: #333;
            margin-bottom: 12px;
            display: flex;
            align-items: center;
        }
        
        .section-title .emoji {
            font-size: 24px;
            margin-right: 10px;
        }
        
        label {
            display: block;
            margin-bottom: 8px;
            color: #555;
            font-size: 14px;
            font-weight: 500;
        }
        
        input[type="text"], input[type="url"], textarea, input[type="file"] {
            width: 100%;
            padding: 12px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 14px;
            transition: border-color 0.3s;
            font-family: inherit;
        }
        
        input[type="text"]:focus, input[type="url"]:focus, textarea:focus {
            outline: none;
            border-color: #667eea;
        }
        
        textarea {
            min-height: 100px;
            resize: vertical;
        }
        
        .help-text {
            font-size: 12px;
            color: #888;
            margin-top: 5px;
        }
        
        .btn {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 16px 40px;
            border-radius: 50px;
            font-size: 18px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 8px 20px rgba(102, 126, 234, 0.4);
            margin-top: 20px;
            width: 100%;
        }
        .btn:hover:not(:disabled) {
            transform: translateY(-3px);
            box-shadow: 0 12px 30px rgba(102, 126, 234, 0.6);
        }
        .btn:disabled {
            background: #ccc;
            cursor: not-allowed;
            transform: none;
            box-shadow: none;
        }
        
        .status-box {
            margin-top: 30px;
            padding: 20px;
            border-radius: 12px;
            font-size: 15px;
            display: none;
            animation: fadeIn 0.5s;
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(-10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .status-box.show { display: block; }
        .status-box.loading {
            background: #fff3cd;
            border: 2px solid #ffc107;
            color: #856404;
        }
        .status-box.success {
            background: #d4edda;
            border: 2px solid #28a745;
            color: #155724;
        }
        .status-box.error {
            background: #f8d7da;
            border: 2px solid #dc3545;
            color: #721c24;
        }
        
        .spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #667eea;
            border-radius: 50%;
            width: 50px;
            height: 50px;
            animation: spin 1s linear infinite;
            margin: 20px auto;
            display: none;
        }
        .spinner.show { display: block; }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .sheet-link {
            display: inline-block;
            margin-top: 20px;
            color: #667eea;
            text-decoration: none;
            font-size: 14px;
            transition: color 0.3s;
            text-align: center;
            width: 100%;
        }
        .sheet-link:hover { color: #764ba2; text-decoration: underline; }
        
        .upload-area {
            border: 2px dashed #667eea;
            border-radius: 8px;
            padding: 20px;
            text-align: center;
            background: #f8f9ff;
            cursor: pointer;
            transition: all 0.3s;
        }
        .upload-area:hover {
            background: #f0f2ff;
            border-color: #764ba2;
        }
        .upload-area.dragover {
            background: #e8ebff;
            border-color: #667eea;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🧪 MVP Testing Scraper</h1>
        <p class="subtitle">Automated Data Extraction from Testing Tools</p>
        
        <!-- Section 1: Name of List -->
        <div class="section">
            <div class="section-title">
                <span class="emoji">📝</span>
                Name of List
            </div>
            <input type="text" id="listName" placeholder="e.g., MVP Testing Tools Comparison" value="MVP Testing Tools">
            <p class="help-text">Give your scraping project a name</p>
        </div>
        
        <!-- Section 2: Links of List -->
        <div class="section">
            <div class="section-title">
                <span class="emoji">🔗</span>
                Links of List
            </div>
            <textarea id="urlList" placeholder="Enter URLs (one per line):
https://www.hotjar.com
https://www.lookback.com
https://www.usertesting.com"></textarea>
            <p class="help-text">Enter website URLs to scrape, one per line. You can add up to 83 URLs from your document.</p>
        </div>
        
        <!-- Section 3: Format of List -->
        <div class="section">
            <div class="section-title">
                <span class="emoji">📋</span>
                Format of List
            </div>
            <label for="formatFile">Upload CSV/Excel template showing desired output format:</label>
            <div class="upload-area" onclick="document.getElementById('formatFile').click()">
                <p>📄 Click or drag to upload CSV/Excel file</p>
                <p class="help-text">Example columns: Tool Name, Description, Pricing, Features, etc.</p>
            </div>
            <input type="file" id="formatFile" accept=".csv,.xlsx,.xls" style="display: none;" onchange="handleFileUpload(this)">
            <p id="fileName" style="margin-top: 10px; color: #667eea; font-weight: 600;"></p>
        </div>
        
        <!-- Section 4: Destination -->
        <div class="section">
            <div class="section-title">
                <span class="emoji">📊</span>
                Destination of List
            </div>
            <input type="url" id="sheetUrl" value="https://docs.google.com/spreadsheets/d/1tDFA7DIRm0b-9mbZby2lNyfgYJtGXSjOwl5ezN3CeME/edit" readonly>
            <p class="help-text">Data will be exported to this Google Sheet</p>
        </div>
        
        <button class="btn" id="scrapeBtn" onclick="startScraper()">
            <span id="btnText">🚀 Start Scraping</span>
        </button>
        
        <div class="spinner" id="spinner"></div>
        
        <div class="status-box" id="statusBox">
            <div id="statusText"></div>
        </div>
        
        <a href="https://docs.google.com/spreadsheets/d/1tDFA7DIRm0b-9mbZby2lNyfgYJtGXSjOwl5ezN3CeME/edit" target="_blank" class="sheet-link">
            📊 View Results in Google Sheet
        </a>
    </div>
    
    <script>
        let uploadedFormat = null;
        
        function handleFileUpload(input) {
            const file = input.files[0];
            if (file) {
                document.getElementById('fileName').textContent = `✅ Uploaded: ${file.name}`;
                uploadedFormat = file;
            }
        }
        
        function startScraper() {
            const listName = document.getElementById('listName').value.trim();
            const urlList = document.getElementById('urlList').value.trim();
            const btn = document.getElementById('scrapeBtn');
            const btnText = document.getElementById('btnText');
            const spinner = document.getElementById('spinner');
            const statusBox = document.getElementById('statusBox');
            const statusText = document.getElementById('statusText');
            
            // Validation
            if (!listName) {
                alert('Please enter a name for your list');
                return;
            }
            
            if (!urlList) {
                alert('Please enter at least one URL to scrape');
                return;
            }
            
            if (!uploadedFormat) {
                alert('Please upload a CSV/Excel template showing the desired format');
                return;
            }
            
            // Parse URLs
            const urls = urlList.split('\\n').filter(url => url.trim()).map(url => url.trim());
            
            if (urls.length === 0) {
                alert('No valid URLs found');
                return;
            }
            
            btn.disabled = true;
            btnText.textContent = '⏳ Starting...';
            spinner.classList.add('show');
            statusBox.className = 'status-box loading show';
            statusText.innerHTML = '<strong>Initializing scraper...</strong><br>Analyzing format template...';
            
            // Prepare form data
            const formData = new FormData();
            formData.append('list_name', listName);
            formData.append('urls', JSON.stringify(urls));
            formData.append('format_file', uploadedFormat);
            
            fetch('/trigger', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                if (data.status === 'started') {
                    statusText.innerHTML = `<strong>✅ Scraper Started!</strong><br>Processing ${urls.length} website(s). This will take 5-15 minutes.`;
                    btnText.textContent = '⏳ Scraping...';
                    startPolling();
                } else {
                    throw new Error(data.message || 'Failed to start scraper');
                }
            })
            .catch(error => {
                statusBox.className = 'status-box error show';
                statusText.innerHTML = '<strong>❌ Error</strong><br>' + error.message;
                btn.disabled = false;
                btnText.textContent = '🚀 Start Scraping';
                spinner.classList.remove('show');
            });
        }
        
        function startPolling() {
            setTimeout(pollStatus, 5000);
        }
        
        function pollStatus() {
            fetch('/status')
            .then(response => response.json())
            .then(data => {
                const btn = document.getElementById('scrapeBtn');
                const btnText = document.getElementById('btnText');
                const spinner = document.getElementById('spinner');
                const statusBox = document.getElementById('statusBox');
                const statusText = document.getElementById('statusText');
                
                if (data.running) {
                    statusBox.className = 'status-box loading show';
                    statusText.innerHTML = `
                        <strong>🔄 Scraping in Progress...</strong><br>
                        ${data.message}<br>
                        <small>Started: ${data.last_run || 'Just now'}</small>
                    `;
                    btn.disabled = true;
                    btnText.textContent = '⏳ Scraping...';
                    spinner.classList.add('show');
                    setTimeout(pollStatus, 5000);
                } else {
                    statusBox.className = 'status-box success show';
                    statusText.innerHTML = `
                        <strong>✅ Scraping Complete!</strong><br>
                        ${data.message}<br>
                        ${data.events_found > 0 ? `Found ${data.events_found} items. ` : ''}
                        Check your Google Sheet for results.
                    `;
                    btn.disabled = false;
                    btnText.textContent = '🚀 Start Scraping';
                    spinner.classList.remove('show');
                }
            })
            .catch(error => {
                console.error('Polling error:', error);
                setTimeout(pollStatus, 5000);
            });
        }
        
        // Drag and drop
        const uploadArea = document.querySelector('.upload-area');
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            uploadArea.addEventListener(eventName, preventDefaults, false);
        });
        
        function preventDefaults(e) {
            e.preventDefault();
            e.stopPropagation();
        }
        
        ['dragenter', 'dragover'].forEach(eventName => {
            uploadArea.addEventListener(eventName, () => uploadArea.classList.add('dragover'), false);
        });
        
        ['dragleave', 'drop'].forEach(eventName => {
            uploadArea.addEventListener(eventName, () => uploadArea.classList.remove('dragover'), false);
        });
        
        uploadArea.addEventListener('drop', function(e) {
            const dt = e.dataTransfer;
            const files = dt.files;
            document.getElementById('formatFile').files = files;
            handleFileUpload(document.getElementById('formatFile'));
        }, false);
    </script>
</body>
</html>
"""

def run_scraper_background():
    """Run scraper in background with configuration"""
    global scraper_status
    
    print("\n" + "="*80)
    print("🚀 STARTING MVP TESTING SCRAPER")
    print("="*80)
    
    scraper_status["running"] = True
    scraper_status["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    scraper_status["message"] = "Scraper initializing..."
    scraper_status["events_found"] = 0
    
    try:
        env = os.environ.copy()
        creds_path = "/etc/secrets/credentials.json"
        
        if not os.path.exists(creds_path):
            alt_paths = ["credentials.json", "../credentials.json"]
            for path in alt_paths:
                if os.path.exists(path):
                    creds_path = path
                    break
        
        env["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        
        # Pass configuration to scraper
        env["SCRAPER_URLS"] = ",".join(scraper_config["urls"])
        env["SCRAPER_COLUMNS"] = ",".join(scraper_config["format_columns"])
        
        print(f"🔑 Credentials: {creds_path}")
        print(f"🔗 URLs to scrape: {len(scraper_config['urls'])}")
        print(f"📋 Format columns: {scraper_config['format_columns']}")
        print("-"*80)
        
        scraper_status["message"] = f"Scraping {len(scraper_config['urls'])} websites..."
        
        result = subprocess.run(
            ["scrapy", "crawl", "mvp_tools"],
            capture_output=True,
            text=True,
            env=env,
            timeout=900,
            check=True
        )
        
        if "items scraped:" in result.stdout.lower():
            import re
            match = re.search(r'(\d+)\s+items?\s+scraped', result.stdout, re.IGNORECASE)
            if match:
                scraper_status["events_found"] = int(match.group(1))
        
        print("="*80)
        print("✅ SCRAPER COMPLETED SUCCESSFULLY")
        print("="*80)
        
        scraper_status["message"] = f"Completed successfully!"
        
    except subprocess.TimeoutExpired:
        print("⏱️ TIMEOUT: Scraper exceeded 15 minutes")
        scraper_status["message"] = "Timed out after 15 minutes"
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        scraper_status["message"] = f"Error: {str(e)[:200]}"
        
    finally:
        scraper_status["running"] = False
        print("="*80 + "\n")

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route("/trigger", methods=["POST"])
def trigger():
    global scraper_status, scraper_config
    
    if scraper_status["running"]:
        return jsonify({
            "status": "already_running",
            "message": "Scraper is already running"
        }), 400
    
    try:
        # Get configuration
        list_name = request.form.get('list_name')
        urls_json = request.form.get('urls')
        format_file = request.files.get('format_file')
        
        # Parse URLs
        import json
        urls = json.loads(urls_json)
        scraper_config["urls"] = urls
        
        # Parse format file to get column names
        if format_file:
            content = format_file.read().decode('utf-8')
            csv_reader = csv.reader(io.StringIO(content))
            headers = next(csv_reader)
            scraper_config["format_columns"] = headers
        
        # Start scraper
        thread = threading.Thread(target=run_scraper_background, daemon=True)
        thread.start()
        
        return jsonify({
            "status": "started",
            "message": f"Scraper started for {len(urls)} URLs"
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/status")
def status():
    return jsonify(scraper_status)

@app.route("/health")
def health():
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"🌐 MVP Testing Scraper starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)





# from flask import Flask, render_template_string, request, jsonify
# import subprocess
# import os
# import threading
# import time
# from datetime import datetime

# app = Flask(__name__)

# # Track scraper status globally
# scraper_status = {
#     "running": False,
#     "last_run": None,
#     "message": "Ready",
#     "events_found": 0
# }

# HTML_TEMPLATE = """
# <!DOCTYPE html>
# <html lang="en">
# <head>
#     <meta charset="UTF-8">
#     <meta name="viewport" content="width=device-width, initial-scale=1.0">
#     <title>COP30 Event Scraper</title>
#     <style>
#         * { margin: 0; padding: 0; box-sizing: border-box; }
#         body { 
#             font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
#             background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
#             min-height: 100vh;
#             display: flex;
#             align-items: center;
#             justify-content: center;
#             padding: 20px;
#         }
#         .container { 
#             background: white;
#             padding: 50px 40px;
#             border-radius: 20px;
#             box-shadow: 0 20px 60px rgba(0,0,0,0.3);
#             max-width: 600px;
#             width: 100%;
#             text-align: center;
#         }
#         h1 { 
#             font-size: 32px;
#             color: #333;
#             margin-bottom: 10px;
#             font-weight: 700;
#         }
#         .subtitle {
#             color: #666;
#             margin-bottom: 30px;
#             font-size: 16px;
#         }
#         .info-card {
#             background: linear-gradient(135deg, #667eea15 0%, #764ba215 100%);
#             border-radius: 12px;
#             padding: 20px;
#             margin: 25px 0;
#             text-align: left;
#         }
#         .info-row {
#             display: flex;
#             align-items: center;
#             margin: 12px 0;
#             font-size: 15px;
#             color: #444;
#         }
#         .info-row strong {
#             margin-left: 10px;
#             color: #667eea;
#         }
#         .btn {
#             background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
#             color: white;
#             border: none;
#             padding: 16px 40px;
#             border-radius: 50px;
#             font-size: 18px;
#             font-weight: 600;
#             cursor: pointer;
#             transition: all 0.3s ease;
#             box-shadow: 0 8px 20px rgba(102, 126, 234, 0.4);
#             margin-top: 10px;
#         }
#         .btn:hover:not(:disabled) {
#             transform: translateY(-3px);
#             box-shadow: 0 12px 30px rgba(102, 126, 234, 0.6);
#         }
#         .btn:disabled {
#             background: #ccc;
#             cursor: not-allowed;
#             transform: none;
#             box-shadow: none;
#         }
#         .status-box {
#             margin-top: 30px;
#             padding: 20px;
#             border-radius: 12px;
#             font-size: 15px;
#             display: none;
#             animation: fadeIn 0.5s;
#         }
#         @keyframes fadeIn {
#             from { opacity: 0; transform: translateY(-10px); }
#             to { opacity: 1; transform: translateY(0); }
#         }
#         .status-box.show { display: block; }
#         .status-box.loading {
#             background: #fff3cd;
#             border: 2px solid #ffc107;
#             color: #856404;
#         }
#         .status-box.success {
#             background: #d4edda;
#             border: 2px solid #28a745;
#             color: #155724;
#         }
#         .status-box.error {
#             background: #f8d7da;
#             border: 2px solid #dc3545;
#             color: #721c24;
#         }
#         .spinner {
#             border: 4px solid #f3f3f3;
#             border-top: 4px solid #667eea;
#             border-radius: 50%;
#             width: 50px;
#             height: 50px;
#             animation: spin 1s linear infinite;
#             margin: 20px auto;
#             display: none;
#         }
#         .spinner.show { display: block; }
#         @keyframes spin {
#             0% { transform: rotate(0deg); }
#             100% { transform: rotate(360deg); }
#         }
#         .logs-link {
#             display: inline-block;
#             margin-top: 20px;
#             color: #667eea;
#             text-decoration: none;
#             font-size: 14px;
#             transition: color 0.3s;
#         }
#         .logs-link:hover { color: #764ba2; text-decoration: underline; }
#         .emoji { font-size: 24px; margin-right: 8px; }
#     </style>
# </head>
# <body>
#     <div class="container">
#         <h1><span class="emoji">🌍</span>COP30 Event Scraper</h1>
#         <p class="subtitle">Automated Climate Events Data Collection</p>
        
#         <div class="info-card">
#             <div class="info-row">
#                 <span class="emoji">📅</span>
#                 <strong>October 1 - November 10, 2025</strong>
#             </div>
#             <div class="info-row">
#                 <span class="emoji">🌐</span>
#                 <strong>UNFCCC Official Calendar</strong>
#             </div>
#             <div class="info-row">
#                 <span class="emoji">📊</span>
#                 <strong>Exports to Google Sheets</strong>
#             </div>
#         </div>
        
#         <button class="btn" id="scrapeBtn" onclick="startScraper()">
#             <span id="btnText">🚀 Start Scraping</span>
#         </button>
        
#         <div class="spinner" id="spinner"></div>
        
#         <div class="status-box" id="statusBox">
#             <div id="statusText"></div>
#         </div>
        
#         <a href="https://docs.google.com/spreadsheets/d/1tDFA7DIRm0b-9mbZby2lNyfgYJtGXSjOwl5ezN3CeME/edit?gid=932436651#gid=932436651" target="_blank" class="logs-link">
#             📊 View Results in Google Sheet
#         </a>
#     </div>
    
#     <script>
#         let polling = false;
        
#         // Check status on page load
#         window.onload = function() {
#             checkStatus();
#         };
        
#         function startScraper() {
#             const btn = document.getElementById('scrapeBtn');
#             const btnText = document.getElementById('btnText');
#             const spinner = document.getElementById('spinner');
#             const statusBox = document.getElementById('statusBox');
#             const statusText = document.getElementById('statusText');
            
#             btn.disabled = true;
#             btnText.textContent = '⏳ Starting...';
#             spinner.classList.add('show');
#             statusBox.className = 'status-box loading show';
#             statusText.innerHTML = '<strong>Initializing scraper...</strong><br>This may take a moment.';
            
#             fetch('/trigger', {
#                 method: 'POST',
#                 headers: { 'Content-Type': 'application/json' }
#             })
#             .then(response => response.json())
#             .then(data => {
#                 if (data.status === 'started') {
#                     statusText.innerHTML = '<strong>✅ Scraper Started!</strong><br>Processing events in background. This will take 2-5 minutes.';
#                     btnText.textContent = '⏳ Scraping...';
#                     startPolling();
#                 } else if (data.status === 'already_running') {
#                     statusText.innerHTML = '<strong>⚠️ Already Running</strong><br>A scraping job is already in progress.';
#                     startPolling();
#                 } else {
#                     throw new Error(data.message || 'Unknown error');
#                 }
#             })
#             .catch(error => {
#                 statusBox.className = 'status-box error show';
#                 statusText.innerHTML = '<strong>❌ Error</strong><br>' + error.message;
#                 btn.disabled = false;
#                 btnText.textContent = '🚀 Start Scraping';
#                 spinner.classList.remove('show');
#             });
#         }
        
#         function startPolling() {
#             if (!polling) {
#                 polling = true;
#                 pollStatus();
#             }
#         }
        
#         function pollStatus() {
#             if (!polling) return;
            
#             fetch('/status')
#             .then(response => response.json())
#             .then(data => {
#                 const btn = document.getElementById('scrapeBtn');
#                 const btnText = document.getElementById('btnText');
#                 const spinner = document.getElementById('spinner');
#                 const statusBox = document.getElementById('statusBox');
#                 const statusText = document.getElementById('statusText');
                
#                 if (data.running) {
#                     statusBox.className = 'status-box loading show';
#                     statusText.innerHTML = `
#                         <strong>🔄 Scraping in Progress...</strong><br>
#                         ${data.message}<br>
#                         <small>Started: ${data.last_run || 'Just now'}</small>
#                     `;
#                     btn.disabled = true;
#                     btnText.textContent = '⏳ Scraping...';
#                     spinner.classList.add('show');
#                     setTimeout(pollStatus, 5000); // Poll every 5 seconds
#                 } else {
#                     polling = false;
#                     statusBox.className = 'status-box success show';
#                     statusText.innerHTML = `
#                         <strong>✅ Scraping Complete!</strong><br>
#                         ${data.message}<br>
#                         ${data.events_found > 0 ? `Found ${data.events_found} events. ` : ''}
#                         Check your Google Sheet for results.
#                     `;
#                     btn.disabled = false;
#                     btnText.textContent = '🚀 Start Scraping';
#                     spinner.classList.remove('show');
#                 }
#             })
#             .catch(error => {
#                 console.error('Polling error:', error);
#                 setTimeout(pollStatus, 5000);
#             });
#         }
        
#         function checkStatus() {
#             fetch('/status')
#             .then(response => response.json())
#             .then(data => {
#                 if (data.running) {
#                     const statusBox = document.getElementById('statusBox');
#                     const statusText = document.getElementById('statusText');
#                     const btn = document.getElementById('scrapeBtn');
#                     const btnText = document.getElementById('btnText');
#                     const spinner = document.getElementById('spinner');
                    
#                     statusBox.className = 'status-box loading show';
#                     statusText.innerHTML = `
#                         <strong>🔄 Scraper is Running</strong><br>
#                         ${data.message}<br>
#                         <small>Started: ${data.last_run || 'Unknown'}</small>
#                     `;
#                     btn.disabled = true;
#                     btnText.textContent = '⏳ Scraping...';
#                     spinner.classList.add('show');
#                     startPolling();
#                 }
#             });
#         }
#     </script>
# </body>
# </html>
# """

# def run_scraper_background():
#     """Run scraper in background thread"""
#     global scraper_status
    
#     print("\n" + "="*80)
#     print("🚀 STARTING SCRAPER PROCESS")
#     print("="*80)
    
#     scraper_status["running"] = True
#     scraper_status["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     scraper_status["message"] = "Scraper initializing..."
#     scraper_status["events_found"] = 0
    
#     try:
#         # Setup environment
#         env = os.environ.copy()
        
#         # Render stores secret files in /etc/secrets/
#         creds_path = "/etc/secrets/credentials.json"
        
#         # Fallback paths for local testing
#         if not os.path.exists(creds_path):
#             alt_paths = [
#                 "credentials.json",
#                 "../credentials.json",
#                 os.path.join(os.getcwd(), "credentials.json")
#             ]
#             for path in alt_paths:
#                 if os.path.exists(path):
#                     creds_path = path
#                     print(f"✅ Found credentials at: {path}")
#                     break
#             else:
#                 print("⚠️ WARNING: No credentials file found!")
        
#         env["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        
#         print(f"🔑 Credentials: {creds_path}")
#         print(f"📝 Command: scrapy crawl unfccc_events")
#         print("-"*80)
        
#         scraper_status["message"] = "Navigating pages and extracting events..."
        
#         # Run scraper with 15 minute timeout
#         result = subprocess.run(
#             ["scrapy", "crawl", "unfccc_events"],
#             capture_output=True,
#             text=True,
#             env=env,
#             timeout=900,  # 15 minutes max
#             check=True
#         )
        
#         # Parse output for event count
#         if "Total events scraped:" in result.stdout:
#             import re
#             match = re.search(r'Total events scraped:\s*(\d+)', result.stdout)
#             if match:
#                 scraper_status["events_found"] = int(match.group(1))
        
#         print("="*80)
#         print("✅ SCRAPER COMPLETED SUCCESSFULLY")
#         print("="*80)
#         print("Last 2000 chars of output:")
#         print(result.stdout[-2000:])
        
#         scraper_status["message"] = f"Completed successfully!"
        
#     except subprocess.TimeoutExpired:
#         print("⏱️ TIMEOUT: Scraper exceeded 15 minutes")
#         scraper_status["message"] = "Timed out after 15 minutes"
        
#     except subprocess.CalledProcessError as e:
#         print("="*80)
#         print("❌ SCRAPER FAILED")
#         print("="*80)
#         print(f"Exit Code: {e.returncode}")
#         print("STDOUT:", e.stdout[-2000:] if e.stdout else "None")
#         print("STDERR:", e.stderr[-1000:] if e.stderr else "None")
#         scraper_status["message"] = f"Failed: {e.stderr[:200] if e.stderr else 'Unknown error'}"
        
#     except Exception as e:
#         print(f"❌ UNEXPECTED ERROR: {e}")
#         scraper_status["message"] = f"Error: {str(e)}"
        
#     finally:
#         scraper_status["running"] = False
#         print("="*80)
#         print(f"🏁 Process ended at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#         print("="*80 + "\n")

# @app.route("/")
# def index():
#     """Main page"""
#     return render_template_string(HTML_TEMPLATE)

# @app.route("/trigger", methods=["POST"])
# def trigger():
#     """Trigger scraper - returns immediately"""
#     global scraper_status
    
#     if scraper_status["running"]:
#         return jsonify({
#             "status": "already_running",
#             "message": "Scraper is already running"
#         }), 400
    
#     # Start scraper in background thread (daemon so it doesn't block shutdown)
#     thread = threading.Thread(target=run_scraper_background, daemon=True)
#     thread.start()
    
#     # Return immediately - don't wait for scraper to finish
#     return jsonify({
#         "status": "started",
#         "message": "Scraper started in background"
#     }), 200

# @app.route("/status")
# def status():
#     """Check scraper status"""
#     return jsonify(scraper_status)

# @app.route("/health")
# def health():
#     """Health check for Render"""
#     return jsonify({"status": "healthy"}), 200

# if __name__ == "__main__":
#     port = int(os.environ.get("PORT", 8080))
#     print(f"🌐 Flask server starting on port {port}")
#     print(f"📍 Access at: http://0.0.0.0:{port}")
#     app.run(host="0.0.0.0", port=port, debug=False)

