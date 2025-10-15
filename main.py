from flask import Flask, jsonify, render_template_string
import subprocess
import os

# Initialize the Flask web server
app = Flask(__name__)

# This is the HTML for the webpage with the button
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>COP30 Scraper Control</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; background-color: #f0f2f5; margin: 0; }
        .container { text-align: center; background: white; padding: 40px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.1); }
        h1 { color: #333; }
        p { color: #666; }
        button { background-color: #007bff; color: white; border: none; padding: 15px 30px; font-size: 16px; border-radius: 8px; cursor: pointer; transition: background-color 0.3s; }
        button:hover { background-color: #0056b3; }
        button:disabled { background-color: #cccccc; cursor: not-allowed; }
        #status { margin-top: 20px; font-weight: bold; color: #333; }
    </style>
</head>
<body>
    <div class="container">
        <h1>COP30 Event Scraper</h1>
        <p>Click the button below to start scraping the UNFCCC website.</p>
        <button id="runButton" onclick="startScraper()">Run Scraper</button>
        <p id="status"></p>
    </div>

    <script>
        function startScraper() {
            const button = document.getElementById('runButton');
            const status = document.getElementById('status');

            // Disable the button and show a "starting" message
            button.disabled = true;
            status.textContent = 'Starting scraper... Please wait.';
            status.style.color = '#333';

            // Make a request to our server's /run endpoint
            fetch('/run', { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    // Update status with the server's response
                    status.textContent = data.message;
                    status.style.color = 'green';
                    // The scraper is running in the background, so we don't re-enable the button immediately.
                })
                .catch(error => {
                    console.error('Error:', error);
                    status.textContent = 'Error starting scraper. Check logs.';
                    status.style.color = 'red';
                    button.disabled = false; // Re-enable button on error
                });
        }
    </script>
</body>
</html>
"""

# This is the main webpage
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

# This is the endpoint that the button click will call
@app.route('/run', methods=['POST'])
def run_scraper():
    """
    This function starts the Scrapy spider as a background process.
    It does NOT wait for the scraper to finish, so it can return a message
    to the user immediately.
    """
    print("Received request to start scraper...")
    try:
        # Use subprocess.Popen to run the command in the background
        subprocess.Popen(["scrapy", "crawl", "unfccc_events"])
        print("Scraper process started successfully.")
        return jsonify({"message": "Scraper process started! Check your Google Sheet for results in a few minutes."}), 200
    except Exception as e:
        print(f"Error starting scraper: {e}")
        return jsonify({"message": f"Error: {e}"}), 500

if __name__ == "__main__":
    # Get the port from the environment variable Render provides
    port = int(os.environ.get("PORT", 8080))
    # Run the web server
    app.run(host='0.0.0.0', port=port)