from flask import Flask, render_template_string, request # <--- THE FIX IS HERE
import subprocess
import os
import threading

app = Flask(__name__)

# --- HTML Template for the webpage ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>COP30 Event Scraper</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background-color: #f0f2f5; }
        .container { background: white; padding: 40px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); text-align: center; max-width: 500px; }
        h1 { color: #333; }
        p { color: #666; margin-bottom: 24px; }
        button { background-color: #007aff; color: white; border: none; padding: 12px 24px; border-radius: 6px; font-size: 16px; cursor: pointer; transition: background-color 0.2s; }
        button:disabled { background-color: #cccccc; cursor: not-allowed; }
        .message { margin-top: 20px; color: #28a745; font-weight: bold; }
    </style>
</head>
<body>
    <div class="container">
        <h1>COP30 Event Scraper</h1>
        <p>Click the button below to start scraping the UNFCCC website.</p>
        <form method="post">
            <button type="submit" id="run-button">Run Scraper</button>
        </form>
        <div class="message" id="message-div" style="display: none;">
            Scraper process started! Check your Render logs and Google Sheet for results in a few minutes.
        </div>
    </div>
    <script>
        document.querySelector('form').addEventListener('submit', function(e) {
            const button = document.getElementById('run-button');
            const message = document.getElementById('message-div');
            button.disabled = true;
            button.innerText = 'Scraping...';
            message.style.display = 'block';
        });
    </script>
</body>
</html>
"""

def run_scraper():
    """Runs the Scrapy crawl command in a subprocess."""
    print("--- Starting Scrapy Process ---")
    try:
        # Create a new environment for the subprocess and pass our secrets to it.
        proc_env = os.environ.copy()
        
        # Tell the subprocess where to find the credentials file.
        # Render automatically places secret files at '/etc/secrets/'.
        proc_env["GOOGLE_APPLICATION_CREDENTIALS"] = "/etc/secrets/credentials.json"
        
        # Run the scraper command with the correct environment
        result = subprocess.run(
            ["scrapy", "crawl", "unfccc_events"], 
            check=True, 
            capture_output=True, 
            text=True,
            env=proc_env
        )
        print("--- Scrapy Process Finished Successfully ---")
        print(f"Stdout: {result.stdout}")
        print(f"Stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print("--- Scrapy Process Failed ---")
        print(f"Return Code: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
    except Exception as e:
        print(f"An unexpected error occurred while running the scraper: {e}")

@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Run the scraper in a background thread so the webpage doesn't time out
        thread = threading.Thread(target=run_scraper)
        thread.start()
        return render_template_string(HTML_TEMPLATE)
    return render_template_string(HTML_TEMPLATE)

if __name__ == "__main__":
    # The port is set by Render's environment, defaulting to 8080 for local testing
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)