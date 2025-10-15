# Scrapy settings for cop30_scraper project

BOT_NAME = "cop30_scraper"

SPIDER_MODULES = ["cop30_scraper.spiders"]
NEWSPIDER_MODULE = "cop30_scraper.spiders"

# --- *** ADD A STANDARD USER AGENT *** ---
# This makes our scraper look like a real browser.
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'

# Google Sheets pipeline
ITEM_PIPELINES = {
   "cop30_scraper.pipelines.GoogleSheetsPipeline": 300,
}

DOWNLOAD_HANDLERS = {
   "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
   "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# Browser settings
PLAYWRIGHT_BROWSER_TYPE = "chromium"

# --- *** NEW LAUNCH OPTIONS TO AVOID BOT DETECTION *** ---
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "args": [
        "--disable-blink-features=AutomationControlled",
    ]
}

PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 120000    # 120 seconds
PLAYWRIGHT_MAX_PAGES_PER_CONTEXT = 3
LOG_LEVEL = "INFO"

# Reactor
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"



