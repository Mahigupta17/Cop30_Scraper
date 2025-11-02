import scrapy
from scrapy_playwright.page import PageMethod
from datetime import datetime
import google.generativeai as genai
import os
import re
import pytz
import json

class DynamicMVPSpider(scrapy.Spider):
    name = "mvp_tools"
    
    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'ITEM_PIPELINES': {
           'cop30_scraper.pipelines.DynamicMVPPipeline': 300,
        },
        'PLAYWRIGHT_BROWSER_TYPE': 'chromium',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'timeout': 90000,
            'args': ['--no-sandbox', '--disable-setuid-sandbox']
        },
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 90000,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'CONCURRENT_REQUESTS': 2,
        'DOWNLOAD_DELAY': 3,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scraped_count = 0
        self.failed_count = 0
        self.ist = pytz.timezone('Asia/Kolkata')
        
        # Get configuration from environment variables
        self.urls_to_scrape = self.get_urls_from_env()
        self.format_columns = self.get_format_from_env()
        
        self.logger.info(f"🎯 Initialized scraper with {len(self.urls_to_scrape)} URLs")
        self.logger.info(f"📋 Target format columns: {self.format_columns}")
    
    def get_urls_from_env(self):
        """Get URLs from environment variable"""
        urls_str = os.getenv("SCRAPER_URLS", "")
        self.logger.info(f"🔍 Reading SCRAPER_URLS from environment")
        self.logger.info(f"📝 Raw value: {urls_str[:200] if urls_str else 'EMPTY'}")
        
        if urls_str:
            urls = [url.strip() for url in urls_str.split(",") if url.strip()]
            self.logger.info(f"✅ Parsed {len(urls)} URLs from environment")
            for i, url in enumerate(urls, 1):
                self.logger.info(f"  {i}. {url}")
            return urls
        
        self.logger.error("❌ No URLs found in SCRAPER_URLS environment variable!")
        return []
    
    def get_format_from_env(self):
        """Get format columns from environment variable"""
        columns_str = os.getenv("SCRAPER_COLUMNS", "")
        self.logger.info(f"🔍 Reading SCRAPER_COLUMNS from environment")
        self.logger.info(f"📝 Raw value: {columns_str[:200] if columns_str else 'EMPTY'}")
        
        if columns_str:
            columns = [col.strip() for col in columns_str.split(",") if col.strip()]
            self.logger.info(f"✅ Parsed {len(columns)} columns from environment")
            for i, col in enumerate(columns, 1):
                self.logger.info(f"  {i}. {col}")
            return columns
        
        self.logger.warning("⚠️ No columns found in SCRAPER_COLUMNS, using defaults")
        return ["Tool Name", "Description", "Pricing", "Features", "India Available"]
    
    def start_requests(self):
        """Generate requests for all configured URLs"""
        if not self.urls_to_scrape:
            self.logger.error("❌ No URLs configured!")
            return
        
        for url in self.urls_to_scrape:
            # Ensure URL has protocol
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            
            yield scrapy.Request(
                url,
                callback=self.parse_website,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "networkidle", timeout=30000),
                    ],
                },
                errback=self.errback_close_page,
                dont_filter=True
            )
    
    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error(f"Request failed: {failure}")
        self.failed_count += 1
    
    async def parse_website(self, response):
        """Parse each website using AI based on format template"""
        page = response.meta["playwright_page"]
        
        try:
            self.logger.info(f"🔍 Scraping: {response.url}")
            
            # Wait longer for page to fully load
            await page.wait_for_timeout(5000)  # Increased from 3s to 5s
            
            # Try to wait for main content
            try:
                await page.wait_for_selector('body', timeout=10000)
            except:
                self.logger.warning(f"Body selector timeout for {response.url}")
            
            # Scroll page to trigger lazy loading
            await page.evaluate('''async () => {
                await new Promise(resolve => {
                    let scrollHeight = document.body.scrollHeight;
                    let currentScroll = 0;
                    const distance = 100;
                    const delay = 100;
                    
                    const timer = setInterval(() => {
                        window.scrollBy(0, distance);
                        currentScroll += distance;
                        
                        if (currentScroll >= scrollHeight) {
                            clearInterval(timer);
                            window.scrollTo(0, 0);
                            resolve();
                        }
                    }, delay);
                });
            }''')
            
            await page.wait_for_timeout(2000)  # Wait after scrolling
            
            # Get page content with better extraction
            page_text = await page.evaluate('''() => {
                // Remove unwanted elements
                const unwanted = document.querySelectorAll(
                    'script, style, nav, header, footer, .cookie-banner, .modal, iframe, noscript'
                );
                unwanted.forEach(el => el.remove());
                
                // Get main content - try multiple selectors
                let main = document.querySelector('main');
                if (!main) main = document.querySelector('[role="main"]');
                if (!main) main = document.querySelector('.main-content');
                if (!main) main = document.querySelector('#main');
                if (!main) main = document.body;
                
                return main.innerText;
            }''')
            
            # Get page title
            page_title = await page.title()
            
            # Get meta description as additional context
            meta_description = await page.evaluate('''() => {
                const meta = document.querySelector('meta[name="description"]');
                return meta ? meta.content : '';
            }''')
            
            # Clean and limit text
            page_text = re.sub(r'\s+', ' ', page_text).strip()
            
            # If page text is too short, site might be blocking us
            if len(page_text) < 100:
                self.logger.warning(f"⚠️ Very little content extracted from {response.url} (only {len(page_text)} chars)")
                self.logger.warning(f"This might be due to anti-bot protection or JavaScript issues")
            
            page_text = page_text[:20000]  # Limit to 20k chars
            
            # Combine all context
            full_context = f"{page_title}\n{meta_description}\n{page_text}"
            
            # Extract structured data using Gemini
            extracted_data = await self.extract_with_gemini(
                response.url,
                page_title,
                full_context,
                self.format_columns
            )
            
            if extracted_data:
                self.scraped_count += 1
                self.logger.info(f"✅ Successfully scraped {response.url} ({self.scraped_count}/{len(self.urls_to_scrape)})")
                yield extracted_data
            else:
                self.failed_count += 1
                self.logger.error(f"❌ Failed to extract data from {response.url}")
                yield self.create_fallback_item(response.url, "Extraction failed")
                
        except Exception as e:
            self.logger.error(f"❌ Error scraping {response.url}: {e}")
            self.failed_count += 1
            yield self.create_fallback_item(response.url, str(e))
        finally:
            await page.close()
    
    async def extract_with_gemini(self, url, page_title, content, columns):
        """Use Gemini to extract data based on user-defined format columns"""
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            self.logger.error("No Gemini API key found")
            return None
        
        try:
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-2.0-flash')
            
            # Build dynamic prompt based on format columns
            columns_description = "\n".join([f"- {col}: Extract information for this field" for col in columns])
            
            # Create JSON structure template
            json_template = {col: "..." for col in columns}
            
            prompt = f"""You are a data extraction expert. Extract information from this website and return ONLY valid JSON.

Website URL: {url}
Page Title: {page_title}

Website Content:
{content}

Extract the following information and return as JSON with these EXACT keys:
{columns_description}

Guidelines:
- Extract accurate information from the content
- If information is not found, use "N/A"
- Be concise but informative
- For pricing: include free tier and paid plans if available
- For availability questions: answer "Yes", "No", or "Unknown"

Return ONLY this JSON format:
{json.dumps(json_template, indent=2)}

Do not include any explanations, only the JSON object."""
            
            response = await model.generate_content_async(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.1,
                    max_output_tokens=2000
                )
            )
            
            if not response.text:
                return None
            
            # Extract JSON from response
            json_str = response.text.strip()
            json_str = re.sub(r'```(?:json)?\s*', '', json_str).strip()
            json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', json_str, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(0)
            
            data = json.loads(json_str)
            
            # Add metadata
            timestamp = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
            
            # Build result with dynamic columns
            result = {
                "scraped_at": timestamp,
                "website_url": url,
                "scraping_status": "Success"
            }
            
            # Add all extracted fields
            for col in columns:
                result[col] = data.get(col, "N/A")
            
            return result
            
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON parsing error for {url}: {e}")
            self.logger.error(f"Response was: {response.text[:500]}")
            return None
        except Exception as e:
            self.logger.error(f"Gemini extraction error for {url}: {e}")
            return None
    
    def create_fallback_item(self, url, error):
        """Create item when scraping fails"""
        timestamp = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
        result = {
            "scraped_at": timestamp,
            "website_url": url,
            "scraping_status": f"Failed: {error}"
        }
        
        # Fill format columns with N/A
        for col in self.format_columns:
            result[col] = "N/A"
        
        return result
    
    def closed(self, reason):
        """Log summary when spider closes"""
        self.logger.info("=" * 80)
        self.logger.info(f"🎯 SCRAPING SUMMARY")
        self.logger.info(f"✅ Successfully scraped: {self.scraped_count}/{len(self.urls_to_scrape)}")
        self.logger.info(f"❌ Failed: {self.failed_count}/{len(self.urls_to_scrape)}")
        if len(self.urls_to_scrape) > 0:
            self.logger.info(f"Success rate: {(self.scraped_count/len(self.urls_to_scrape)*100):.1f}%")
        self.logger.info("=" * 80)




# import scrapy
# from scrapy_playwright.page import PageMethod
# from datetime import datetime
# import google.generativeai as genai
# import os
# import re
# import pytz
# import json

# class DynamicMVPSpider(scrapy.Spider):
#     name = "mvp_tools"
    
#     custom_settings = {
#         'DOWNLOAD_HANDLERS': {
#             'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
#             'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
#         },
#         'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
#         'ITEM_PIPELINES': {
#            'cop30_scraper.pipelines.DynamicMVPPipeline': 300,
#         },
#         'PLAYWRIGHT_BROWSER_TYPE': 'chromium',
#         'PLAYWRIGHT_LAUNCH_OPTIONS': {
#             'headless': True,
#             'timeout': 90000,
#             'args': ['--no-sandbox', '--disable-setuid-sandbox']
#         },
#         'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 90000,
#         'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
#         'CONCURRENT_REQUESTS': 2,
#         'DOWNLOAD_DELAY': 3,
#     }
    
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.scraped_count = 0
#         self.failed_count = 0
#         self.ist = pytz.timezone('Asia/Kolkata')
        
#         # Get configuration from environment variables
#         self.urls_to_scrape = self.get_urls_from_env()
#         self.format_columns = self.get_format_from_env()
        
#         self.logger.info(f"🎯 Initialized scraper with {len(self.urls_to_scrape)} URLs")
#         self.logger.info(f"📋 Target format columns: {self.format_columns}")
    
#     def get_urls_from_env(self):
#         """Get URLs from environment variable"""
#         urls_str = os.getenv("SCRAPER_URLS", "")
#         if urls_str:
#             urls = [url.strip() for url in urls_str.split(",") if url.strip()]
#             return urls
#         return []
    
#     def get_format_from_env(self):
#         """Get format columns from environment variable"""
#         columns_str = os.getenv("SCRAPER_COLUMNS", "")
#         if columns_str:
#             columns = [col.strip() for col in columns_str.split(",") if col.strip()]
#             return columns
#         return ["Tool Name", "Description", "Pricing", "Features", "India Available"]
    
#     def start_requests(self):
#         """Generate requests for all configured URLs"""
#         if not self.urls_to_scrape:
#             self.logger.error("❌ No URLs configured!")
#             return
        
#         for url in self.urls_to_scrape:
#             # Ensure URL has protocol
#             if not url.startswith(('http://', 'https://')):
#                 url = 'https://' + url
            
#             yield scrapy.Request(
#                 url,
#                 callback=self.parse_website,
#                 meta={
#                     "playwright": True,
#                     "playwright_include_page": True,
#                     "playwright_page_methods": [
#                         PageMethod("wait_for_load_state", "networkidle", timeout=30000),
#                     ],
#                 },
#                 errback=self.errback_close_page,
#                 dont_filter=True
#             )
    
#     async def errback_close_page(self, failure):
#         page = failure.request.meta.get("playwright_page")
#         if page:
#             await page.close()
#         self.logger.error(f"Request failed: {failure}")
#         self.failed_count += 1
    
#     async def parse_website(self, response):
#         """Parse each website using AI based on format template"""
#         page = response.meta["playwright_page"]
        
#         try:
#             self.logger.info(f"🔍 Scraping: {response.url}")
            
#             # Wait for page to load
#             await page.wait_for_timeout(3000)
            
#             # Get page content
#             page_text = await page.evaluate('''() => {
#                 // Remove unwanted elements
#                 const unwanted = document.querySelectorAll(
#                     'script, style, nav, header, footer, .cookie-banner, .modal, iframe'
#                 );
#                 unwanted.forEach(el => el.remove());
                
#                 // Get main content
#                 const main = document.querySelector('main') || document.body;
#                 return main.innerText;
#             }''')
            
#             # Clean and limit text
#             page_text = re.sub(r'\s+', ' ', page_text).strip()[:20000]
            
#             # Get page title
#             page_title = await page.title()
            
#             # Extract structured data using Gemini based on format columns
#             extracted_data = await self.extract_with_gemini(
#                 response.url,
#                 page_title,
#                 page_text,
#                 self.format_columns
#             )
            
#             if extracted_data:
#                 self.scraped_count += 1
#                 self.logger.info(f"✅ Successfully scraped {response.url} ({self.scraped_count}/{len(self.urls_to_scrape)})")
#                 yield extracted_data
#             else:
#                 self.failed_count += 1
#                 self.logger.error(f"❌ Failed to extract data from {response.url}")
#                 yield self.create_fallback_item(response.url, "Extraction failed")
                
#         except Exception as e:
#             self.logger.error(f"❌ Error scraping {response.url}: {e}")
#             self.failed_count += 1
#             yield self.create_fallback_item(response.url, str(e))
#         finally:
#             await page.close()
    
#     async def extract_with_gemini(self, url, page_title, content, columns):
#         """Use Gemini to extract data based on user-defined format columns"""
#         api_key = os.getenv("GOOGLE_API_KEY")
#         if not api_key:
#             self.logger.error("No Gemini API key found")
#             return None
        
#         try:
#             genai.configure(api_key=api_key)
#             model = genai.GenerativeModel('gemini-2.5-flash')
            
#             # Build dynamic prompt based on format columns
#             columns_description = "\n".join([f"- {col}: Extract information for this field" for col in columns])
            
#             # Create JSON structure template
#             json_template = {col: "..." for col in columns}
            
#             prompt = f"""You are a data extraction expert. Extract information from this website and return ONLY valid JSON.

# Website URL: {url}
# Page Title: {page_title}

# Website Content:
# {content}

# Extract the following information and return as JSON with these EXACT keys:
# {columns_description}

# Guidelines:
# - Extract accurate information from the content
# - If information is not found, use "N/A"
# - Be concise but informative
# - For pricing: include free tier and paid plans if available
# - For availability questions: answer "Yes", "No", or "Unknown"

# Return ONLY this JSON format:
# {json.dumps(json_template, indent=2)}

# Do not include any explanations, only the JSON object."""
            
#             response = await model.generate_content_async(
#                 prompt,
#                 generation_config=genai.types.GenerationConfig(
#                     temperature=0.1,
#                     max_output_tokens=2000
#                 )
#             )
            
#             if not response.text:
#                 return None
            
#             # Extract JSON from response
#             json_str = response.text.strip()
#             json_str = re.sub(r'```(?:json)?\s*', '', json_str).strip()
#             json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', json_str, re.DOTALL)
            
#             if json_match:
#                 json_str = json_match.group(0)
            
#             data = json.loads(json_str)
            
#             # Add metadata
#             timestamp = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
            
#             # Build result with dynamic columns
#             result = {
#                 "scraped_at": timestamp,
#                 "website_url": url,
#                 "scraping_status": "Success"
#             }
            
#             # Add all extracted fields
#             for col in columns:
#                 result[col] = data.get(col, "N/A")
            
#             return result
            
#         except json.JSONDecodeError as e:
#             self.logger.error(f"JSON parsing error for {url}: {e}")
#             self.logger.error(f"Response was: {response.text[:500]}")
#             return None
#         except Exception as e:
#             self.logger.error(f"Gemini extraction error for {url}: {e}")
#             return None
    
#     def create_fallback_item(self, url, error):
#         """Create item when scraping fails"""
#         timestamp = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
#         result = {
#             "scraped_at": timestamp,
#             "website_url": url,
#             "scraping_status": f"Failed: {error}"
#         }
        
#         # Fill format columns with N/A
#         for col in self.format_columns:
#             result[col] = "N/A"
        
#         return result
    
#     def closed(self, reason):
#         """Log summary when spider closes"""
#         self.logger.info("=" * 80)
#         self.logger.info(f"🎯 SCRAPING SUMMARY")
#         self.logger.info(f"✅ Successfully scraped: {self.scraped_count}/{len(self.urls_to_scrape)}")
#         self.logger.info(f"❌ Failed: {self.failed_count}/{len(self.urls_to_scrape)}")
#         if len(self.urls_to_scrape) > 0:
#             self.logger.info(f"Success rate: {(self.scraped_count/len(self.urls_to_scrape)*100):.1f}%")
#         self.logger.info("=" * 80)


