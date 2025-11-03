import scrapy
from scrapy_playwright.page import PageMethod
from datetime import datetime, date
from cop30_scraper.items import Cop30ScraperItem
import re
import os
import json
import google.generativeai as genai

class UNFCCCEventSpider(scrapy.Spider):
    name = "unfccc_events"

    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'ITEM_PIPELINES': {
           'cop30_scraper.pipelines.GoogleSheetsPipeline': 300,
        },
        'PLAYWRIGHT_BROWSER_TYPE': 'chromium',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'timeout': 90000,
            'args': ['--no-sandbox', '--disable-setuid-sandbox']
        },
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 90000,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 2,
    }
    
    START_DATE = date(2025, 10, 1)
    END_DATE = date(2025, 11, 10)
    
    start_urls = ["https://unfccc.int/calendar/events-list"]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.events_scraped = 0
        self.seen_urls = set()
    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_all_pages,
                meta={"playwright": True, "playwright_include_page": True},
                errback=self.errback_close_page,
            )

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error(f"Request failed: {failure}")

    def parse_event_date(self, date_str_raw):
        """Enhanced date parser that handles ALL formats including day names"""
        try:
            date_str_raw = re.sub(r'\s+', ' ', date_str_raw).strip()
            
            # Skip month headers like "OCTOBER 2025"
            if re.match(r'^[A-Z]+\s+\d{4}$', date_str_raw):
                return None
            
            # Skip "Date to be confirmed"
            if 'Date to be confirmed' in date_str_raw:
                return None
            
            # Extract year (default to 2025 if not found)
            year_match = re.search(r'\b(20\d{2})\b', date_str_raw)
            year = year_match.group(1) if year_match else "2025"
            
            # Handle different date formats
            
            # Format 1: Date ranges like "1st October - 3rd October 2025"
            if ' - ' in date_str_raw:
                start_date_str = date_str_raw.split(' - ')[0].strip()
                # Remove day names and ordinals from start date
                start_date_str = re.sub(r'^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*', '', start_date_str)
                start_date_str = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', start_date_str)
                cleaned_date_str = f"{start_date_str} {year}"
            
            # Format 2: Single dates with day names like "Thursday, 2nd October 2025"
            elif re.match(r'^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)', date_str_raw):
                # Remove day name and comma
                date_without_day = re.sub(r'^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*', '', date_str_raw)
                # Remove ordinal indicators
                date_without_day = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_without_day)
                # Remove year if present and use our extracted year
                date_without_day = re.sub(r'\s*20\d{2}$', '', date_without_day).strip()
                cleaned_date_str = f"{date_without_day} {year}"
            
            # Format 3: Simple dates
            else:
                cleaned_date_str = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str_raw)
                cleaned_date_str = re.sub(r'\s*20\d{2}$', '', cleaned_date_str).strip()
                cleaned_date_str = f"{cleaned_date_str} {year}"
            
            # Remove any remaining commas
            cleaned_date_str = re.sub(r',', '', cleaned_date_str).strip()
            
            # Try all possible formats in order of likelihood
            formats = [
                '%d %B %Y',           # "1 October 2025" (most common)
                '%d %b %Y',           # "1 Oct 2025"
                '%B %d %Y',           # "October 1 2025"
                '%b %d %Y',           # "Oct 1 2025"
                '%A %d %B %Y',        # "Thursday 2 October 2025"
                '%A %d %b %Y',        # "Thursday 2 Oct 2025"
            ]
            
            for fmt in formats:
                try:
                    parsed_date = datetime.strptime(cleaned_date_str, fmt).date()
                    self.logger.debug(f"✓ Date parsed: '{date_str_raw}' -> {parsed_date} (format: {fmt})")
                    return parsed_date
                except ValueError:
                    continue
                    
            self.logger.warning(f"⚠️  Could not parse date format: '{date_str_raw}' -> '{cleaned_date_str}'")
                    
        except Exception as e:
            self.logger.warning(f"Date parse error: '{date_str_raw}' - {e}")
        return None

    async def parse_all_pages(self, response):
        """Parse ALL pages by navigating through pagination"""
        page = response.meta["playwright_page"]
        
        try:
            # Wait for table to load
            self.logger.info("⏳ Waiting for DataTables to initialize...")
            await page.wait_for_selector('#DataTables_Table_0', timeout=60000)
            await page.wait_for_timeout(5000)
            
            # Set page length to show more events
            self.logger.info("🔧 Setting page length to 100...")
            await page.evaluate('''() => {
                try {
                    const table = $('#DataTables_Table_0').DataTable();
                    table.page.len(100).draw();
                    return true;
                } catch(e) { 
                    console.log('DataTable error:', e);
                    return false;
                }
            }''')
            await page.wait_for_timeout(5000)
            
            page_num = 0
            has_more_pages = True
            found_events_count = 0
            
            while has_more_pages and page_num < 50:  # Safety limit
                page_num += 1
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"📄 PROCESSING PAGE {page_num}")
                self.logger.info(f"{'='*60}")
                
                # Wait for page to stabilize
                await page.wait_for_timeout(3000)
                
                # Get current page content
                content = await page.content()
                from scrapy.http import HtmlResponse
                html_resp = HtmlResponse(url=page.url, body=content, encoding='utf-8')
                
                # Parse events on current page
                rows = html_resp.css('#DataTables_Table_0 tbody tr')
                self.logger.info(f"📊 Found {len(rows)} rows on page {page_num}")
                
                if not rows:
                    self.logger.warning("❌ No rows found - stopping")
                    break
                
                page_events_count = 0
                page_earliest_date = None
                page_latest_date = None
                
                for idx, row in enumerate(rows, 1):
                    try:
                        # Extract date from first column
                        date_cell = row.css('td:nth-child(1)')
                        date_texts = date_cell.css('::text').getall()
                        date_str = " ".join([text.strip() for text in date_texts if text.strip()])
                        
                        if not date_str:
                            continue
                            
                        # Parse the date
                        parsed_date = self.parse_event_date(date_str)
                        
                        if not parsed_date:
                            # Skip month headers and unparseable dates
                            continue
                        
                        # Track date range for this page
                        if page_earliest_date is None or parsed_date < page_earliest_date:
                            page_earliest_date = parsed_date
                        if page_latest_date is None or parsed_date > page_latest_date:
                            page_latest_date = parsed_date
                        
                        # Check if date is within our target range
                        if self.START_DATE <= parsed_date <= self.END_DATE:
                            # Extract event URL and title
                            event_url = None
                            event_title = "Unknown Title"
                            
                            # Try multiple selectors for URL - check all columns
                            for col_idx in [2, 3, 4]:  # Try different columns
                                url = row.css(f'td:nth-child({col_idx}) a::attr(href)').get()
                                if url:
                                    event_url = url
                                    break
                            
                            # Extract title from the same column where we found the URL
                            if event_url:
                                # Get text from the link or the cell
                                for col_idx in [2, 3, 4]:
                                    title = row.css(f'td:nth-child({col_idx}) a::text').get()
                                    if title and title.strip():
                                        event_title = title.strip()
                                        break
                                # If no link text, try cell text
                                if event_title == "Unknown Title":
                                    for col_idx in [2, 3, 4]:
                                        title = row.css(f'td:nth-child({col_idx})::text').get()
                                        if title and title.strip():
                                            event_title = title.strip()
                                            break
                            
                            if event_url:
                                full_url = html_resp.urljoin(event_url)
                                
                                if full_url in self.seen_urls:
                                    continue
                                
                                self.seen_urls.add(full_url)
                                found_events_count += 1
                                page_events_count += 1
                                
                                self.logger.info(f"✅ EVENT #{found_events_count}: {parsed_date} | {event_title[:60]}...")
                                
                                # Yield event for detailed scraping
                                yield scrapy.Request(
                                    full_url,
                                    callback=self.parse_with_gemini,
                                    meta={
                                        "event_date": date_str,
                                        "event_title": event_title,  # Pass the title from list page
                                        "playwright": True,
                                        "playwright_page_methods": [
                                            PageMethod("wait_for_load_state", "networkidle", timeout=30000),
                                        ],
                                    },
                                )
                            else:
                                self.logger.warning(f"⚠️  No URL found for event: {date_str}")
                    
                    except Exception as e:
                        self.logger.error(f"❌ Error processing row {idx}: {e}")
                        continue
                
                self.logger.info(f"📈 Page {page_num}: {page_events_count} events in target range")
                if page_earliest_date and page_latest_date:
                    self.logger.info(f"📅 Page date range: {page_earliest_date} to {page_latest_date}")
                
                # Check if we should stop (all dates on page are after our end date)
                if page_earliest_date and page_earliest_date > self.END_DATE:
                    self.logger.info(f"🏁 All dates on page are after {self.END_DATE} - stopping")
                    break
                
                # Check if we should stop (all dates on page are before our start date)  
                if page_latest_date and page_latest_date < self.START_DATE:
                    self.logger.info(f"🏁 All dates on page are before {self.START_DATE} - stopping")
                    break
                
                # Try to navigate to next page
                next_button = await page.query_selector('#DataTables_Table_0_next:not(.disabled)')
                
                if not next_button:
                    self.logger.info("✅ No more pages available")
                    has_more_pages = False
                else:
                    self.logger.info("🔄 Clicking next page...")
                    await next_button.click()
                    await page.wait_for_timeout(3000)
                    
                    # Verify page actually changed
                    try:
                        await page.wait_for_function(
                            '''() => {
                                const info = document.querySelector('#DataTables_Table_0_info');
                                return info && !info.textContent.includes('Loading');
                            }''',
                            timeout=10000
                        )
                    except:
                        self.logger.warning("⚠️  Page change verification timeout")
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"🎯 SCRAPING COMPLETED")
            self.logger.info(f"📊 Total events found: {found_events_count}")
            self.logger.info(f"📊 Unique URLs: {len(self.seen_urls)}")
            self.logger.info(f"{'='*60}")
            
        except Exception as e:
            self.logger.error(f"❌ Error in parse_all_pages: {e}")
        finally:
            if page:
                await page.close()

    async def parse_with_gemini(self, response):
        """Parse detail page with Gemini - IMPROVED VERSION"""
        self.logger.info(f"🔍 Parsing event details: {response.url}")
        
        event_date = response.meta.get("event_date", "N/A")
        list_title = response.meta.get("event_title", "N/A")
        api_key = os.getenv("GOOGLE_API_KEY")
        
        if not api_key:
            self.logger.error("❌ No Google API key found")
            yield self.parse_generic(response, "No API key", event_date, list_title)
            return

        try:
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-2.0-flash')

            # Extract text content more selectively
            page_text = self.extract_structured_content(response)
            
            prompt = self.create_enhanced_prompt(page_text, list_title)
            
            api_response = await model.generate_content_async(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.1,
                    top_p=0.8,
                    top_k=40
                )
            )
            
            if not api_response.text:
                self.logger.warning("⚠️  Empty response from Gemini")
                yield self.parse_generic(response, "Empty Gemini response", event_date, list_title)
                return
            
            json_str = api_response.text.strip()
            json_str = re.sub(r'```(?:json)?\s*', '', json_str).strip()
            
            # Extract JSON from response
            json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', json_str, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
            
            data = json.loads(json_str)

            item = Cop30ScraperItem()
            item["Scheduled"] = event_date
            item["Time_Location"] = data.get("time_and_location", "N/A")
            item["Organizer"] = response.url
            
            # Improved tags handling
            tags = data.get("tags", [])
            if isinstance(tags, str):
                tags = [tag.strip() for tag in tags.split(",") if tag.strip()]
            elif not isinstance(tags, list):
                tags = ["N/A"]
            
            # Remove duplicates and limit tags
            tags = list(dict.fromkeys(tags))[:8]  # Keep first 8 unique tags
            item["Tags"] = ", ".join(tags)
            
            item["Title_Theme_Speakers"] = {
                "title": data.get("title", list_title),  # Use list title as fallback
                "theme": data.get("theme", "N/A"),
                "speakers": data.get("speakers", "N/A")
            }
            
            self.logger.info(f"✅ Created item: {item['Title_Theme_Speakers']['title'][:50]}...")
            self.logger.info(f"📝 Theme: {item['Title_Theme_Speakers']['theme'][:80]}...")
            self.logger.info(f"🏷️  Tags: {item['Tags']}")
            
            yield item
            
        except Exception as e:
            self.logger.error(f"❌ Gemini error for {response.url}: {e}")
            yield self.parse_generic(response, str(e), event_date, list_title)

    def extract_structured_content(self, response):
        """Extract structured content from the page for better Gemini processing"""
        # Remove script and style elements
        for element in response.css('script, style, nav, header, footer'):
            element.drop()
        
        # Extract key sections with priority
        content_parts = []
        
        # 1. Main content area
        main_content = response.css('main, .content, .main-content, #content, .event-details')
        if main_content:
            content_parts.append("MAIN CONTENT:")
            content_parts.append(" ".join(main_content.css('::text').getall()))
        
        # 2. Article or event body
        article_content = response.css('article, .article, .event-content, .body')
        if article_content:
            content_parts.append("ARTICLE CONTENT:")
            content_parts.append(" ".join(article_content.css('::text').getall()))
        
        # 3. Headings
        headings = response.css('h1, h2, h3, h4, h5, h6::text').getall()
        if headings:
            content_parts.append("HEADINGS:")
            content_parts.append(" | ".join(headings))
        
        # 4. Any remaining body text
        body_text = " ".join(response.css('body ::text').getall())
        content_parts.append("FULL TEXT:")
        content_parts.append(body_text)
        
        # Combine and clean
        full_text = "\n\n".join(content_parts)
        full_text = re.sub(r'\s+', ' ', full_text).strip()
        
        return full_text[:12000]  # Limit length

    def create_enhanced_prompt(self, page_text, list_title):
        """Create enhanced prompt for better theme and tag extraction"""
        return f"""Analyze this UNFCCC event page and extract key information. Focus on understanding the event's purpose, scope, and relevance to climate change topics.

PAGE CONTEXT:
List Title: {list_title}
Page Content: {page_text}

EXTRACTION REQUIREMENTS:

1. TITLE: Use the most descriptive title available. Prefer the list title if it's meaningful, otherwise extract from page content.

2. THEME: Write a comprehensive 1-2 sentence description that explains:
   - What the event is about
   - Its purpose and objectives
   - Key topics being discussed
   - Relevance to climate change negotiations

3. SPEAKERS: Extract names of speakers, panelists, or key participants. If none found, use "N/A".

4. TIME AND LOCATION: Extract time (if available) and location details. Include timezone if mentioned. Do NOT include dates.

5. TAGS: Select 5-8 relevant tags from these categories:
   - Climate topics: Mitigation, Adaptation, Loss and Damage, Finance, Technology, Transparency, etc.
   - Event types: Workshop, Conference, Meeting, Panel, Summit, Consultation
   - UNFCCC bodies: COP, CMA, CMP, SBI, SBSTA, APA, etc.
   - Specific mechanisms: Paris Agreement, Kyoto Protocol, NDCs, Global Stocktake, etc.

OUTPUT FORMAT: Return ONLY valid JSON with this structure:
{{
  "title": "Event Title",
  "theme": "Comprehensive description of the event's purpose and content",
  "speakers": "Speaker names or N/A",
  "time_and_location": "Time and location details without dates",
  "tags": ["Tag1", "Tag2", "Tag3"]
}}

IMPORTANT: Make the theme descriptive and informative. Focus on what the event aims to achieve and its significance in climate policy."""

    def parse_generic(self, response, error_msg, event_date="N/A", list_title="N/A"):
        """Fallback parser with improved generic extraction"""
        # Try to extract some basic info without Gemini
        title = list_title if list_title != "N/A" else "Scraping Failed"
        
        # Extract basic theme from headings
        headings = response.css('h1, h2::text').getall()
        theme = " | ".join(headings[:3]) if headings else error_msg
        
        # Generate basic tags from URL and title
        tags_keywords = []
        url_lower = response.url.lower()
        title_lower = title.lower()
        
        # Add tags based on common UNFCCC terms
        unfccc_terms = ['cop', 'cmp', 'cma', 'sbi', 'sbsta', 'nda', 'ndc', 'paris', 'kyoto', 'mitigation', 
                       'adaptation', 'finance', 'technology', 'transparency', 'workshop', 'meeting', 'conference']
        
        for term in unfccc_terms:
            if term in url_lower or term in title_lower:
                tags_keywords.append(term.title())
        
        tags = ", ".join(tags_keywords[:5]) if tags_keywords else "UNFCCC Event"
        
        return Cop30ScraperItem(
            Scheduled=event_date,
            Time_Location="N/A",
            Organizer=response.url,
            Tags=tags,
            Title_Theme_Speakers={
                "title": title,
                "theme": theme,
                "speakers": "N/A"
            }
        )




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
#         self.logger.info(f"🔍 Reading SCRAPER_URLS from environment")
#         self.logger.info(f"📝 Raw value: {urls_str[:200] if urls_str else 'EMPTY'}")
        
#         if urls_str:
#             urls = [url.strip() for url in urls_str.split(",") if url.strip()]
#             self.logger.info(f"✅ Parsed {len(urls)} URLs from environment")
#             for i, url in enumerate(urls, 1):
#                 self.logger.info(f"  {i}. {url}")
#             return urls
        
#         self.logger.error("❌ No URLs found in SCRAPER_URLS environment variable!")
#         return []
    
#     def get_format_from_env(self):
#         """Get format columns from environment variable"""
#         columns_str = os.getenv("SCRAPER_COLUMNS", "")
#         self.logger.info(f"🔍 Reading SCRAPER_COLUMNS from environment")
#         self.logger.info(f"📝 Raw value: {columns_str[:200] if columns_str else 'EMPTY'}")
        
#         if columns_str:
#             columns = [col.strip() for col in columns_str.split(",") if col.strip()]
#             self.logger.info(f"✅ Parsed {len(columns)} columns from environment")
#             for i, col in enumerate(columns, 1):
#                 self.logger.info(f"  {i}. {col}")
#             return columns
        
#         self.logger.warning("⚠️ No columns found in SCRAPER_COLUMNS, using defaults")
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
#         url = response.url
        
#         self.logger.info(f"=" * 80)
#         self.logger.info(f"🔍 STARTING SCRAPE: {url}")
#         self.logger.info(f"=" * 80)
        
#         try:
#             # Wait longer for page to fully load
#             await page.wait_for_timeout(5000)
            
#             # Try to wait for main content
#             try:
#                 await page.wait_for_selector('body', timeout=10000)
#                 self.logger.info(f"✅ Body loaded for {url}")
#             except Exception as e:
#                 self.logger.warning(f"⚠️ Body selector timeout for {url}: {e}")
            
#             # Scroll page to trigger lazy loading
#             self.logger.info(f"📜 Scrolling page: {url}")
#             await page.evaluate('''async () => {
#                 await new Promise(resolve => {
#                     let scrollHeight = document.body.scrollHeight;
#                     let currentScroll = 0;
#                     const distance = 100;
#                     const delay = 100;
                    
#                     const timer = setInterval(() => {
#                         window.scrollBy(0, distance);
#                         currentScroll += distance;
                        
#                         if (currentScroll >= scrollHeight) {
#                             clearInterval(timer);
#                             window.scrollTo(0, 0);
#                             resolve();
#                         }
#                     }, delay);
#                 });
#             }''')
            
#             await page.wait_for_timeout(2000)
            
#             # Get page content with better extraction
#             self.logger.info(f"📄 Extracting content from: {url}")
#             page_text = await page.evaluate('''() => {
#                 const unwanted = document.querySelectorAll(
#                     'script, style, nav, header, footer, .cookie-banner, .modal, iframe, noscript'
#                 );
#                 unwanted.forEach(el => el.remove());
                
#                 let main = document.querySelector('main');
#                 if (!main) main = document.querySelector('[role="main"]');
#                 if (!main) main = document.querySelector('.main-content');
#                 if (!main) main = document.querySelector('#main');
#                 if (!main) main = document.body;
                
#                 return main.innerText;
#             }''')
            
#             page_title = await page.title()
#             meta_description = await page.evaluate('''() => {
#                 const meta = document.querySelector('meta[name="description"]');
#                 return meta ? meta.content : '';
#             }''')
            
#             # Clean text
#             page_text = re.sub(r'\s+', ' ', page_text).strip()
            
#             self.logger.info(f"📊 Content stats for {url}:")
#             self.logger.info(f"  - Title: {page_title}")
#             self.logger.info(f"  - Content length: {len(page_text)} chars")
#             self.logger.info(f"  - First 200 chars: {page_text[:200]}")
            
#             # Check if content is sufficient
#             if len(page_text) < 100:
#                 self.logger.error(f"❌ Insufficient content from {url} (only {len(page_text)} chars)")
#                 self.logger.error(f"This site may be blocking automated access")
#                 self.failed_count += 1
#                 yield self.create_fallback_item(url, f"Insufficient content ({len(page_text)} chars) - possible bot detection")
#                 return
            
#             page_text = page_text[:20000]
#             full_context = f"{page_title}\n{meta_description}\n{page_text}"
            
#             # Extract with Gemini
#             self.logger.info(f"🤖 Sending to Gemini for extraction: {url}")
#             extracted_data = await self.extract_with_gemini(
#                 url,
#                 page_title,
#                 full_context,
#                 self.format_columns
#             )
            
#             if extracted_data:
#                 self.scraped_count += 1
#                 self.logger.info(f"=" * 80)
#                 self.logger.info(f"✅ SUCCESS ({self.scraped_count}/{len(self.urls_to_scrape)}): {url}")
#                 self.logger.info(f"=" * 80)
#                 yield extracted_data
#             else:
#                 self.failed_count += 1
#                 self.logger.error(f"=" * 80)
#                 self.logger.error(f"❌ FAILED ({self.failed_count}/{len(self.urls_to_scrape)}): {url}")
#                 self.logger.error(f"=" * 80)
#                 yield self.create_fallback_item(url, "Gemini extraction returned None")
                
#         except Exception as e:
#             self.logger.error(f"=" * 80)
#             self.logger.error(f"❌ EXCEPTION for {url}: {type(e).__name__}")
#             self.logger.error(f"Error: {str(e)}")
#             import traceback
#             self.logger.error(f"Traceback:\n{traceback.format_exc()}")
#             self.logger.error(f"=" * 80)
            
#             self.failed_count += 1
#             yield self.create_fallback_item(url, f"{type(e).__name__}: {str(e)}")
#         finally:
#             try:
#                 await page.close()
#                 self.logger.info(f"🔒 Page closed for {url}")
#             except Exception as e:
#                 self.logger.warning(f"⚠️ Error closing page for {url}: {e}")
    
#     async def extract_with_gemini(self, url, page_title, content, columns):
#         """Use Gemini to extract data based on user-defined format columns"""
#         api_key = os.getenv("GOOGLE_API_KEY")
#         if not api_key:
#             self.logger.error("No Gemini API key found")
#             return None
        
#         try:
#             genai.configure(api_key=api_key)
#             model = genai.GenerativeModel('gemini-2.0-flash')
            
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



