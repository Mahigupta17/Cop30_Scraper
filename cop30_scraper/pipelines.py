import gspread
from google.oauth2.service_account import Credentials
import os
from datetime import datetime
import pytz

class DynamicMVPPipeline:
    def __init__(self):
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not creds_path:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        
        self.creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        self.client = gspread.authorize(self.creds)
        
        # Use your existing spreadsheet
        self.spreadsheet_id = "1tDFA7DIRm0b-9mbZby2lNyfgYJtGXSjOwl5ezN3CeME"
        
        # Get sheet name from environment or create new one
        list_name = os.getenv("SCRAPER_LIST_NAME", "MVP_Tools_Data")
        self.sheet_name = list_name.replace(" ", "_")
        
        try:
            spreadsheet = self.client.open_by_key(self.spreadsheet_id)
            self.sheet = spreadsheet.worksheet(self.sheet_name)
        except:
            # Create new worksheet if it doesn't exist
            spreadsheet = self.client.open_by_key(self.spreadsheet_id)
            self.sheet = spreadsheet.add_worksheet(
                title=self.sheet_name, 
                rows="1000", 
                cols="20"
            )
        
        self.ist = pytz.timezone('Asia/Kolkata')
        self.session_start = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
        self.items_scraped = 0
        
        # Get format columns from environment
        self.format_columns = self.get_format_columns()
        
        # Add headers if sheet is empty
        self.initialize_headers()
    
    def get_format_columns(self):
        """Get format columns from environment variable"""
        columns_str = os.getenv("SCRAPER_COLUMNS", "")
        if columns_str:
            columns = [col.strip() for col in columns_str.split(",") if col.strip()]
            return columns
        return ["Tool Name", "Description", "Pricing", "Features"]
    
    def initialize_headers(self):
        """Add headers based on format columns"""
        try:
            if not self.sheet.get('A1'):
                # Build header row: Timestamp + URL + Format Columns + Status
                headers = ["Scraped At", "Website URL"] + self.format_columns + ["Status"]
                self.sheet.append_row(headers)
                
                # Format header row
                end_col = chr(65 + len(headers) - 1)  # Convert to letter (A, B, C...)
                self.sheet.format(f'A1:{end_col}1', {
                    "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.8},
                    "textFormat": {
                        "bold": True, 
                        "foregroundColor": {"red": 1, "green": 1, "blue": 1}
                    },
                    "horizontalAlignment": "CENTER"
                })
                print(f"✅ Created headers: {headers}")
        except Exception as e:
            print(f"Could not add headers: {e}")

    def process_item(self, item, spider):
        """Write each scraped item to Google Sheets"""
        
        timestamp = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
        
        # Build row dynamically based on format columns
        row = [
            timestamp,
            item.get("website_url", "N/A"),
        ]
        
        # Add data for each format column
        for col in self.format_columns:
            row.append(item.get(col, "N/A"))
        
        # Add status at the end
        row.append(item.get("scraping_status", "Success"))

        # Debug logging
        spider.logger.info("=" * 70)
        spider.logger.info(f"Writing to Google Sheets:")
        spider.logger.info(f"  URL: {item.get('website_url')}")
        for col in self.format_columns[:3]:  # Log first 3 columns
            value = item.get(col, "N/A")
            spider.logger.info(f"  {col}: {str(value)[:50]}...")
        spider.logger.info(f"  Status: {item.get('scraping_status')}")
        spider.logger.info("=" * 70)

        # Append the row
        self.sheet.append_row(row, value_input_option='USER_ENTERED')
        
        # Get the row number
        row_number = len(self.sheet.get_all_values())
        
        # Format based on status
        try:
            end_col = chr(65 + len(row) - 1)
            
            # NO background color for data rows - keep them white/default
            # Only format text wrapping for long text columns
            for i, col in enumerate(self.format_columns, start=3):  # Start from column C
                col_letter = chr(65 + i - 1)
                self.sheet.format(f'{col_letter}{row_number}', {
                    "wrapStrategy": "WRAP"
                })
                
            # Only add red background for failures
            if item.get("scraping_status") != "Success":
                self.sheet.format(f'A{row_number}:{end_col}{row_number}', {
                    "backgroundColor": {"red": 1, "green": 0.9, "blue": 0.9}
                })
                
        except Exception as e:
            spider.logger.warning(f"Could not format row: {e}")
        
        self.items_scraped += 1
        spider.logger.info(f"✅ Added to sheet (row {row_number})") 
        return item
    
    def close_spider(self, spider):
        """Add separator when scraping completes"""
        try:
            session_end = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
            session_date = datetime.now(self.ist).strftime("%Y-%m-%d")
            
            separator_text = f"═══ SCRAPING COMPLETED on {session_date} ═══"
            summary_text = f"Started: {self.session_start} | Ended: {session_end} | Items: {self.items_scraped}"
            
            # Create row with enough cells
            num_cols = 2 + len(self.format_columns) + 1
            separator_row = [separator_text, summary_text] + [""] * (num_cols - 2)
            
            self.sheet.append_row(separator_row, value_input_option='USER_ENTERED')
            row_number = len(self.sheet.get_all_values())
            
            # Format separator
            end_col = chr(65 + num_cols - 1)
            self.sheet.format(f'A{row_number}:{end_col}{row_number}', {
                "backgroundColor": {"red": 0.2, "green": 0.8, "blue": 0.2},
                "textFormat": {
                    "bold": True,
                    "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                    "fontSize": 11
                },
                "horizontalAlignment": "CENTER"
            })
            
            self.sheet.merge_cells(f'A{row_number}:{end_col}{row_number}')
            
            spider.logger.info("=" * 80)
            spider.logger.info(f"✅ Added separator row")
            spider.logger.info(f"📊 Total items scraped: {self.items_scraped}")
            spider.logger.info("=" * 80)
            
        except Exception as e:
            spider.logger.error(f"❌ Could not add separator: {e}")




#  import gspread
# from google.oauth2.service_account import Credentials
# import os
# from datetime import datetime
# import pytz

# class DynamicMVPPipeline:
#     def __init__(self):
#         creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
#         if not creds_path:
#             raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")

#         scopes = [
#             "https://www.googleapis.com/auth/spreadsheets",
#             "https://www.googleapis.com/auth/drive",
#         ]
        
#         self.creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
#         self.client = gspread.authorize(self.creds)
        
#         # Use your existing spreadsheet
#         self.spreadsheet_id = "1tDFA7DIRm0b-9mbZby2lNyfgYJtGXSjOwl5ezN3CeME"
        
#         # Get sheet name from environment or create new one
#         list_name = os.getenv("SCRAPER_LIST_NAME", "MVP_Tools_Data")
#         self.sheet_name = list_name.replace(" ", "_")
        
#         try:
#             spreadsheet = self.client.open_by_key(self.spreadsheet_id)
#             self.sheet = spreadsheet.worksheet(self.sheet_name)
#         except:
#             # Create new worksheet if it doesn't exist
#             spreadsheet = self.client.open_by_key(self.spreadsheet_id)
#             self.sheet = spreadsheet.add_worksheet(
#                 title=self.sheet_name, 
#                 rows="1000", 
#                 cols="20"
#             )
        
#         self.ist = pytz.timezone('Asia/Kolkata')
#         self.session_start = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
#         self.items_scraped = 0
        
#         # Get format columns from environment
#         self.format_columns = self.get_format_columns()
        
#         # Add headers if sheet is empty
#         self.initialize_headers()
    
#     def get_format_columns(self):
#         """Get format columns from environment variable"""
#         columns_str = os.getenv("SCRAPER_COLUMNS", "")
#         if columns_str:
#             columns = [col.strip() for col in columns_str.split(",") if col.strip()]
#             return columns
#         return ["Tool Name", "Description", "Pricing", "Features"]
    
#     def initialize_headers(self):
#         """Add headers based on format columns"""
#         try:
#             if not self.sheet.get('A1'):
#                 # Build header row: Timestamp + URL + Format Columns + Status
#                 headers = ["Scraped At", "Website URL"] + self.format_columns + ["Status"]
#                 self.sheet.append_row(headers)
                
#                 # Format header row
#                 end_col = chr(65 + len(headers) - 1)  # Convert to letter (A, B, C...)
#                 self.sheet.format(f'A1:{end_col}1', {
#                     "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.8},
#                     "textFormat": {
#                         "bold": True, 
#                         "foregroundColor": {"red": 1, "green": 1, "blue": 1}
#                     },
#                     "horizontalAlignment": "CENTER"
#                 })
#                 print(f"✅ Created headers: {headers}")
#         except Exception as e:
#             print(f"Could not add headers: {e}")

#     def process_item(self, item, spider):
#         """Write each scraped item to Google Sheets"""
        
#         timestamp = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
        
#         # Build row dynamically based on format columns
#         row = [
#             timestamp,
#             item.get("website_url", "N/A"),
#         ]
        
#         # Add data for each format column
#         for col in self.format_columns:
#             row.append(item.get(col, "N/A"))
        
#         # Add status at the end
#         row.append(item.get("scraping_status", "Success"))

#         # Debug logging
#         spider.logger.info("=" * 70)
#         spider.logger.info(f"Writing to Google Sheets:")
#         spider.logger.info(f"  URL: {item.get('website_url')}")
#         for col in self.format_columns[:3]:  # Log first 3 columns
#             value = item.get(col, "N/A")
#             spider.logger.info(f"  {col}: {str(value)[:50]}...")
#         spider.logger.info(f"  Status: {item.get('scraping_status')}")
#         spider.logger.info("=" * 70)

#         # Append the row
#         self.sheet.append_row(row, value_input_option='USER_ENTERED')
        
#         # Get the row number
#         row_number = len(self.sheet.get_all_values())
        
#         # Format based on status
#         try:
#             end_col = chr(65 + len(row) - 1)
            
#             if item.get("scraping_status") == "Success":
#                 # Light green for success
#                 self.sheet.format(f'A{row_number}:{end_col}{row_number}', {
#                     "backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}
#                 })
#             else:
#                 # Light red for failures
#                 self.sheet.format(f'A{row_number}:{end_col}{row_number}', {
#                     "backgroundColor": {"red": 1, "green": 0.9, "blue": 0.9}
#                 })
            
#             # Text wrapping for long text columns
#             for i, col in enumerate(self.format_columns, start=3):  # Start from column C
#                 col_letter = chr(65 + i - 1)
#                 self.sheet.format(f'{col_letter}{row_number}', {
#                     "wrapStrategy": "WRAP"
#                 })
#         except Exception as e:
#             spider.logger.warning(f"Could not format row: {e}")
        
#         self.items_scraped += 1
#         spider.logger.info(f"✅ Added to sheet (row {row_number})") 
#         return item
    
#     def close_spider(self, spider):
#         """Add separator when scraping completes"""
#         try:
#             session_end = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
#             session_date = datetime.now(self.ist).strftime("%Y-%m-%d")
            
#             separator_text = f"═══ SCRAPING COMPLETED on {session_date} ═══"
#             summary_text = f"Started: {self.session_start} | Ended: {session_end} | Items: {self.items_scraped}"
            
#             # Create row with enough cells
#             num_cols = 2 + len(self.format_columns) + 1
#             separator_row = [separator_text, summary_text] + [""] * (num_cols - 2)
            
#             self.sheet.append_row(separator_row, value_input_option='USER_ENTERED')
#             row_number = len(self.sheet.get_all_values())
            
#             # Format separator
#             end_col = chr(65 + num_cols - 1)
#             self.sheet.format(f'A{row_number}:{end_col}{row_number}', {
#                 "backgroundColor": {"red": 0.2, "green": 0.8, "blue": 0.2},
#                 "textFormat": {
#                     "bold": True,
#                     "foregroundColor": {"red": 1, "green": 1, "blue": 1},
#                     "fontSize": 11
#                 },
#                 "horizontalAlignment": "CENTER"
#             })
            
#             self.sheet.merge_cells(f'A{row_number}:{end_col}{row_number}')
            
#             spider.logger.info("=" * 80)
#             spider.logger.info(f"✅ Added separator row")
#             spider.logger.info(f"📊 Total items scraped: {self.items_scraped}")
#             spider.logger.info("=" * 80)
            
#         except Exception as e:
#             spider.logger.error(f"❌ Could not add separator: {e}")





