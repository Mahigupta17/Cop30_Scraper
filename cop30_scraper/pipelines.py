import gspread
from google.oauth2.service_account import Credentials
import os
from datetime import datetime
import pytz

class GoogleSheetsPipeline:
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
        
        self.spreadsheet_id = "1tDFA7DIRm0b-9mbZby2lNyfgYJtGXSjOwl5ezN3CeME"
        self.sheet_name = "Sheet3"

        self.sheet = self.client.open_by_key(self.spreadsheet_id).worksheet(self.sheet_name)
        
        # Set timezone to Indian Standard Time
        self.ist = pytz.timezone('Asia/Kolkata')
        
        # Track when scraping session started (in IST)
        self.session_start = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
        self.items_scraped = 0
        
        # Add headers if sheet is empty
        try:
            if not self.sheet.get('A1:F1'):
                headers = ["Scraped At", "Scheduled", "Time/Location", "Organizer", "Tags", "Title/Theme/Speakers"]
                self.sheet.append_row(headers)
                # Format header row
                self.sheet.format('A1:F1', {
                    "backgroundColor": {"red": 0.4, "green": 0.5, "blue": 0.9},
                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                    "horizontalAlignment": "CENTER"
                })
        except Exception as e:
            print(f"Could not add headers: {e}")

    def process_item(self, item, spider):
        """
        Write item to Google Sheets with 6 columns:
        A: Scraped At (timestamp in IST)
        B: Scheduled
        C: Time_Location
        D: Organizer
        E: Tags
        F: Title_Theme_Speakers (combined formatted text)
        """
        
        # Get current timestamp in IST
        timestamp = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
        
        # Extract the combined Title/Theme/Speakers data
        tts_data = item.get("Title_Theme_Speakers", {})
        title = tts_data.get("title", "N/A")
        theme = tts_data.get("theme", "No description found.")
        speakers = tts_data.get("speakers", "N/A")
        
        # Format as combined text with line breaks for column F
        formatted_tts = f"Title: {title}\n\nTheme: {theme}\n\nSpeakers: {speakers}"

        # Build row with 6 columns
        row = [
            timestamp,                           # Column A - Scraped At (IST)
            item.get("Scheduled", "N/A"),        # Column B - Date
            item.get("Time_Location", "N/A"),    # Column C - Time & Location
            item.get("Organizer", "N/A"),        # Column D - URL
            item.get("Tags", "N/A"),             # Column E - Tags
            formatted_tts,                       # Column F - Title/Theme/Speakers
        ]

        # Debug logging
        spider.logger.info("=" * 70)
        spider.logger.info(f"Writing to Google Sheets:")
        spider.logger.info(f"  Column A (Scraped At IST): '{timestamp}'")
        spider.logger.info(f"  Column B (Scheduled): '{row[1]}'")
        spider.logger.info(f"  Column C (Time/Location): '{row[2][:50]}...'")
        spider.logger.info(f"  Column D (Organizer): '{row[3][:50]}...'")
        spider.logger.info(f"  Column E (Tags): '{row[4]}'")
        spider.logger.info(f"  Column F (Title): '{title[:50]}...'")
        spider.logger.info("=" * 70)

        # Append the row
        self.sheet.append_row(row, value_input_option='USER_ENTERED')
        
        # Get the row number that was just added
        row_number = len(self.sheet.get_all_values())
        
        # Set text wrapping for the Title/Theme/Speakers column (NO GREEN BACKGROUND)
        try:
            self.sheet.format(f'F{row_number}', {
                "wrapStrategy": "WRAP"
            })
        except Exception as e:
            spider.logger.warning(f"Could not format row: {e}")
        
        self.items_scraped += 1
        spider.logger.info(f"✅ Row added successfully at row {row_number} for: {title[:50]}...") 
        return item
    
    def close_spider(self, spider):
        """
        Called when spider closes - add a separator row to mark end of scraping session
        """
        try:
            # Get current time in IST
            session_end = datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
            session_date = datetime.now(self.ist).strftime("%Y-%m-%d")
            
            # Create separator message
            separator_text = f"═══ SCRAPING SESSION COMPLETED on {session_date} ═══"
            summary_text = f"Started: {self.session_start} | Ended: {session_end} | Events Found: {self.items_scraped}"
            
            separator_row = [
                separator_text,
                summary_text,
                "",
                "",
                "",
                ""
            ]
            
            self.sheet.append_row(separator_row, value_input_option='USER_ENTERED')
            
            # Get the separator row number
            row_number = len(self.sheet.get_all_values())
            
            # Format the separator row with bright green background and bold white text
            self.sheet.format(f'A{row_number}:F{row_number}', {
                "backgroundColor": {"red": 0.2, "green": 0.8, "blue": 0.2},
                "textFormat": {
                    "bold": True,
                    "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                    "fontSize": 11
                },
                "horizontalAlignment": "CENTER"
            })
            
            # Merge cells A to F for the separator
            self.sheet.merge_cells(f'A{row_number}:F{row_number}')
            
            spider.logger.info("=" * 80)
            spider.logger.info(f"✅ Added separator row at row {row_number}")
            spider.logger.info(f"📊 Scraping session complete: {self.items_scraped} events added")
            spider.logger.info(f"🕐 Session ended at: {session_end} IST")
            spider.logger.info("=" * 80)
            
        except Exception as e:
            spider.logger.error(f"❌ Could not add separator row: {e}")



# import gspread
# from google.oauth2.service_account import Credentials
# import os
# from datetime import datetime

# class GoogleSheetsPipeline:
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
        
#         self.spreadsheet_id = "1tDFA7DIRm0b-9mbZby2lNyfgYJtGXSjOwl5ezN3CeME"
#         self.sheet_name = "Sheet3"

#         self.sheet = self.client.open_by_key(self.spreadsheet_id).worksheet(self.sheet_name)
        
#         # Add headers if sheet is empty
#         try:
#             if not self.sheet.get('A1:F1'):
#                 headers = ["Scraped At", "Scheduled", "Time/Location", "Organizer", "Tags", "Title/Theme/Speakers"]
#                 self.sheet.append_row(headers)
#                 # Format header row (optional but looks nice)
#                 self.sheet.format('A1:F1', {
#                     "backgroundColor": {"red": 0.4, "green": 0.5, "blue": 0.9},
#                     "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
#                     "horizontalAlignment": "CENTER"
#                 })
#         except Exception as e:
#             print(f"Could not add headers: {e}")

#     def process_item(self, item, spider):
#         """
#         Write item to Google Sheets with 6 columns:
#         A: Scraped At (timestamp)
#         B: Scheduled
#         C: Time_Location
#         D: Organizer
#         E: Tags
#         F: Title_Theme_Speakers (combined formatted text)
#         """
        
#         # Get current timestamp
#         timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
#         # Extract the combined Title/Theme/Speakers data
#         tts_data = item.get("Title_Theme_Speakers", {})
#         title = tts_data.get("title", "N/A")
#         theme = tts_data.get("theme", "No description found.")
#         speakers = tts_data.get("speakers", "N/A")
        
#         # Format as combined text with line breaks for column F
#         formatted_tts = f"Title: {title}\n\nTheme: {theme}\n\nSpeakers: {speakers}"

#         # Build row with 6 columns (timestamp in A, rest shifted right)
#         row = [
#             timestamp,                           # Column A - Scraped At
#             item.get("Scheduled", "N/A"),        # Column B - Date
#             item.get("Time_Location", "N/A"),    # Column C - Time & Location
#             item.get("Organizer", "N/A"),        # Column D - URL
#             item.get("Tags", "N/A"),             # Column E - Tags
#             formatted_tts,                       # Column F - Title/Theme/Speakers
#         ]

#         # Debug logging
#         spider.logger.info("=" * 70)
#         spider.logger.info(f"Writing to Google Sheets:")
#         spider.logger.info(f"  Column A (Scraped At): '{timestamp}'")
#         spider.logger.info(f"  Column B (Scheduled): '{row[1]}'")
#         spider.logger.info(f"  Column C (Time/Location): '{row[2][:50]}...'")
#         spider.logger.info(f"  Column D (Organizer): '{row[3][:50]}...'")
#         spider.logger.info(f"  Column E (Tags): '{row[4]}'")
#         spider.logger.info(f"  Column F (Title): '{title[:50]}...'")
#         spider.logger.info("=" * 70)

#         # Append the row
#         self.sheet.append_row(row, value_input_option='USER_ENTERED')
        
#         # Get the row number that was just added
#         row_number = len(self.sheet.get_all_values())
        
#         # Highlight the new row in light green
#         try:
#             self.sheet.format(f'A{row_number}:F{row_number}', {
#                 "backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}
#             })
            
#             # Set text wrapping for the Title/Theme/Speakers column
#             self.sheet.format(f'F{row_number}', {
#                 "wrapStrategy": "WRAP"
#             })
#         except Exception as e:
#             spider.logger.warning(f"Could not format row: {e}")
        
#         spider.logger.info(f"✅ Row added successfully at row {row_number} for: {title[:50]}...") 
#         return item



