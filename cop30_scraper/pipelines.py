import gspread
from google.oauth2.service_account import Credentials
import os

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

    def process_item(self, item, spider):
        """
        Write item to Google Sheets with 5 columns only:
        A: Scheduled
        B: Time_Location
        C: Organizer
        D: Tags
        E: Title_Theme_Speakers (combined formatted text)
        """
        
        # Extract the combined Title/Theme/Speakers data
        tts_data = item.get("Title_Theme_Speakers", {})
        title = tts_data.get("title", "N/A")
        theme = tts_data.get("theme", "No description found.")
        speakers = tts_data.get("speakers", "N/A")
        
        # Format as combined text with line breaks for column E
        formatted_tts = f"Title: {title}\n\nTheme: {theme}\n\nSpeakers: {speakers}"

        # Build row with ONLY 5 columns (no empty column A)
        row = [
            item.get("Scheduled", "N/A"),        # Column A - Date
            item.get("Time_Location", "N/A"),    # Column B - Time & Location
            item.get("Organizer", "N/A"),        # Column C - URL
            item.get("Tags", "N/A"),             # Column D - Tags
            formatted_tts,                       # Column E - Title/Theme/Speakers
        ]

        # Debug logging
        spider.logger.info("=" * 70)
        spider.logger.info(f"Writing to Google Sheets:")
        spider.logger.info(f"  Column A (Scheduled): '{row[0]}'")
        spider.logger.info(f"  Column B (Time/Location): '{row[1][:50]}...'")
        spider.logger.info(f"  Column C (Organizer): '{row[2][:50]}...'")
        spider.logger.info(f"  Column D (Tags): '{row[3]}'")
        spider.logger.info(f"  Column E (Title): '{title[:50]}...'")
        spider.logger.info("=" * 70)

        # Append the row
        self.sheet.append_row(row, value_input_option='USER_ENTERED')
        
        spider.logger.info(f"✅ Row added successfully for: {title[:50]}...") 
        return item



