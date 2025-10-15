import scrapy

class Cop30ScraperItem(scrapy.Item):
    # New fields based on the provided spreadsheet
    Scheduled = scrapy.Field()
    Time_Location = scrapy.Field()
    Organizer = scrapy.Field()  # This will now hold the source URL as per the sheet's format
    Tags = scrapy.Field()
    Title_Theme_Speakers = scrapy.Field()


