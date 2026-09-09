import scrapy


class SmartFitItem(scrapy.Item):

    location_id = scrapy.Field()
    club_name = scrapy.Field()

    address_first_line = scrapy.Field()
    address_second_line = scrapy.Field()
    city_state = scrapy.Field()

    latitude = scrapy.Field()
    longitude = scrapy.Field()
    distance = scrapy.Field()

    prices = scrapy.Field()
    plan_names = scrapy.Field()
    schedules = scrapy.Field()

    facilities = scrapy.Field()
    features = scrapy.Field()
    activities = scrapy.Field()

    promotion = scrapy.Field()
    opened = scrapy.Field()
    franchise = scrapy.Field()
    sales_available = scrapy.Field()

    picture_url = scrapy.Field()
    permalink = scrapy.Field()

    source_url = scrapy.Field()
    scraped_at = scrapy.Field()