import scrapy

from datetime import datetime, timezone

from smartfit_scraper.items import SmartFitItem


class SmartFitSpider(scrapy.Spider):

    name = "smartfit"

    allowed_domains = [
        "smartfit.com.co"
    ]

    start_urls = [
        "https://www.smartfit.com.co/sedes.json?utf8=%E2%9C%93&name=&promotion_id=&page=1&promotion_only=&facilities=&activities=&features=&lat=33.5922&lng=-7.6184&except_digital=true"
    ]


    def parse(self, response):

        data = response.json()

        locations = data.get("locations", [])

        for location in locations:

            item = SmartFitItem()

            address = location.get("address", {})
            position = address.get("position", {})

            item["location_id"] = location.get("id")
            item["club_name"] = location.get("name")

            item["address_first_line"] = address.get("first_line")
            item["address_second_line"] = address.get("second_line")
            item["city_state"] = address.get("third_line")

            item["latitude"] = position.get("latitude")
            item["longitude"] = position.get("longitude")

            item["distance"] = location.get("distance")

            item["prices"] = location.get("prices", {})
            item["plan_names"] = location.get("plan_names", [])

            item["schedules"] = location.get("schedules", {})
            item["facilities"] = location.get("facilities", [])
            item["features"] = location.get("features", [])
            item["activities"] = location.get("activities", [])

            item["promotion"] = location.get("promotion")

            item["opened"] = location.get("opened")
            item["franchise"] = location.get("franchise")
            item["sales_available"] = location.get("sales_available")

            item["picture_url"] = location.get("picture_url")
            item["permalink"] = location.get("permalink")

            item["source_url"] = response.url

            item["scraped_at"] = datetime.now(
                timezone.utc
            ).isoformat()

            yield item