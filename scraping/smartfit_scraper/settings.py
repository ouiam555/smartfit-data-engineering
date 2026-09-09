BOT_NAME = "smartfit_scraper"

SPIDER_MODULES = [
    "smartfit_scraper.spiders"
]

NEWSPIDER_MODULE = "smartfit_scraper.spiders"

ROBOTSTXT_OBEY = True

CONCURRENT_REQUESTS_PER_DOMAIN = 1

DOWNLOAD_DELAY = 1

FEED_EXPORT_ENCODING = "utf-8"

ITEM_PIPELINES = {
    "smartfit_scraper.pipelines.KafkaPipeline": 300,
}
