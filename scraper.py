import datetime
import os
import sys

import scrapy
from scrapy import Selector
from scrapy.crawler import CrawlerProcess

# output dir to store scraped data and logs
OUTPUT_DIR = "output_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def should_abort_request(request):
    """
    Will abort images requests to reduce bandwidth cost
    """
    return (
        request.resource_type == "image"
        or ".jpg" in request.url
        or request.resource_type == "css"
        or ".css" in request.url
    )


class DarazSpider(scrapy.Spider):
    name = "daraz_spider"

    def __init__(self, *args, **kwargs):
        super(DarazSpider, self).__init__(*args, **kwargs)
        self.start_urls = kwargs.get("start_urls")

    custom_settings = dict(
        ROBOTSTXT_OBEY=True,
        DOWNLOAD_DELAY=0.5,
        RETRY_TIMES=10,
        # Specify UTF-8 encoding for CSV output
        FEED_EXPORT_ENCODING="utf-8",
        # export as CSV format
        FEED_FORMAT="csv",
        FEED_URI=f"{OUTPUT_DIR}/products.csv",
        CONCURRENT_REQUESTS=1,
        PLAYWRIGHT_ABORT_REQUEST=should_abort_request,
        PLAYWRIGHT_BROWSER_TYPE="chromium",
        PLAYWRIGHT_LAUNCH_OPTIONS={
            "headless": True,
            "timeout": 40 * 1000,  # 20 seconds
        },
        TWISTED_REACTOR="twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        DOWNLOAD_HANDLERS={
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        ITEM_PIPELINES={
            "item_pipeline.ProductPipeline": 300,
        },
        # log setting
        LOG_ENCODING="utf-8",
        LOG_LEVEL="INFO",
        LOG_FILE=f"{OUTPUT_DIR}/info.log",
    )

    def start_requests(self):
        # GET request
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta=dict(
                    playwright=True,
                    playwright_include_page=True,
                    playwright_page_goto_kwargs={
                        "wait_until": "networkidle",
                    },
                ),
                errback=self.errback,
            )

    async def parse(self, response):
        page = response.meta["playwright_page"]

        while True:
            # get next page element
            next_page = await page.query_selector("li[title='Next Page']")
            pagination_url = page.url

            # current pagination url
            print(f"LAST URL:\t{pagination_url}")

            # fetch products
            products = await page.query_selector_all('div[data-spm="sku"] > div')
            print(f"total products:\t{len(products)}")
            for product in products:
                product = await product.as_element().inner_html()

                product = Selector(text=product)
                product_title = product.css("div[id='id-title']::text").get()
                product_url = product.css("a::attr(href)").get()
                product_img = product.css("img[id='id-img']::attr(src)").get()
                current_price = product.css(
                    "div[style='font-size: 18px;'] span:nth-of-type(2)::text"
                ).get()

                original_price = product.css(
                    "div[style='font-size: 10px;'] > del::text"
                ).get()

                is_free_delivery = bool(
                    product.css("div.free-delivery--OD68c::text").get()
                )

                # Yield the scraped data
                yield {
                    "product_title": product_title,
                    "product_url": product_url,
                    "product_img": product_img,
                    "current_price": current_price,
                    "original_price": original_price,
                    "is_free_delivery": is_free_delivery,
                    "pagination_url": pagination_url,
                }

            try:
                print("Checking Next Page.")
                if await next_page.get_attribute("aria-disabled") == "true":
                    print("Next Page Element Disabled. Quitting...")

                    current_timestamp = datetime.datetime.now().timestamp()
                    await page.screenshot(
                        path=os.path.join(
                            OUTPUT_DIR, f"screenshot_{current_timestamp}.png"
                        ),
                        full_page=True,
                    )
                    # break on last page
                    break

                await next_page.click()
                await page.wait_for_timeout(500)
                # await next_page.waitForLoadState()
                await page.wait_for_load_state()
            except Exception as err:
                if "ElementHandle.click: Element is not attached to the DOM" in str(
                    err
                ):
                    print("Next Page Not Found. Quitting...")
                    # break on error page
                    break
                else:
                    raise Exception(f"Unknown Error:\t{err}")

        await page.close()

    async def errback(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()


if __name__ == "__main__":
    try:
        # Validate if category URL from the first argument is a string type
        if not isinstance(sys.argv[1], str):
            raise TypeError("Category URL argument must be a string.")
        category_url = sys.argv[1]
        print(f"Category Url:\t{category_url}")
    except (IndexError, TypeError) as e:
        print(f"Error: {e}")
        print(
            "Please provide a valid category URL as an argument. "
            "For example: python3 scraper.py https://www.daraz.pk/washers-dryers"
        )

        print("Exiting...")
        exit()

    crawler = CrawlerProcess()
    crawler.crawl(DarazSpider, start_urls=[category_url])
    crawler.start()
