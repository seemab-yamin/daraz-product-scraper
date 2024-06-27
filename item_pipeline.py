from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

PKR_TO_DOLLAR_RATE = 278.81


def process_price(price_text: str) -> float:
    """
    clean prices and convert to dollar
    """
    price = eval(price_text.strip("Rs.").replace(",", ""))

    return round(float(price / PKR_TO_DOLLAR_RATE), 2)


class ProductPipeline:
    def __init__(self):
        self.urls_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter["product_url"] in self.urls_seen:
            raise DropItem(f"Duplicate Url found: {item!r}")
        else:
            self.urls_seen.add(adapter["product_url"])

        # drop item if no url
        if not adapter.get("product_url"):
            raise DropItem(f"DEBUG# Missing product_url in {item}")

        # add https: if not exist
        if adapter.get("product_url"):
            if not adapter.get("product_url").startswith("https:"):
                adapter["product_url"] = "https:" + adapter["product_url"]

        # construct brand from title text
        if adapter.get("product_title"):
            adapter["product_brand"] = adapter["product_title"].split(" ")[0]

        # clean prices and convert to dollar
        if adapter.get("current_price"):
            adapter["current_price"] = process_price(adapter["current_price"])

        if adapter.get("original_price"):
            adapter["original_price"] = process_price(adapter["original_price"])

        return item
