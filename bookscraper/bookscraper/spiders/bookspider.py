import scrapy
from bookscraper.items import BookItem

class BookspiderSpider(scrapy.Spider):
    # what to call "scrapy crawl bookspider"
    name = "bookspider"
    # domains to scrape, to prevent scraping the whole internet
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com"]

    def parse(self, response):
        # parses the html content
        books = response.css("article.product_pod")

        for book in books:
            book_page = book.css("h3 a::attr(href)").get()
            book_page = book_page.replace("catalogue/", "")
            book_page_url = "https://books.toscrape.com/catalogue/" + book_page 

            yield response.follow(book_page_url, callback=self.parse_book_page)

        next_page = response.css("li.next a::attr(href)").get()
        if next_page is not None:
            next_page = next_page.replace("catalogue/", "")
            next_page_url = "https://books.toscrape.com/catalogue/" + next_page
            # go to this next page url using response.follow
            # callback will be executed once the response is received
            yield response.follow(next_page_url, callback=self.parse)

    def parse_book_page(self, response):
        table = response.css("table.table-striped tr")
        book_item = BookItem()
            
        book_item["url"] = response.url
        book_item["title"] = response.css("div.product_main h1::text").get()
        book_item["upc"] = table[0].css("td ::text").get()
        book_item["product_type"] = table[1].css("td ::text").get()
        book_item["price_excl_tax"] = table[2].css("td ::text").get()
        book_item["price_incl_tax"] = table[3].css("td ::text").get()
        book_item["tax"] = table[4].css("td ::text").get()
        book_item["availability"] = table[5].css("td ::text").get()
        book_item["num_reviews"] = table[6].css("td ::text").get()
        book_item["stars"] = response.css("p.star-rating::attr(class)").get()
        book_item["category"] = response.xpath("//li[@class='active']/preceding-sibling::li[1]/a/text()").get()
        book_item["description"] = response.xpath("//div[@id='product_description']/following-sibling::p/text()").get()
        book_item["price"] = response.css("p.price_color::text").get()

        yield book_item