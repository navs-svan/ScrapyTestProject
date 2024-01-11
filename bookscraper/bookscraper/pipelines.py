# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
import os
import psycopg2

class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        ## Strip all whitespaces from strings
        fieldnames = adapter.field_names()
        for fieldname in fieldnames:
            if fieldname != 'description':
                value = adapter.get(fieldname)
                adapter[fieldname] = value.strip(' ')

        ## Category & Product Type --> switch to lowercase
        lowercase_keys = ('category', 'product_type')
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()        

        ## Price --> convert to float
        price_keys = ('price_excl_tax', 'price_incl_tax', 'tax', 'price')
        for price_key in price_keys:
            value = adapter.get(price_key).strip('£')
            adapter[price_key] = float(value)

        ## Availability --> extract number of books in stock
        value = adapter.get('availability')
        stock=[]
        for char in value:
            try:
                int(char)
                stock.append(char)
            except ValueError:
                continue    
        if len(stock) > 0:
            stock = ''.join(stock)
        else:
            stock = 0
        adapter['availability'] = stock

        ## Reviews --> convert string to number
        value = adapter.get('num_reviews')
        adapter['num_reviews'] = int(value)

        ## Stars --> convert text to number
        star_dict = {'Zero':2, 'One':1, 'Two':2, 'Three':3, 'Four':4, 'Five':5}
        value = adapter.get('stars')
        value = value.replace('star-rating ','')
        adapter['stars'] = star_dict[value]
        
        return item

class SaveToPostgresPipeline:

    def __init__(self):
        parent_directory = os.path.split(os.path.split(os.path.dirname(__file__))[0])[0]
        file_path = os.path.join(parent_directory, 'credentials.json')
        with open(file_path, 'r') as f:
            credentials = json.load(f)

        hostname=credentials["hostname"]
        database=credentials["database"]
        username=credentials["username"]
        password=credentials["password"]

        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        self.cur = self.connection.cursor()
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS books(
            id serial PRIMARY KEY, 
            url VARCHAR(255),
            title text,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            price DECIMAL,
            availability INTEGER,
            num_reviews INTEGER,
            stars INTEGER,
            category VARCHAR(255),
            description text
        )
        """)

    def process_item(self, item, spider):
        self.cur.execute(""" insert into books (
            url, 
            title, 
            upc, 
            product_type, 
            price_excl_tax,
            price_incl_tax,
            tax,
            price,
            availability,
            num_reviews,
            stars,
            category,
            description
            ) values (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
                )""", (
            item["url"],
            item["title"],
            item["upc"],
            item["product_type"],
            item["price_excl_tax"],
            item["price_incl_tax"],
            item["tax"],
            item["price"],
            item["availability"],
            item["num_reviews"],
            item["stars"],
            item["category"],
            str(item["description"])
        ))

        self.connection.commit()
        return item
    
    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()