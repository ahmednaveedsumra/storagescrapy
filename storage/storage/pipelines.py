import csv
import json
import pandas as pd
from .models import Session, Facility
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import logging


class CSVStoragePipeline:
    def open_spider(self, spider):
        self.file = open('output.csv', 'w', newline='', encoding='utf-8')
        fieldnames = [
            'location', 'name', 'url', 'rating', 'phone', 'address', 'zipcode',
            'unit_name', 'storage_type', 'current_price', 'old_price', 'features',
            'availability', 'promotion' , 'unit_url'
        ]
        self.writer = csv.DictWriter(self.file, fieldnames=fieldnames)
        self.writer.writeheader()

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        self.writer.writerow(dict(item))
        return item


class JSONStoragePipeline:
    def open_spider(self, spider):
        self.file = open('output.json', 'w', encoding='utf-8')
        self.file.write('[')

    def close_spider(self, spider):
        self.file.write(']')
        self.file.close()

    def process_item(self, item, spider):
        json.dump(dict(item), self.file, ensure_ascii=False)
        self.file.write(',\n')
        return item


class ParquetStoragePipeline:
    def __init__(self):
        self.items = []

    def process_item(self, item, spider):

        self.items.append(dict(item))
        return item

    def close_spider(self, spider):
        if self.items:

            df = pd.DataFrame(self.items)
            df.to_parquet('output.parquet', engine='pyarrow', index=False)


class SQLAlchemyPipeline:
    def open_spider(self, spider):
        self.session = Session()
        logging.info("Database session opened for spider.")

    def close_spider(self, spider):
        self.session.close()
        logging.info("Database session closed for spider.")

    def process_item(self, item, spider):
        try:
            data = {
                'location': item.get('location', {}),
                'name': item.get('name', ''),
                'url': item.get('url', ''),
                'rating': item.get('rating', ''),
                'phone': item.get('phone', ''),
                'address': item.get('address', ''),
                'zipcode': item.get('zipcode', ''),
                'unit_name': item.get('unit_name', ''),
                'storage_type': item.get('storage_type', ''),
                'current_price': float(item.get('current_price', 0.0)),
                'old_price': float(item.get('old_price', 0.0)),
                'features': item.get('features', ''),
                'availability': item.get('availability', ''),
                'promotion': item.get('promotion', ''),
                'unit_url': item.get('unit_url', ''),
                'updated_at': func.now()
            }
        except (TypeError, ValueError) as e:
            logging.error(f"Data type error in item {item}: {e}")
            return item

        stmt = insert(Facility).values(data)
        stmt = stmt.on_duplicate_key_update(
            **{col: stmt.inserted[col] for col in data if col != 'id'}
        )

        try:
            self.session.execute(stmt)
            self.session.commit()
            logging.info(f"Record for {data['unit_url']} inserted or updated.")
        except Exception as e:
            self.session.rollback()
            logging.error(f"Error occurred while processing item {data['unit_url']}: {e}")

        return item