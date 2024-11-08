import csv
import json
import pandas as pd
from .models import Session, Facility
from datetime import datetime

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

    def close_spider(self, spider):
        self.session.close()

    def process_item(self, item, spider):
        facility = Facility(

            location=item.get('location', {}),
            name=item.get('name', ''),
            url=item.get('url', ''),
            rating=item.get('rating', ''),
            phone=item.get('phone', ''),
            address=item.get('address', ''),
            zipcode=item.get('zipcode', ''),
            unit_name=item.get('unit_name', ''),
            storage_type=item.get('storage_type', ''),
            current_price=float(item.get('current_price', 0)),
            old_price=float(item.get('old_price', 0)),
            features=item.get('features', ''),
            availability=item.get('availability', ''),
            promotion=item.get('promotion', ''),
            unit_url = item.get('unit_url', '')
        )

        self.session.add(facility)
        self.session.commit()

        return item
