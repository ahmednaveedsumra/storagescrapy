import csv
import json
import pandas as pd
from .models import Session, Facility, Unit
from sqlalchemy.dialects.mysql import insert
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
        # Insert or update Facility
        facility_data = {
            'location': item.get('location', {}),
            'name': item.get('name', ''),
            'url': item.get('url', ''),
            'rating': item.get('rating', ''),
            'phone': item.get('phone', ''),
            'address': item.get('address', ''),
            'zipcode': item.get('zipcode', ''),
            'updated_at': func.now()
        }

        stmt = insert(Facility).values(facility_data)
        stmt = stmt.on_duplicate_key_update(**{col: stmt.inserted[col] for col in facility_data if col != 'id'})

        try:
            result = self.session.execute(stmt)
            self.session.commit()
            facility_id = result.lastrowid or self.session.query(Facility.id).filter_by(url=facility_data['url']).first()[0]
            logging.info(f"Facility {facility_data['url']} inserted or updated with ID: {facility_id}")

            # Process Unit data associated with the Facility
            unit_data = {
                'facility_id': facility_id,
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

            unit_stmt = insert(Unit).values(unit_data)
            unit_stmt = unit_stmt.on_duplicate_key_update(
                **{col: unit_stmt.inserted[col] for col in unit_data if col != 'id'} #add facility id here
            )

            self.session.execute(unit_stmt)
            self.session.commit()
            logging.info(f"Unit {unit_data['storage_type']} for Facility {facility_data['url']} inserted or updated.")

        except Exception as e:
            self.session.rollback()
            logging.error(f"Error processing item {item}: {e}")

        return item