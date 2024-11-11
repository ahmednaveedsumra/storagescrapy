import csv
import json
import pandas as pd
from .models import engine, Session, Facility, Unit, CSV_FILE_PATH, TABLE_NAME
from sqlalchemy.exc import IntegrityError
from datetime import date
from sqlalchemy import text


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
        is_last_item = getattr(spider, 'is_last_item', False)
        if is_last_item:
            json.dump(dict(item), self.file, ensure_ascii=False)
        else:
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
        session = self.session

        existing_facility = session.query(Facility).filter_by(url=item['url']).first()

        if not existing_facility:
            facility = Facility(
                location=item.get('location', {}),
                name=item.get('name', ''),
                url=item.get('url', ''),
                rating=item.get('rating', ''),
                phone=item.get('phone', ''),
                address=item.get('address', ''),
                zipcode=item.get('zipcode', ''),
            )
            session.add(facility)
            session.commit()
        else:
            facility = existing_facility

            for attr in [column.name for column in Facility.__table__.columns]:
                val = item.get(attr, None)
                if val is not None:
                    setattr(facility, attr, val)

        try:
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error while committing facility: {e}")
        unit = Unit(
            facility_id=facility.id,
            unit_name=item.get('unit_name', ''),
            storage_type=item.get('storage_type', ''),
            current_price=float(item.get('current_price', 0.0)),
            old_price=float(item.get('old_price', 0.0)),
            features=item.get('features', ''),
            availability=item.get('availability', ''),
            promotion=item.get('promotion', ''),
            unit_url=item.get('unit_url', ''),
            created_at_date=date.today()
        )

        session.add(unit)
        try:
            session.commit()
        except IntegrityError as e:
            session.rollback()
            print(f"Integrity error: {e}")
        except Exception as e:
            session.rollback()
            print(f"Unexpected error: {e}")

        return item


class CsvToMysqlPipeline:
    def __init__(self):
        self.engine = engine
        self.session = Session()
        self.csv_file = CSV_FILE_PATH
        self.table_name = TABLE_NAME

    def open_spider(self, spider):
        self.data = pd.read_csv(self.csv_file, low_memory=False)
        self.data = self.data.loc[:, ~self.data.columns.str.contains('^Unnamed')]

    def process_item(self, item, spider):
        df = pd.DataFrame(self.data)

        try:
            df.to_sql(self.table_name, con=self.engine, if_exists='fail', index=False, chunksize=5000)
            self._alter_table()
        except Exception as e:
            spider.logger.error(f"Failed to insert data: {e}")

        return item

    def _alter_table(self):
        with self.engine.connect() as connection:
            alter_table_query = text(f"""
                ALTER TABLE {self.table_name}
                ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST,
                ADD COLUMN created DATETIME DEFAULT CURRENT_TIMESTAMP;
            """)
            connection.execute(alter_table_query)

    def close_spider(self, spider):
        self.session.close()
        self.engine.dispose()