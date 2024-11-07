import csv
from itemadapter import ItemAdapter

class StoragePipeline:
    def open_spider(self, spider):
        self.file = open('output.csv', 'w', newline='', encoding='utf-8')
        fieldnames = [
            'location', 'name', 'url', 'rating', 'phone', 'address', 'zipcode',
            'unit_name', 'storage_type', 'current_price', 'old_price', 'features',
            'availability', 'promotion'
        ]
        self.writer = csv.DictWriter(self.file, fieldnames=fieldnames)
        self.writer.writeheader()

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()

        # Combine state and city into one 'location' field
        location = f"{item_dict.get('location', {}).get('state', '')}, {item_dict.get('location', {}).get('city', '')}"

        row = {
            'location': location,
            'name': item_dict.get('name', ''),
            'url': item_dict.get('url', ''),
            'rating': item_dict.get('rating', ''),
            'phone': item_dict.get('phone', ''),
            'address': item_dict.get('address', ''),
            'zipcode': item_dict.get('zipcode', ''),
            'unit_name': item_dict.get('unit_name', ''),
            'storage_type': item_dict.get('storage_type', ''),
            'current_price': item_dict.get('current_price', ''),
            'old_price': item_dict.get('old_price', ''),
            'features': ', '.join(item_dict.get('features', [])),  # Convert list to a comma-separated string
            'availability': item_dict.get('availability', ''),
            'promotion': item_dict.get('promotion', '')
        }

        self.writer.writerow(row)
        return item


# import pandas as pd
# from itemadapter import ItemAdapter
#
#
# class StoragePipeline:
#     def open_spider(self, spider):
#         self.items = []
#
#     def close_spider(self, spider):
#         # Convert items to DataFrame and write to Parquet
#         df = pd.DataFrame(self.items)
#
#         # Ensure 'features' is a string, in case it's a list
#         if 'features' in df.columns:
#             df['features'] = df['features'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
#
#         # Write DataFrame to Parquet
#         df.to_parquet('output.parquet', engine='pyarrow', index=False)
#
#     def process_item(self, item, spider):
#         item_dict = ItemAdapter(item).asdict()
#
#         # Combine state and city into a single 'location' field
#         location = f"{item_dict.get('location', {}).get('state', '')}, {item_dict.get('location', {}).get('city', '')}"
#
#         # Update the item dictionary with the combined location
#         item_dict['location'] = location
#
#         # Append item data to list
#         self.items.append(item_dict)
#
#         return item


# import json
# from itemadapter import ItemAdapter
#
#
# class StoragePipeline:
#     def open_spider(self, spider):
#         # Open file for writing in utf-8 encoding and start the JSON array
#         self.file = open('output.json', 'w', encoding='utf-8')
#         self.file.write("[")
#
#     def close_spider(self, spider):
#         # Close the JSON array and the file
#         self.file.write("]")
#         self.file.close()
#
#     def process_item(self, item, spider):
#         item_dict = ItemAdapter(item).asdict()
#
#         # Write the item dictionary as a JSON object
#         if self.file.tell() > 1:  # Avoid adding a comma before the first item
#             self.file.write(",\n")
#
#         json.dump(item_dict, self.file, ensure_ascii=False)
#
#         return item
#
