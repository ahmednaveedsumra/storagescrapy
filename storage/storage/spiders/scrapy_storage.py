import scrapy
import logging
import json
import re


class NSASpider(scrapy.Spider):
    name = "nsaspider"
    start_urls = [
        'https://www.nsastorage.com/'
    ]

    def parse(self, response):
        state_blocks = response.css('div.block_list_1.three_across.form > div.item')

        for state_block in state_blocks:
            state_name = state_block.css('div.opener::text').get().strip()

            for link in state_block.css('div.slide div.block_list a.link-item'):
                city_name = link.css('::text').get().strip()
                city_url = response.urljoin(link.css('::attr(href)').get())

                location = {'state': state_name, 'city': city_name}

                yield response.follow(city_url, self.parse_city, meta={'location': location})

    def parse_city(self, response):
        location = response.meta['location']

        facilities = response.css('div#facilitiesBlock div.item')

        for facility in facilities:
            facility_data = {
                'location': location,
                'name': facility.css('a.part_title_1::text').get(default="").strip(),
                'url': response.urljoin(facility.css('a.part_title_1::attr(href)').get()),
                'rating': facility.css('div.part-reviews-num::text').get(default="").strip(),
                'phone': facility.css('a.block_location_1::attr(href)').re_first(r'tel:(\d+)'),
                'address': ' '.join(facility.css('address.block_location_1 *::text').getall()).strip(),
            }

            full_address = facility_data['address']
            facility_data['zipcode'] = re.search(r'\b\d{5}\b', full_address).group() if re.search(r'\b\d{5}\b',
                                                                                                  full_address) else ""

            facility_id = facility_data['url'].split('-')[-1]
            api_url = f'https://www.nsastorage.com/facility-units/{facility_id}'

            yield scrapy.Request(api_url, callback=self.parse_facility_units, meta={'facility_data': facility_data})

    def parse_facility_units(self, response):
        facility_data = response.meta['facility_data']
        data = json.loads(response.text)
        units_html = data.get("data", {}).get("html", {}).get("units", "")

        units_selector = scrapy.Selector(text=units_html)
        units = units_selector.css('div.unit-select-item')

        if not units:
            logging.warning("No units found in API response for: %s", response.url)

        for unit in units:

            relative_url = unit.css('a.form-opener::attr(href)').get(default="")
            facility_data['unit_url'] = f"https://www.nsastorage.com{relative_url}"

            facility_data['unit_name'] = unit.css('div.unit-select-item-detail-heading::text').get(default="").strip()
            facility_data['storage_type'] = unit.css('p.det::text').get(default="").strip()
            facility_data['current_price'] = re.sub(r'[^\d]', '', unit.css('div.part_item_price::text').get(default="").strip())
            facility_data['old_price'] = unit.css('div.part_item_old_price span.stroke::text').get(default="").strip()
            facility_data['features'] = str([feature.css('span::text').get(default="").strip() for feature in unit.css('ul.det-listing li')])
            facility_data['availability'] = unit.css('span.items-left::text').get(default="").strip()
            facility_data['promotion'] = unit.css('div.part_badge span::text').get(default="").strip()

            yield facility_data
