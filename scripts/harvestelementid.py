"""Purpose: To automate TCSI element id extraction for consumption in main script
Author: TW
"""
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import csv
import time
import random

class DataElement:
    def element_number(self):
        print("\nStart element id extraction")
        wait_min = 10  # Defines min random wait time(in seconds) before extraction
        wait_max = 30  # Defines max random wait time(in seconds) before extraction
        now = datetime.now()
        elementids = []
        base_url = "https://www.tcsisupport.gov.au/elements"
        current_url = "?contains=&sort_by=field_code_value&sort_order=ASC&page=0"

        while True:
            url = f"{base_url}{current_url}"
            response = requests.get(url)
            print(f" processing url: {url}")
            time.sleep(random.randint(wait_min, wait_max))  # Add random wait time before extraction
            webcontent = BeautifulSoup(response.content, "lxml")
            table = webcontent.find('table')
            for tr in table.find_all('tr')[1:]:
                td = tr.find_all('td')
                elementid_obj = {
                    'ELEMENT_ID': td[0].text,
                    'PAGE_ACCESS_TIMESTAMP': now
                }
                elementids.append(elementid_obj)
            # Handle pagination
            try:
                pg = webcontent.find('ul', 'pager__items js-pager__items au-link-list au-link-list--inline')
                active_pg = pg.find('li', 'pager__item is-active')
                next_url = active_pg.find_next_sibling('li').a.get('href')
                if next_url:
                    print(f" next url: {next_url}")
                    current_url = next_url
                else:
                    break 
            except Exception as e:
                print(f" next url does not exist: {e}")
                break
            
        # Export to a csv file for record keeping; csv is not consumed at this stage.
        with open(f'HEIMS_Element_TCSI_IDS_{now.date()}.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fields = ["ELEMENT_ID", "PAGE_ACCESS_TIMESTAMP"]
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            writer.writerows(elementids)

        print("End element id extraction")
        return elementids
