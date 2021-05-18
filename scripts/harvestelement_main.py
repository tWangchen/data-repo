"""Purpose: To automate TCSI open web data harvesting due to lack of standard api
Author: TW
"""
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import csv
import time
import random
import re

from harvestelementid import DataElement

def main():
    wait_min = 10  # Defines min random wait time(in seconds) before extraction
    wait_max = 40  # Defines max random wait time(in seconds) before extraction
    base_url = "https://www.tcsisupport.gov.au/element/"
    now = datetime.now()
    element = []
    
    page_harvestor = input("Hi there, I am a data harvester, please tell me your name to proceed: ")
    logger = (f"Hi {page_harvestor}, recorded below are your logs from today {now.date()}\n")
    if page_harvestor:
        # Start element_num for loop
        for elementid in DataElement().element_number():
            element_num = elementid['ELEMENT_ID'].strip()
            url = f"{base_url}{element_num}"
            response = requests.get(url)
            if response.status_code == 200:
                try:
                    time.sleep(random.randint(wait_min, wait_max))  # Add random wait time before extraction
                    webcontent = BeautifulSoup(response.content, "lxml")
                    # Start extracting webcontent.
                    # Important note: if the output is unexpected, likely the webcontent have changed,
                    # in which case try updating the target definition(s) below.
                    title = webcontent.find('h1').text
                    description = webcontent.find('div', class_='field field--name-field-description field--type-string-long field--label-hidden field__item').text
                    code_category = webcontent.find('div', class_='field field--name-field-code-category field--type-entity-reference field--label-inline').find('div', class_='field__item').text
                    element_no = webcontent.find('div', class_='field field--name-field-code field--type-integer field--label-inline').find('div', class_='field__item').text
                    element_type = webcontent.find('div', class_='block block-ctools-block block-entity-fieldnodefield-element-type').find('div', class_='field__item').text
                    width = webcontent.find('div', class_='block block-ctools-block block-entity-fieldnodefield-data-size').find('div', class_='field__item').text
                    ver_rev_date = webcontent.find('div', class_='block block-ctools-block block-entity-fieldnodefield-version-revision-date').find('div', class_='field__item').text
                    version = webcontent.find('div', class_='block block-ctools-block block-entity-fieldnodefield-version').find('div', class_='field__item').text
                    yrs_ver_active = webcontent.find('div', class_='block block-ctools-block block-entity-fieldnodefield-years').find('div', class_='field__item').text
                    retired = webcontent.find('div', class_='validity-699')
                    tables = webcontent.find_all('table')
                    # End extracting webcontent
                  
                    # Start core data requirements logic
                    # Retired condition may be redundant as Data Element Dictionaly table(https://www.tcsisupport.gov.au/elements) appears to only have active elements; 
                    # need further review of elements.
                    if retired:  # Process retired elements
                        if tables:
                            for table in tables:
                                table_title = table.find_previous(re.compile("^h")).text
                                for tr in table.find_all('tr')[1:]:
                                    td = tr.find_all('td')
                                    td_length = len(td)
                                    print(f"\nProcessing HEIMS_E{element_num}: current table have {td_length} table columns; if table columns are more than 3, please investigate further.")
                                    logger += (f"\nProcessing HEIMS_E{element_num}: current table have {td_length} table columns; if table columns are more than 3, please investigate further.")
                                    if td_length == 3:
                                        spec = DataRequired(url, title, now, page_harvestor, description, code_category, element_no,\
                                            element_type, width, ver_rev_date, version, yrs_ver_active, table_title, td[0].text, td[1].text, td[2].text, "Y", "N"
                                        )
                                        element.append(spec)
                                    else:  # Intentionally left without condition, so everything outside 3 columns will at least have two columns processed
                                        spec = DataRequired(url, title, now, page_harvestor, description, code_category, element_no,\
                                            element_type, width, ver_rev_date, version, yrs_ver_active, table_title, td[0].text, td[1].text, "", "Y", "N"
                                        )
                                        element.append(spec)
                        else:
                            nospec = DataRequired(url, title, now, page_harvestor, description, code_category, element_no,\
                                element_type, width, ver_rev_date, version, yrs_ver_active, "", "", "", "", "N", "N"
                            )
                            element.append(nospec)
                        print(f"\nProcessed HEIMS_E{element_num}")
                        logger += (f"\nProcessed HEIMS_E{element_num}")
                    else:  # Process active(non-retired) elements
                        if tables:
                            for table in tables:
                                table_title = table.find_previous(re.compile("^h")).text
                                for tr in table.find_all('tr')[1:]:
                                    td = tr.find_all('td')
                                    td_length = len(td)
                                    print(f"\nProcessing HEIMS_E{element_num}: current table have {td_length} table columns; if table columns are more than 3, please investigate further.")
                                    logger += (f"\nProcessing HEIMS_E{element_num}: current table have {td_length} table columns; if table columns are more than 3, please investigate further.")
                                    if td_length == 3:
                                        spec = DataRequired(url, title, now, page_harvestor, description, code_category, element_no,\
                                            element_type, width, ver_rev_date, version, yrs_ver_active, table_title, td[0].text, td[1].text, td[2].text, "Y", "N"
                                        )
                                        element.append(spec)
                                    else:  # Intentionally left without condition, so everything outside 3 columns will at least have two columns processed
                                        spec = DataRequired(url, title, now, page_harvestor, description, code_category, element_no,\
                                            element_type, width, ver_rev_date, version, yrs_ver_active, table_title, td[0].text, td[1].text, "", "Y", "N"
                                        )
                                        element.append(spec)
                        else:
                            nospec = DataRequired(url, title, now, page_harvestor, description, code_category, element_no,\
                                element_type, width, ver_rev_date, version, yrs_ver_active, "", "", "", "", "N", "N"
                            )
                            element.append(nospec)
                        print(f"\nProcessed HEIMS_E{element_num}")
                        logger += (f"\nProcessed HEIMS_E{element_num}")
                    # End core data requirements logic
                except Exception as error_msg:
                    print(f"\n\nError processing HEIMS_E{element_num}: {error_msg}\n")
                    logger += (f"\n\nError processing HEIMS_E{element_num}: {error_msg}\n")
            else:
                print(f"\n\nURL: {url}\n Response code: {response.status_code}\n")
                logger += (f"\n\nURL:{url}\n Response code: {response.status_code}\n")
        # End element_num for loop

        with open(f'HEIMS_Element_TCSI_{now.date()}.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["PAGE_URL", "PAGE_TITLE", "PAGE_ACCESS_TIMESTAMP", "PAGE_HARVESTOR", "DESCRIPTION", "CODE_CATEGORY",\
                "ELEMENT_NO",  "ELEMENT_TYPE", "WIDTH", "VERSION_REVISION_DATE", "VERSION", "YEARS_VERSION_ACTIVE",\
                "SUB_HEADER", "VALUE", "MEANING", "DERIVATION", "SPEC_FLAG", "RETIRED"
            ])
            for row in element:
                writer.writerow([row.page_url, row.page_title, row.page_access_timestamp, row.page_harvestor, row.description,\
                    row.code_category, row.element_no, row.element_type, row.width, row.version_revision_date, row.version,\
                    row.years_version_active, row.sub_header, row.value, row.meaning, row.derivation, row.spec_flag, row.retired
                ])
        print(f"\n\nDone.")
        logger += (f"\n\nDone.")

        # This will write to a log file
        f = open(f"tcsiharvest_{now.date()}.log", 'a')
        f.write(f"{logger}")
        f.close()
    else:
        print(f"\nUm... sorry, I did not get your name. \n Try rerunning the code; don't forget to include your name this time.\n")

# Reusable helpers
class DataRequired:  # Helps define data requirements 
    def __init__(self, url, title, now, page_harvestor, description, code_category, element_no, element_type, width, ver_rev_date,\
        version, yrs_ver_active, sub_header, value, meaning, derivation, spec_flag, retired):
        self.page_url = url
        self.page_title = title
        self.page_access_timestamp = now
        self.page_harvestor = page_harvestor
        self.description = description
        self.code_category = code_category
        self.element_no = element_no
        self.element_type = element_type
        self.width = width
        self.version_revision_date = ver_rev_date
        self.version = version
        self.years_version_active = yrs_ver_active
        self.sub_header = sub_header
        self.value = value
        self.meaning = meaning
        self.derivation = derivation
        self.spec_flag = spec_flag
        self.retired = retired

if __name__ == '__main__':
    main()
