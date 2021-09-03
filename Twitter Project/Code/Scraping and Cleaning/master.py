"""
Master

Runs the entire data collection pipeline from a Raspberry Pi.
"""

from cleaning_tools import *
from scraping_tools import *

print('downloading files.')
download_files()

print('beginning scrape of twitter.')
scrape_twitter()
print('adding scraped data to merged.')
scrape_to_merge()
print('sending merged data to filtered data.')
merged_to_filtered()
print('cleaning the filtered data.')
filtered_to_clean()
print('producing csvs to label.')
get_data_to_label()

print('uploading files.')
upload_files()