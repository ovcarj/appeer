import os
import requests
import json
import time
import sys

def load_json(json_filename):

    with open(json_filename, encoding='utf-8-sig') as f:
        data = json.load(f)

    return data

def initialize_headers():

    headers = requests.utils.default_headers()
    headers.update({'User-Agent': 'My User Agent 1.0'})

    return headers

def scrape_htmls(publications, requests_headers, destination_directory, verbose=True,
        sleep_time=0.5):
    
    os.makedirs(destination_directory, exist_ok=True)

    for i in range(len(publications)):

        publication = publications[i]
        url = publication['article_url']

        if verbose:
            print(f'{i + 1}/{len(publications)}')
            print(f'Scraping {url}...')

        response = requests.get(url, headers=headers)

        write_to_file(f'{destination_directory}/{i}_html.dat', response)

        time.sleep(sleep_time)

    if verbose:
        print(f'Job done!')

def write_to_file(path_to_file, response):

	with open(path_to_file, 'w+') as f:
            f.write(response.text)


if __name__ == '__main__':

    json_filename = sys.argv[1]
    publications = load_json(json_filename)

    headers = initialize_headers()

    prefix = json_filename.split('.')[0]

    destination_directory = f'{prefix}_scraped_htmls'

    scrape_htmls(publications, headers, destination_directory)
