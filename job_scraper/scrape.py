import json

from job_scraper.crawler import Crawler
from job_scraper.data_saver import Saver
from job_scraper.id_loader import IdLoader
from job_scraper.response_mapping import ResponseMapper
import job_scraper.config as conf
import requests
import time
import argparse

parser = argparse.ArgumentParser(description='Scrape monster.com job applications by id.')
parser.add_argument('-b', '--batch', help='Set the size of each batch.', type=int, default=100)
parser.add_argument('-d', '--delay', help='Set the delay between crawls (seconds).', type=float, default=1.0)
parser.add_argument('-l', '--limit', help='Max number of ids.', type=int)
parser.add_argument('-v', '--verbosity', help='very verbose: debug data', action="count", default=0)

args = parser.parse_args()

batch_size = args.batch
verbosity = args.verbosity
delay_time = args.delay
limit = args.limit

max_attempts_link = 3
max_failed_links = 5
sleep_time = 3600

loader = IdLoader(processed_dir=conf.PROCESSED_DIR, queue_file=conf.QUEUE_FILE, skip_file=conf.SKIP_FILE,
                  verbosity=verbosity)
crawler = Crawler(url_format=conf.CRAWL_URL)
data_saver = Saver(conf.API_ENDPOINT, verbosity)

batches = loader.load_batches_from_queue(batch_size=batch_size, limit=limit)
for batch in batches:
    failed_uploads = 0
    for id in batch:
        time.sleep(delay_time)
        if failed_uploads >= max_failed_links:
            print(f'Failed on {max_failed_links} consecutive crawls, going to sleep for {sleep_time} seconds.')
            time.sleep(sleep_time)

        attempts = 0
        while attempts < max_attempts_link:
            try:
                response = crawler.crawl_id(id)
                mapped_response = ResponseMapper.map(id, response)
                data_saver.upload(mapped_response)
                failed_uploads = 0
                break
            except requests.RequestException:
                attempts += 1
                if verbosity > 1:
                    print(f'Crawling id failed at attempt {attempts}.')
                if attempts == 3:
                    failed_uploads += 1
            except json.decoder.JSONDecodeError:
                attempts += 1
                if verbosity > 1:
                    print(f'Json decode failure, skipping id: {id}.')
                if attempts == 3:
                    failed_uploads += 1
                loader.update_skip_file(id)
                continue

    loader.update_processed(batch)
    loader.update_queue_file()
