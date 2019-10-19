import csv
import math
from os import listdir
from typing import List
from datetime import datetime

import numpy as np


class IdLoader:
    def __init__(self, processed_dir: str, queue_file: str, skip_file: str, verbosity: int = 0):
        self.verbosity = verbosity
        self.processed_dir = processed_dir
        self.queue_file = queue_file
        self.skip_file = skip_file

    def load_batches_from_queue(self, batch_size: int, limit: int = None) -> List[List[str]]:
        queue = self._get_updated_queue(limit)
        n_batches = math.ceil(len(queue) / batch_size)
        batches = np.array_split(queue, max(1, n_batches))

        return batches

    def update_queue_file(self):
        queue = self._get_updated_queue()
        if self.verbosity > 0:
            print(f'new queue length is {len(queue)}.')
        file = open(self.queue_file, 'w', newline='', encoding='utf-8')
        writer = csv.writer(file)
        writer.writerow(queue)
        file.close()

    def update_processed(self, ids: list):
        files = list(listdir(self.processed_dir))
        batch_nr = len(files) + 1
        now = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        file_name = f'batch-{batch_nr}-{now}.csv'
        file = open(self.processed_dir + '/' + file_name, 'w', newline='', encoding='utf-8')
        writer = csv.writer(file)
        writer.writerow(ids)
        file.close()

    def update_skip_file(self, id: str):
        file = open(self.skip_file, 'a', newline='', encoding='utf-8')
        writer = csv.writer(file)
        writer.writerow([id])
        file.close()

    def _get_updated_queue(self, limit: int = None):
        file = open(self.queue_file)
        reader = csv.reader(file)
        queue = [row for row in reader][0]

        if limit is not None:
            queue = queue[:limit]

        processed_ids = self._load_processed_ids()
        updated_queue = list(set(queue) - set(processed_ids))
        file.close()
        if self.verbosity > 0:
            print(f'Updated queue from {len(queue)} to {len(updated_queue)}.')

        return updated_queue

    def _load_processed_ids(self) -> List[str]:
        processed_ids = []

        for processed_batch_file_name in listdir(self.processed_dir):
            processed_batch_file = open(f'{self.processed_dir}/{processed_batch_file_name}')
            processed_batch_reader = csv.reader(processed_batch_file)
            processed_ids += [row for row in processed_batch_reader][0]
            processed_batch_file.close()

        return processed_ids
