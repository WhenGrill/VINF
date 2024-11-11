import os
import json
import csv
import re
import argparse
from collections import defaultdict
from loguru import logger
from enum import Enum
from tqdm import tqdm
import html
from functools import wraps
import time


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        # first item in the args, ie `args[0]` is `self`
        logger.debug(f'Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

class Regexes(Enum):
    SPECIAL_CHARS = r'[^A-Za-z0-9]'
    MULTIPLE_SPACES = r'\s{2,}'

class Indexer:
    def __init__(self, data_folder: str, output_folder: str, input_file: str):
        self.data_folder = data_folder
        self.output_folder = output_folder
        self.input_file = input_file
        self.documents = {}
        self.word_ids = {}
        self.posting_list = defaultdict(lambda: defaultdict(int))
        self.curr_doc_id = 0

    @timeit
    def index_data(self):
        csv_file = os.path.join(self.data_folder, self.input_file)
        num_of_rows = 0
        with open(csv_file, 'r', encoding='utf-8') as f:
            _dmp_data = list(csv.reader(f))
            num_of_rows = len(_dmp_data)

        del _dmp_data

        with open(csv_file, 'r', encoding='utf-8') as f:
            next(f)  # Skip the header line

            for idx, line in enumerate(f): # tqdm(enumerate(f), desc="Indexing data...", unit=" line", total=num_of_rows):
                row = line.strip().split(sep='\t')
                if len(row) >= 2: # Skip empty lines, there is at least Link every time
                    name, link = row[0], row[-1]
                    self.documents[idx] = {"name": html.unescape(name), "link": link}
                    self.process_text(idx, row)
                    #logger.info(f"Assigned doc_id {idx} to {name}")
                    

        self.save_output()

    @timeit
    def clean_text(self, row: list[str]) -> list[str]:
        text = ' '.join(row[1:-1]).lower()
        text = html.unescape(text)
        _text_cleaned = re.sub(Regexes.SPECIAL_CHARS.value, ' ', text)
        _text_cleaned = re.sub(Regexes.MULTIPLE_SPACES.value, ' ', _text_cleaned)
        words = _text_cleaned.split(sep=' ')
        return words
    
    @timeit
    def process_text(self, doc_id, row):
        # Clean the text and split into words
        words = self.clean_text(row)
        word_count = defaultdict(int)
        
        # Create a reverse mapping of words to word_ids for faster lookup
        word_to_id = {word: id for id, word in self.word_ids.items()}
        
        for word in words:
            # Use the reverse mapping for faster word_id lookup
            if word in word_to_id:
                word_id = word_to_id[word]
            else:
                word_id = len(self.word_ids)
                self.word_ids[word_id] = word
                word_to_id[word] = word_id
            
            # Count occurrences of each word (by word_id) using defaultdict
            word_count[word_id] += 1
        
        # Update the posting list with the word counts for this document
        for word_id, count in word_count.items():
            self.posting_list[word_id][doc_id] = count

    def save_output(self):
        os.makedirs(self.output_folder, exist_ok=True)

        with open(os.path.join(self.output_folder, 'documents.json'), 'w') as f:
            json.dump(self.documents, f)

        with open(os.path.join(self.output_folder, 'word_ids.json'), 'w') as f:
            json.dump(self.word_ids, f)

        with open(os.path.join(self.output_folder, 'posting_list.json'), 'w') as f:
            json.dump({str(k): v for k, v in self.posting_list.items()}, f)

        logger.info(f"Indexing complete. Output saved to {self.output_folder}")

def main():
    parser = argparse.ArgumentParser(description="Index data from CSV file.")
    parser.add_argument("--data", default=os.getcwd(), help="Path to the folder containing input CSV file")
    parser.add_argument("--input", default="_merged_data.csv", help="Name of the input CSV file")
    parser.add_argument("--output", default="indexed_data", help="Path to the output folder for indexed data")

    args = parser.parse_args()

    indexer = Indexer(args.data, args.output, args.input)
    indexer.index_data()

if __name__ == "__main__":
    main()
