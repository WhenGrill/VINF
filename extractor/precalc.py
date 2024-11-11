import os
import sys
import re
import json
import math
from loguru import logger
from collections import defaultdict
from typing import List, Dict, Optional, Union


class PreCompute:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.documents = {}
        self.word_ids = {}
        self.posting_list = {}
        
        self.load()
        
        
    def load(self):
        with open(os.path.join(os.getcwd(), self.file_path, 'documents.json'), 'r', encoding='utf-8') as f:
            self.documents = json.load(f)
        
        with open(os.path.join(os.getcwd(), self.file_path, 'word_ids.json'), 'r', encoding='utf-8') as f:
            self.word_ids = json.load(f)
            
        with open(os.path.join(os.getcwd(), self.file_path, 'posting_list.json'), 'r', encoding='utf-8') as f:
            self.posting_list = json.load(f)

    
    def save_document(self):
        with open(os.path.join(os.getcwd(), self.file_path, 'documents_w_length.json'), 'w', encoding='utf-8') as f:
            json.dump(self.documents, f)
    

    def _compute(self):
        for doc_id in self.documents.keys():
            length = 0
            for _, doc_dict in self.posting_list.items():
                if doc_id in doc_dict:
                    length += math.log10(1 + doc_dict[doc_id])
            
            self.documents[doc_id]['wf_length'] = length
            logger.info(f'Computed length for {doc_id}: {length}')
        
        
if __name__ == '__main__':
    pre_compute = PreCompute('indexed_data')
    pre_compute._compute()
    pre_compute.save_document()


