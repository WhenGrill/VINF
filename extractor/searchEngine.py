import json
from typing import List, Dict, Union, Set
import re
from loguru import logger
import math
import os


class SearchEngine:
    def __init__(self, index_folder: str, word_index_file: str, documents_file: str, posting_list_file: str):
        self.index_folder = os.path.join(os.getcwd(), index_folder)
        self.documents_file = os.path.join(self.index_folder, documents_file)
        self.posting_list_file = os.path.join(self.index_folder, posting_list_file)
        self.word_index_file = os.path.join(self.index_folder, word_index_file)
        self.word_ids = {}
        self.posting_list = {}
        self.documents = {}
        
        self.load_indexes()
        
        self.total_documents = len(self.documents)

    def load_indexes(self):
        logger.info(f"Loading index...")
        logger.info(f"Loading word index...")
        with open(self.word_index_file, 'r', encoding='utf-8') as f:
            self.word_ids = json.load(f)
        
        logger.info(f"Loading posting list...")
        with open(self.posting_list_file, 'r', encoding='utf-8') as f:
            self.posting_list = json.load(f)
            
        logger.info(f"Loading documents...")
        with open(self.documents_file, 'r', encoding='utf-8') as f:
            self.documents = json.load(f)
            
        logger.success(f"Index loaded successfully")

        
    def _word_to_id(self, word: str) -> Union[str, None]:
        for word_id, word_str in self.word_ids.items():
            if word_str == word:
                return word_id
        return None
        
    def search_index(self, query_words: List[str]) -> List[Dict[str, str]]:
        results = []
        for word in query_words:
           word_id = self._word_to_id(word)
        return results

    
    def __compute_idf_query(self, ) -> Dict[str, int]:
        idf_values = {word_id: math.log10(self.total_documents / len(self.posting_list[str(word_id)])) for word_id in query_words}
        return idf_values

    def __clean_split_query(self, query: str) -> List[str]:
        _text = re.sub(r'[^a-zA-Z0-9]', ' ', query.lower())
        _text = re.sub(r'\s+', ' ', _text).strip()
        _text_arr = _text.split()
        return _text_arr
    
    def _set_of_docs_containing_word(self, word_list: List[str]) -> Set[str]:
        sets_of_docs_containing_words = []
        for word_id in word_list:
            docs_containing_word = set(self.posting_list[word_id].keys())
            sets_of_docs_containing_words.append(docs_containing_word)
        
        # Intersect all sets to get documents containing all words
        docs_containing_all_words = set.intersection(*sets_of_docs_containing_words)
        
        return docs_containing_all_words
    
    def preprocess_document(self, query_info: Dict[str, Dict[str, float]]):
        _doc_info = {}
        
        docs_containing_all_words = self._set_of_docs_containing_word(query_info.keys())
        
        for word_id in query_info.keys():
            for doc_id_str in docs_containing_all_words:
                if doc_id_str not in _doc_info:
                    _doc_info[doc_id_str] = {'total_score': 0}
                
                doc_tf = self.posting_list[word_id][doc_id_str]
                wf_log = 1 + math.log10(doc_tf)
                wtd = wf_log / self.documents[doc_id_str]['wf_length']
                score = wtd * query_info[word_id]['wtq']
                
                _doc_info[doc_id_str][word_id] = {'tf': doc_tf,
                                                   'wf_log': wf_log,
                                                   'wtd': wtd,
                                                   'score': score
                                                   }
                _doc_info[doc_id_str]['total_score'] += score
        
        # Sort documents by highest total score
        sorted_docs = sorted(_doc_info.items(), key=lambda x: x[1]['total_score'], reverse=True)
        return dict(sorted_docs)
        

    def preprocess_query(self, query: str) -> Dict[int, Dict[str, float]]:
        _text_arr = self.__clean_split_query(query)
        
        _query_info = {}
        for word in _text_arr:
            word_id = self._word_to_id(word)
            if word_id is None:
                logger.warning(f"Word {word} not found in index")
                return None
            
            if word_id not in _query_info:
                _query_info[word_id] = {'tf': 1}
            else:
                _query_info[word_id]['tf'] += 1
        
        for word_id in _query_info.keys():
            tf = _query_info[word_id]['tf']
            df = len(self.posting_list[str(word_id)])
            idf = math.log10(self.total_documents / df)
            tf_idf = tf * idf
            
            wf = 1 + math.log10(tf)
            wtq = wf * idf
            
            _query_info[word_id]['df'] = df
            _query_info[word_id]['idf'] = idf
            _query_info[word_id]['tf_idf'] = tf_idf
            _query_info[word_id]['wf'] = wf
            _query_info[word_id]['wtq'] = wtq
        
        return _query_info
            
    
    def search(self):
        while True:
            try:
                user_query = input("Enter a search query: ")



                query_words = self.preprocess_query(user_query)
                
                if query_words is None or query_words == {}:
                    print("No results found.")
                    continue
                
                results = self.preprocess_document(query_words)
                
                print(f"Total results: {len(results)}")
                
                for i, (doc_id, doc_info) in enumerate(list(results.items())[:10], start=1):
                    print(f"{i}. Document ID: {doc_id}, Score: {doc_info['total_score']}")
                    print(f"   Title: {self.documents[doc_id]['name']}")
                    print(f"   URL: {self.documents[doc_id]['link']}")
                    print() # Newline for better readability
            except KeyboardInterrupt:
                logger.info("Exiting...")
                return


if __name__ == '__main__':
    search_engine = SearchEngine('indexed_data', 'word_ids.json', 'documents_w_length.json', 'posting_list.json')
    
    search_engine.search()

