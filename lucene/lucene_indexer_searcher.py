import lucene
import time
from tqdm import tqdm

from loguru import logger
from contextlib import contextmanager

import os, platform, sys


from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, StringField, TextField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause, TermQuery
from org.apache.lucene.queryparser.classic import QueryParser
from java.nio.file import Paths

__version__ = "0.1.5"

class DataLoader:
    
    SEP = '\t'
     
    @staticmethod
    def load_data(data_file: str):
        data_dict = {}
        lines = []
        _file_len = 0
        _id = 0
        
        with open(data_file, 'r') as file:
            logger.info(f"Loading extracted data from {data_file}...")
            next(file)  # Skip CSV Header
            lines = file.readlines()
            _file_len = len(lines)
            
        tqdm_bar = tqdm(lines, desc="Loading extracted data...", unit="products")
        for line in tqdm_bar:
            data_dict[_id] = {}
            _split = line.split(DataLoader.SEP)
            
            data_dict[_id]['product_name'] = _split[0]
            data_dict[_id]['brands'] = _split[1]
            data_dict[_id]['packaging'] = _split[2]
            data_dict[_id]['categories'] = _split[3]
            data_dict[_id]['stores'] = _split[4]
            data_dict[_id]['countries_where_sold'] = _split[5]
            data_dict[_id]['nutri_score'] = _split[6]
            data_dict[_id]['nova_score'] = _split[7]
            data_dict[_id]['eco_score'] = _split[8]
            data_dict[_id]['ingredients'] = _split[9]
            data_dict[_id]['allergens'] = _split[10]
            data_dict[_id]['traces'] = _split[11]
            data_dict[_id]['additives'] = _split[12]
            data_dict[_id]['additives_analysis'] = _split[13]
            data_dict[_id]['link'] = _split[14]
            
            _id += 1
            
        logger.success(f"Loaded {_id} / {_file_len} products from {data_file}")
        return data_dict


class LuceneIndexSearchEngine:
    
    def __init_lucene_vm(self):
        logger.info("Initializing Lucene VM...")
        try:
            lucene.initVM()
            logger.success("Lucene VM initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Lucene VM: {e}")
            logger.info("Exiting...")
            exit(1)
        
    @contextmanager
    def _store_handler(self):
        store = FSDirectory.open(Paths.get(self.index_dir))
        try:
            yield store
        finally:
            store.close()

    
        
    def __init__(self, index_dir: str, data_file: str):
        self.index_dir = index_dir
        self.data = DataLoader.load_data(os.path.join(os.getcwd(), data_file))
        
            
        self.__init_lucene_vm()
        
        if not Paths.get(index_dir).toFile().exists():
            Paths.get(index_dir).toFile().mkdirs()
            
        self.analyzer = StandardAnalyzer()
        
        self._index_data()
        
    
    def _index_data(self):
        user_input = input("[INDEXER] Do you want to index the data? (Y/N): ")
        
        if user_input.lower() not in ["y", "yes"]:
            logger.info("Skipping indexing...")
            return
        else:
            logger.info("Removing old index files...")
            import shutil
            shutil.rmtree(self.index_dir, ignore_errors=True)
            logger.success("Old index files removed")
        
        logger.info(f"Indexing {len(self.data)} products...")
        with self._store_handler() as store:
            index_config = IndexWriterConfig(self.analyzer)
            index_writer = IndexWriter(store, index_config)
            for _, doc_data in tqdm(self.data.items(), desc="Indexing data...", unit="products"):
                doc = Document()
                for key, value in doc_data.items():
                    field_store_type = Field.Store.YES if key != "link" else Field.Store.NO
                    #if key in ['product_name', 'ingredients', 'categories', 'allergens']:
                    doc.add(TextField(key, value, field_store_type))
                    #else:
                        #doc.add(StringField(key, value, field_store_type))
                index_writer.addDocument(doc)
                
            index_writer.commit()
            
        logger.success(f"Indexed {len(self.data)} products")
            
            
    def search(self, query_string: str, max_results: int = 10):
        # Modify query for better results
        modified_query = f"(\"{query_string}\")||({query_string})"
        
        with self._store_handler() as store:
            index_reader = DirectoryReader.open(store)
            index_searcher = IndexSearcher(index_reader)
            
            # Create a BooleanQuery to search across all fields except 'link'
            boolean_query = BooleanQuery.Builder()
            fields = ['product_name', 'brands', 'packaging', 'categories', 'stores', 
                      'countries_where_sold', 'nutri_score', 'nova_score', 'eco_score', 
                      'ingredients', 'allergens', 'traces', 'additives', 'additives_analysis', 'link']
            
            for field in fields:
                query_parser = QueryParser(field, self.analyzer)
                query = query_parser.parse(modified_query)
                boolean_query.add(query, BooleanClause.Occur.SHOULD)
            
            # Execute the search
            top_docs = index_searcher.search(boolean_query.build(), max_results)
            
            # Process and return results
            results = []
            for score_doc in top_docs.scoreDocs:
                doc = index_searcher.doc(score_doc.doc)
                result = {field: doc.get(field) for field in fields}
                result['score'] = score_doc.score
                results.append(result)
            
            return results


def _print_header():
    logger.info("")
    logger.info("")
    logger.info("####################################################")
    logger.info("#")
    logger.info("#\tVINF - FIIT STU - Indexer & Searcher based on Lucene")
    logger.info("#")
    logger.info("#\t -------------------------------------------------")
    logger.info("#")
    logger.info("#\tAuthor: @WhenGrill (github.com/WhenGrill)")
    logger.info("#")
    logger.info(f"#\tVersion: v{__version__}")
    logger.info("#")
    logger.info(f"#\tOS: {platform.system()} {platform.release()} ({platform.architecture()[0]} - {platform.machine()}) - {platform.platform()}")
    logger.info("#")
    logger.info(f"#\tPython: {platform.python_version()}")
    logger.info("#")
    logger.info("#\t -------------------------------------------------")
    logger.info("#")
    logger.info("#\tDataset: Open Food Facts (https://world.openfoodfacts.org/)")
    logger.info("#")
    logger.info(f"#\tSize of Processed Data: {os.path.getsize('./data.csv') / 1024 / 1024:.2f} MB")
    logger.info("#")
    logger.info("####################################################")
    logger.info("")
    logger.info("")

def main():
    
    _print_header()
    
    index_dir = "./index"
    
    lucene_search_engine = LuceneIndexSearchEngine(index_dir, "./data.csv")
    
    try:
        
        while True:
            query = input("[USER] Enter query: ")
            results = lucene_search_engine.search(query)
            
            for i, result in enumerate(results[:10], 1):
                print(f"\nResult {i}:")
                print("----------------------------------------------------------------")
                print(f"\tScore:         {result['score']}")
                print("----------------------------------------------------------------")
                print(f"\tProduct Name:  {result['product_name']}")
                print(f"\tBrands:        {result['brands']}")
                print(f"\tAllergens:     {result['allergens']}")
                print(f"\tNutri Score:   {result['nutri_score']}")
                print(f"\tNova Score:    {result['nova_score']}")
                print(f"\tEco Score:     {result['eco_score']}")
                print(f"\tCountry:       {result['countries_where_sold']}")
                print(f"\tLink:          {result['link']}")
                print("----------------------------------------------------------------")
                print("")
            
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Error during search: {e}")
    finally:
        logger.info("Exiting...")
        exit(0)


if __name__ == '__main__':
    main()


