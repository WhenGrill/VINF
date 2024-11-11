import pickle
from collections import deque
import re

def load_state(file_path):
    with open(file_path, 'rb') as f:
        return pickle.load(f)

def save_state(file_path, state):
    with open(file_path, 'wb') as f:
        pickle.dump(state, f)

def edit_to_visits(file_path, visits):
    state = load_state(file_path)
    
    #state['to_visit'] = visits
    
    
    _list_to_visit = list(state.get('to_visit', []))
    
    _to_save = visits + _list_to_visit
    
    state['to_visit'] = _to_save
    
    save_state(file_path, state)
    
def _get_number_of_products(file_path):
    PRODUCT_URL = re.compile(r'https://world\.openfoodfacts\.org/product/.*')
    state = load_state(file_path)
    _to_visit = state.get('to_visit', [])
    _products = [PRODUCT_URL.match(x[0]) for x in _to_visit]
    return len([x for x in _products if x is not None])
    
if __name__ == "__main__":
    base_url = 'https://world.openfoodfacts.org'
    add_links = []
    
    print(_get_number_of_products('crawler_state.pkl'))
    
    #for i in range(61, 1000):
    #    add_links.append([f'{base_url}/{i}', 0])
    #edit_to_visits('crawler_state.pkl', add_links)