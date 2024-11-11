
import os
from enum import Enum
import re
import pandas as pd
import glob
from loguru import logger
import sys
from tqdm import tqdm
import csv
from unidecode import unidecode
import argparse

class ExtractRegex(Enum):
    
    # [\s\S]*?

    TAG: re.Pattern = re.compile(r'<[^>]*>')
    #END_TAG: re.Pattern = re.compile(r'</[^>]*>')
    
    PRODUCT_NAME: re.Pattern = re.compile(r'<h2\s+class=\"title-1\"\s+property=\"food:name\"\s+itemprop=\"name\">[\s\S]*?</h2>')
    
    BRANDS: re.Pattern = re.compile(r'<span\s+class=\"field_value\"\s+id=\"field_brands_value\">[\s\S]*?</span>')
    
    PACKAGING: re.Pattern = re.compile(r'<span\s+class=\"field_value\"\s+id=\"field_packaging_value\">[\s\S]*?</span>')
    
    CATEGORIES: re.Pattern = re.compile(r'<span\s+class=\"field_value\"\s+id=\"field_categories_value\">[\s\S]*?</span>')
    
    STORES: re.Pattern = re.compile(r'<span\s+class=\"field_value\"\s+id=\"field_stores_value\">[\s\S]*?</span>')
    
    COUNTRIES_WHERE_SOLD: re.Pattern = re.compile(r'<span\s+class=\"field_value\"\s+id=\"field_countries_value\">[\s\S]*?</span>')
    
    NUTRI_SCORE: re.Pattern = re.compile(r'<a\s+href=\"#panel_nutriscore_[0-9]{1,4}\"\s+onclick=\"[\s\S]*?\">[\s\S]*?</a>')  # TODO LOOK INTO THIS
    
    NOVA_SCORE: re.Pattern = re.compile(r'<a\s+href=\"#panel_nova\"\s+onclick=\"[\s\S]*?\">[\s\S]*?</a>')
    
    ECO_SCORE: re.Pattern = re.compile(r'<a\s+href=\"#panel_ecoscore\"\s+onclick=\"[\s\S]*?\">[\s\S]*?</a>')
    
    INGREDIENTS: re.Pattern = re.compile(r'<div\s+id=\"panel_ingredients_content\"\s+class=\"content\s+panel_content\s+active\s+expand-for-large\">[\s\S]*?</div>')
    
    ALLERGENS: re.Pattern = re.compile(r'(?<=Allergens:)</strong>[\s\S]*?</div>')
    
    TRACES: re.Pattern = re.compile(r'(?<=Traces:)</strong>[\s\S]*?</div>')
    
    ADDITIVES: re.Pattern = re.compile(r'<a\s+href=\"#panel_additive[\s\S]*?\"[\s\S]*?>[\s\S]*?</h4>')
    
    ADDITIVES_ANALYSIS: re.Pattern = re.compile(r'<a\s+href=\"#panel_ingredients_analysis(?!.*_details_content)[^\"]*?\"[^\>]*?>[\s\S]*?</h4>')
    

def load_url_hashes(file_path):
    url_hashes = {}
    with open(file_path, 'r') as f:
        for line in f:
            hash_value, url = line.strip().split('\t')
            url_hashes[hash_value] = url
    return url_hashes

def extract_info(html: str):
    #logger.info("Starting information extraction from HTML")
    extracted_info = {}
    for regex in ExtractRegex:
        if regex == ExtractRegex.TAG:
            continue
        
        matches = regex.value.findall(html)
        if matches:
            _extracted = [ExtractRegex.TAG.value.sub(' ', match).strip() for match in matches] # CLEAR ALL HTML TAGS + Strip (only deletes whitespaces before and after) and merge to one sentence
            _extracted_text = unidecode(' '.join(_extracted).replace('&nbsp;', ' ').replace('\n', ''))
           # _extracted_text = re.sub(r'[^A-Za-z0-9]', ' ', extracted_text).lower()

            _extracted_text = re.sub(r'\s{2,}', ' ', _extracted_text)
            extracted_info[regex.name] = _extracted_text  # Wrap the extracted text in a list
            logger.debug(f"Extracted {len(_extracted)} items for {regex.name}")
        else:
            extracted_info[regex.name] = ''
            logger.debug(f"No matches found for {regex.name}")
    return extracted_info

def merge_files(output_folder: str, merged_output: str):
    # Merge all files
    logger.info("Merging all files")
    files = [f for f in glob.glob(os.path.join(output_folder, '*')) if not os.path.basename(f).startswith('_')]
    merged_output_path = os.path.join(os.getcwd(), merged_output)
    
    with open(merged_output_path, 'w', encoding='utf-8') as outfile:
        # Write header from the first file
        with open(files[0], 'r', encoding='utf-8') as first_file:
            header = first_file.readline().strip()
            outfile.write(header + '\n')
        
        # Process all files
        for file in tqdm(files, desc="Merging files", unit="files"):
            with open(file, 'r', encoding='utf-8') as infile:
                next(infile)  # Skip header
                
                # Write all data rows
                for line in infile:
                    outfile.write(line)
    
    logger.info(f"Saved merged data to {merged_output_path}")
    

def process_html_files(data_folder: str, output_folder: str, url_hashes_file: str, merged_output: str, skip_processed: bool = False):
    logger.info(f"Starting processing of HTML files from {data_folder}")
    os.makedirs(output_folder, exist_ok=True)
    
    header = None
    
    if skip_processed:
        logger.info("Skipping processed files")
        merge_files(output_folder, merged_output)
        return
    
    url_hashes = load_url_hashes(url_hashes_file)

    html_files = glob.glob(os.path.join(data_folder, '*.html'))
    for html_file in tqdm(html_files, desc="Processing HTML files", unit="files"):
        logger.debug(f"Processing file: {html_file}")
        with open(html_file, 'r', encoding='utf-8') as file:
            html_content = file.read()
        
        extracted_info = extract_info(html_content)
        
        # Extract hash from html filename
        file_hash = os.path.splitext(os.path.basename(html_file))[0]
        
        # Add LINK to extracted_info
        extracted_info['LINK'] = url_hashes.get(file_hash, '')
        
        csv_filename = file_hash + '.csv'
        csv_path = os.path.join(output_folder, csv_filename)
        
        if not header:
            header = '\t'.join(extracted_info.keys()) + '\n'
        
        try:
            with open(csv_path, 'w', encoding='utf-8') as f:
                row = '\t'.join(str(value) for value in extracted_info.values()) + '\n'
                f.write(header)
                f.write(row)
        except Exception as e:
            logger.error(f"Error saving extracted data to {csv_path}: {e}")
            logger.error(f"Extracted data: {extracted_info}")
        #logger.info(f"Saved extracted data to {csv_path}")
    
    merge_files(output_folder, merged_output)

def main():
    parser = argparse.ArgumentParser(description="Extract and process HTML files.")
    parser.add_argument("--data", default="data", help="Path to the folder containing HTML files")
    parser.add_argument("--output-folder", default="extracted_data_single_test", help="Path to the output folder for extracted data")
    parser.add_argument("--url-hashes", default="url_hashes.txt", help="Path to the URL hashes file")
    parser.add_argument("--merged-output", default=os.path.join(os.getcwd(), "_merged_data_test.csv"), help="Path and file name for the merged output CSV")
    parser.add_argument("--skip-processed", action="store_true", help="Skip processed files")
    args = parser.parse_args()

    logger.info("Starting extraction ...")
    process_html_files(args.data, args.output_folder, args.url_hashes, args.merged_output, args.skip_processed)
    logger.info(f"Extraction complete. Check the output folder for results. Merged data saved to {args.merged_output}")

if __name__ == "__main__":
    logger.remove()
    logger.add(lambda msg: tqdm.write(msg, end=""), level="INFO")
    logger.add("extractor.log", rotation="10 MB", level="INFO")
    logger.add('extractor_debug.log', level='DEBUG')
    main()
