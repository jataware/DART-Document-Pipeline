from __future__ import print_function
import  openpyxl
import boto3, json
import os
from shutil import copyfile
from hashlib import sha256
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from tika import parser
import PyPDF2
from bs4 import BeautifulSoup
from jsonschema import validate
import warnings
import re
import requests
import urllib3
import magic
import configparser
from PIL import Image 
import pytesseract 
import sys 
from pdf2image import convert_from_path 

warnings.filterwarnings("ignore", category=PyPDF2.utils.PdfReadWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TEMP_DOWNLOAD_PATH = '/tmp'
ERRORS = []

# CONFIG

config = configparser.ConfigParser()
config.readfp(open(r'../config'))
DOC_TYPE = config.get('ES', 'doc_type')

# AWS CONFIG
AWS_PROFILE = config.get('AWS', 'profile')

# S3 CONFIG
S3_BASE_URL = config.get('S3', 'base_url')
S3_BASE_KEY = config.get('S3', 'base_key')
BUCKET_NAME = config.get('S3', 'bucket_name')

# ELASTIC SEARCH CONFIG
ES_INDEX = config.get('ES', 'index')
DOC_TYPE = config.get('ES', 'doc_type')
REGION = config.get('ES', 'region')
SERVICE = config.get('ES', 'service')
ES_HOST = config.get('ES', 'host')

SPREADSHEET = 'Six_Twelve-Month_November_2019_Evaluation_Documents-Updated-6June2019.xlsx'
SHEET_NAMES = [
    'Six-Month Evaluation Documents',
    'Additional Six-Month Eval Docs',
    'Twelve-Month Eval Docs',
    'November 2019 SSudan Docs',
    'November 2019 Ethiopia Docs',
    'Luma-Provided Ethiopia Docs'
]

def extract_tika(file_path):
    """
    Take in a file path of a PDF and return its Tika extraction
    https://github.com/chrismattmann/tika-python
    
    Returns: a tuple of (extracted text, extracted metadata)
    """
    tika_data = parser.from_file(file_path)
    tika_extraction = tika_data.pop('content')
    tika_metadata = tika_data.pop('metadata')
    return (tika_extraction, tika_metadata)

def extract_pypdf2(file_path):
    """
    Take in a file path of a PDF and return its PyPDF2 extraction
    https://github.com/mstamy2/PyPDF2
    """
    
    pdfFileObj = open(file_path, 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    page_count = pdfReader.numPages
    pypdf2_extraction = ''
    for page in range(page_count):
        pageObj = pdfReader.getPage(page)
        page_text = pageObj.extractText()
        pypdf2_extraction += page_text
    return pypdf2_extraction

def extract_bs4(file_path):
    """
    Take in a file path of an HTML document and return its Beautiful Soup extraction
    https://www.crummy.com/software/BeautifulSoup/bs4/doc/
    """        
    htmlFileObj = open(file_path, 'r')
    soup = BeautifulSoup(htmlFileObj, "lxml")
    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.decompose()    # rip it out
    # get text
    text = soup.get_text()        
    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    bs4_extraction = '\n'.join(chunk for chunk in chunks if chunk)
    return bs4_extraction

def parse_pdfinfo(tika_metadata, doc, file_path):
    """
    Takes in pdfinfo from Tika and a document and enriches the document
    with metadata fields
    """
    t_m = extract_tika(file_path)[1]
    title = t_m.get('title',None)
    date = t_m.get('Creation-Date',t_m.get('created',None))
    author = t_m.get('Author',None)
    last_modified = t_m.get('Last-Modified',None)
    if title:
        doc['title'] = title
    if date:
        doc['creation_date'] = {'date': date}
    if author:
        doc['source'] = {'author_name': author}
    if last_modified:
        doc['modification_date'] = {'date': last_modified}
    return doc

def extract_pytesseract(file_path):
    pages = convert_from_path(file_path, 500)
    out_text = ""
    image_counter = 1    
    for page in pages: 
        filename = f"{TEMP_DOWNLOAD_PATH}/page_{str(image_counter)}.jpg"
        page.save(filename, 'JPEG')     
        image_counter = image_counter + 1
    
    filelimit = image_counter-1
    
    for i in range(1, filelimit + 1): 
        filename = f"{TEMP_DOWNLOAD_PATH}/page_{str(i)}.jpg"            
        text = str(((pytesseract.image_to_string(Image.open(filename))))) 
        text = text.replace('-\n', '')         
        out_text += text

    return out_text


def parse_document(file_path, category, source_url):
    """
    Take in the full path to a file and perform appropriate text extrraction
    as well as metadata enrichment (if a PDF, using pdfinfo fields)
    """
    file_name = file_path.split('/')[-1]
    mime = magic.Magic(mime=True)
    file_type = mime.from_file(file_path).split('/')[-1]
    
    # sha256 hash the raw contents of the file to generate a UUID
    raw = open(file_path,'rb').read()
    _id = sha256(raw).hexdigest()
    
    doc = {'_id': _id,
           'file_name': file_name, 
           'file_type': file_type,
           'category': category,
           'source_url': source_url}
    
    extracted_text = {}
    
    # set tika_metadata to None and overwrite it
    # if we are able to extract pdfinfo with Tika
    tika_metadata = None
    
    if 'pdf' in file_type or 'xml' in file_type:
        doc['file_type'] = 'pdf'
        try:
            tika_extraction, tika_metadata = extract_tika(file_path)
            extracted_text['tika'] = tika_extraction
        except:
            # need to strip random unicode from file_path so store a tmp file using the 
            # documents _id. The path for this is currently hard coded
            # TODO: remove hard coding of below error handling paths
            try:
                copyfile(file_path, f'./tmp/{_id}.pdf')
                extract_tika(f'./tmp/{_id}.pdf')
            except Exception as e:
                print(f"Tika extraction failed: {e}") 
        try:
            extracted_text['pypdf2'] = extract_pypdf2(file_path)
        except Exception as e:
            print(f"PyPDF2 extraction failed: {e}")
        try:
            extracted_text['pytesseract'] = extract_pytesseract(file_path)
        except Exception as e:
            print(f"PyTesseract extraction failed: {e}")
    elif 'html' in file_type or 'text' in file_type:
        doc['file_type'] = 'html'
        try:
            extracted_text['bs4'] = extract_bs4(file_path)
        except Exception as e:
            print(f"BS4 extraction failed: {e}")
    else:
        if file_name != 'urlsatrctjqesrcssourcewebcd3ved0ahUKEwiL15Wux4XbAhWFhOAKHWIcAdEQFggxMAIurlhttp3A2F2Fwww.html':
            raise ValueError("*** Could not find extractor for "+file_name+" with mime type - "+file_type)
    
    if tika_metadata:
        doc = parse_pdfinfo(tika_metadata, doc, file_path)
    
    doc['extracted_text'] = extracted_text
    bs4_len = len(extracted_text.get('bs4') or '')
    tika_len = len(extracted_text.get('tika') or '')
    pypdf2_len = len(extracted_text.get('pypdf2') or '')
    pytesseract_len = len(extracted_text.get('pytesseract') or '')
    if bs4_len < 500 and tika_len < 500 and pypdf2_len < 500 and pytesseract_len < 500:
        ERRORS.append("*** Error extracted_text for "+file_name+" with type "+file_type+" is less than 500 chars - "+json.dumps(extracted_text))
        if file_name != 'urlsatrctjqesrcssourcewebcd3ved0ahUKEwiL15Wux4XbAhWFhOAKHWIcAdEQFggxMAIurlhttp3A2F2Fwww.html':
            raise ValueError('ERRORS --- ' + json.dumps(ERRORS))
    
    return doc

def slugify(value):
    return ''.join([c for c in value if c.isalpha() or c.isdigit() or c ==' ' or c == '.']).rstrip()

def get_filename(req, url):
    header = req.headers
    content_type = header.get('content-type')
    ext = content_type.split(';')[0].split('/')[-1]
    return f"{url.split('/')[-1][:225].split('.')[0]}.{ext}"


def connect_to_es():
    session = boto3.Session(region_name=REGION, profile_name='wmuser')
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    token = credentials.token

    aws_auth = AWS4Auth(
        access_key,
        secret_key,
        REGION,
        SERVICE,
        session_token=token
    )
    
    return Elasticsearch(
        hosts = [{'host': ES_HOST, 'port': 443}],
        http_auth=aws_auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

def connect_to_s3():
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3 = session.resource("s3")
    return boto3.client("s3")

def main():
    es = connect_to_es()
    s3_client = connect_to_s3()
    if not es.indices.exists(ES_INDEX):
        es.indices.create(ES_INDEX)
        print(f"Created ES index: {ES_INDEX}")
    
    # Ensure we are connected
    print(json.dumps(es.info(), indent=2))
    
    schema = json.loads(open("../document-schema.json").read())
    book = openpyxl.load_workbook(SPREADSHEET)
    for name in SHEET_NAMES:
        sheet = book[name]
        for row in range(2, sheet.max_row):
            doc_name = sheet[f"A{row}"].value
            url_path = sheet[f"D{row}"].value.strip()
            doc_date = sheet[f"B{row}"].value
            query = {
                "query": {
                    "match" : {
                        "source_url.keyword" : url_path
                    }
                }
            }            
            
            es_count = es.count(index=ES_INDEX, body=query)['count']
            if es_count < 1 and url_path != 'https://www.unicef.org/eapro/Brief_Nutrition_Overview.pdf' and url_path != 'http://documents.wfp.org/stellent/groups/public/documents/ena/wfp284277.pdf?_ga=2.110714181.1951289426.1518533341-841502227.1498664735':
                print(f"Processing - {doc_name}")
                print("Downloading - %s" % (url_path,))
                try:
                    r = requests.get(url_path, verify=False, stream=True, allow_redirects=True, timeout=30)
                except Exception as e:
                    try:
                        r = requests.get(url_path.replace('http://', 'https://'), verify=False, stream=True, allow_redirects=True, timeout=30)
                    except Exception as e:
                        r = requests.get(url_path.replace('www.', ''), verify=False, stream=True, allow_redirects=True, timeout=30)
                r.raw.decode_content = True
                filename = f"{TEMP_DOWNLOAD_PATH}/{slugify(get_filename(r, url_path))}"
                open(filename, 'wb').write(r.content)
                count = 0    
                s3_key = f"{S3_BASE_KEY}{TEMP_DOWNLOAD_PATH}/DEV/{filename.split('/')[-1]}"
                s3_uri = f"{S3_BASE_URL}/{s3_key}"
                s3_client.upload_file(filename, BUCKET_NAME, s3_key)
                
                title = doc_name
                category = name
                source_url = url_path
                creation_date = doc_date
                doc = parse_document(filename, category, source_url)
                doc['stored_url'] = s3_uri
                
                validate(instance=doc, schema=schema)
                    
                es.index(index=ES_INDEX, doc_type=DOC_TYPE, id=doc.pop('_id'), body=doc)
                count += 1
                if count % 25 == 0:
                    print(count)    
    print('ERRORS --- ' + json.dumps(ERRORS))
            

if __name__ == '__main__':
    main()
