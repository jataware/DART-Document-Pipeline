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

warnings.filterwarnings("ignore", category=PyPDF2.utils.PdfReadWarning)

TEMP_DOWNLOAD_PATH = './tmp'

# AWS CONFIG
AWS_PROFILE = 'wmuser'

# S3 CONFIG
S3_BASE_URL = 'https://world-modelers.s3.amazonaws.com'
S3_BASE_KEY = 'documents/migration'

# ELASTIC SEARCH CONFIG
S3_INDEX = 'wm-dev'
DOC_TYPE = 'wm-document'

BUCKET_NAME = 'world-modelers'
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

def parse_pdfinfo(tika_metadata, doc):
    """
    Takes in pdfinfo from Tika and a document and enriches the document
    with metadata fields
    """
    t_m = extract_tika(f"{dir_path}/pdf/{file_path}")[1]
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

def parse_document(file_path, category, source_url):
    """
    Take in the full path to a file and perform appropriate text extrraction
    as well as metadata enrichment (if a PDF, using pdfinfo fields)
    """
    file_name = os.path.basename(file_path)
    file_type = os.path.splitext(file_path)[1]
    
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
    
    if file_type == '.pdf':
        doc['file_type'] = file_type
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
    elif file_type == '.html':
        try:
            extracted_text['bs4'] = extract_bs4(file_path)
        except Exception as e:
            print(f"BS4 extraction failed: {e}")
    
    if tika_metadata:
        doc = parse_pdfinfo(tika_metadata, doc)
    
    doc['extracted_text'] = extracted_text
    
    return doc

def get_filename_from_cd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]


def connect_to_es():
    region = 'us-east-1'
    service = 'es'
    eshost = 'search-world-modelers-dev-gjvcliqvo44h4dgby7tn3psw74.us-east-1.es.amazonaws.com'

    session = boto3.Session(region_name=region, profile_name='wmuser')
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    token = credentials.token

    aws_auth = AWS4Auth(
        access_key,
        secret_key,
        region,
        service,
        session_token=token
    )
    
    es = Elasticsearch(
        hosts = [{'host': eshost, 'port': 443}],
        http_auth=aws_auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

def connect_to_s3():
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3 = session.resource("s3")
    s3_client = boto3.client("s3")

def main():
    es = connect_to_es()
    s3_client = connect_to_s3()
    
    # Ensure we are connected
    print(json.dumps(es.info(), indent=2))
    
    schema = json.loads(open("document-schema.json").read())
    book = openpyxl.load_workbook(SPREADSHEET)
    for name in SHEET_NAMES:
        sheet = book[name]
        for row in range(2, sheet.max_row):
            doc_name = sheet["A%s" % (row,)].value
            url_path = sheet["D%s" % (row,)].value
            doc_date = sheet["B%s" % (row,)].value
                    
            print(f"Processing - {doc_name}")
                        
            if 'http' in url_path:
                print("Downloading - %s" % (sheet[f"D{row}"].value,))
                r = requests.get(url_path, verify=False, stream=True, allow_redirects=True)
                r.raw.decode_content = True
                filename = get_filename_from_cd(r.headers.get('content-disposition'))
                
                open(f"{TEMP_DOWNLOAD_PATH}/{filename}", 'wb').write(r.content)
                #with open(f"{TEMP_DOWNLOAD_PATH}/{filename}", 'wb') as f:
                #    shutil.copyfileobj(r.raw, f)    
                
                count = 0
                for file_path in raw_files:    
                    file_name = f"{TEMP_DOWNLOAD_PATH}/{filename}"
                    s3_key = f"{S3_BASE_KEY}/{file_path}"
                    s3_uri = f"{S3_BASE_URL}/{s3_key}"

                    #############################################
                    ### 1. Upload raw file to S3 ################
                    #############################################
                    s3_client.upload_file(file_name, BUCKET_NAME, s3_key)
                    
                    
                    #############################################
                    ### 2. Parse document #######################
                    #############################################
                    # hard code category and source_url (empty) for the time being
                    title = doc_name
                    category = 'Migration'
                    source_url = url_path
                    creation_date = doc_date
                    doc = parse_document(file_name, category, source_url)
                    doc['stored_url'] = s3_uri
                    
                    # Validate document against schema
                    validate(instance=doc, schema=schema)

                    
                    #############################################
                    ### 3. Index parsed document to Elasticsearch
                    #############################################  
                    
                    # create the index if it does not exist
                    if not es.indices.exists(S3_INDEX):
                        es.indices.create(S3_INDEX)
                        print(f"Created ES index: {S3_INDEX}")
                        
                    es.index(index=S3_INDEX, doc_type=DOC_TYPE, id=doc.pop('_id'), body=doc)
                    count += 1
                    if count % 25 == 0:
                        print(count)    
            else:
                print("Skipping due to incorrect URL")
            

if __name__ == '__main__':
    main()
