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
        doc = parse_pdfinfo(tika_metadata, doc, file_path)
    
    doc['extracted_text'] = extracted_text
    
    return doc

def slugify(value):
    return ''.join([c for c in value if c.isalpha() or c.isdigit() or c ==' ' or c == '.']).rstrip()

def get_filename(cd, url):
    if (not cd) or len(re.findall('filename=(.+)', cd)) == 0:
        return f"{os.path.basename(url)}{'.html' if '.' not in os.path.basename(url) else ''}"
    fname = re.findall('filename=(.+)', cd)
    return f"{fname[0]}{'.html' if '.' not in fname[0] else ''}"


def connect_to_es(profile, host, region, service):
    session = boto3.Session(region_name=region, profile_name=profile)
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
    
    return Elasticsearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth=aws_auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

def upload_doc(profile, region, filename, username, bucket, s3_key):
    session = boto3.Session(region_name=region, profile_name=profile)
    s3 = session.resource("s3")
    s3_client = boto3.client("s3")
    s3_client.upload_file(filename, bucket, f"{s3_key}/{username}/{filename.split('/')[-1]}")
    

def index_doc(es_index, doc_type, doc, profile, host, region, service):
    es = connect_to_es(profile, host, region, service)
    schema = json.loads(open("document-schema.json").read())  
    # Validate document against schema
    validate(instance=doc, schema=schema)

    if not es.indices.exists(es_index):
        es.indices.create(es_index)
        
    es.index(index=es_index, doc_type=doc_type, id=doc.pop('_id'), body=doc)
    