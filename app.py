from flask import Flask
from flask import request
from flask import jsonify
from flask_uploads import UploadSet, configure_uploads, DOCUMENTS
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
import configparser

app = Flask(__name__)

warnings.filterwarnings("ignore", category=PyPDF2.utils.PdfReadWarning)

TEMP_DOWNLOAD_PATH = '/tmp'

# CONFIG

config = configparser.ConfigParser()
config.readfp(open(r'config'))

# UPLOADS

DOCS = UploadSet('datafiles', DOCUMENTS)
app.config['UPLOADED_DATAFILES_DEST'] = 'tmp'
configure_uploads(app, DOCS)

# AWS CONFIG
AWS_PROFILE = config.get('AWS', 'profile')

# S3 CONFIG
S3_BASE_URL = config.get('S3', 'base_url')
S3_BASE_KEY = config.get('S3', 'base_key')
BUCKET_NAME = config.get('S3', 'bucket_name')

# ELASTIC SEARCH CONFIG
DOC_TYPE = config.get('ES', 'doc_type')
REGION = config.get('ES', 'region')
SERVICE = config.get('ES', 'service')
ES_HOST = config.get('ES', 'host')

s3_key = f"{S3_BASE_KEY}{TEMP_DOWNLOAD_PATH}"
s3_uri = f"{S3_BASE_URL}/{s3_key}"

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

def get_filename(cd, url):
    """
    Get filename from content-disposition
    """
    if not cd:
        return os.path.basename(url)
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return os.path.basename(url)
    return fname[0]


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


def index_doc(es_index, doc_type, doc):
    es = connect_to_es()
    schema = json.loads(open("document-schema.json").read())  
    # Validate document against schema
    validate(instance=doc, schema=schema)

    if not es.indices.exists(es_index):
        es.indices.create(es_index)
        
    es.index(index=es_index, doc_type=DOC_TYPE, id=doc.pop('_id'), body=doc)


@app.route("/process_raw", methods=['POST'])
def process_raw():
    s3_client = connect_to_s3()
    es_index = request.form['index']
    filename = DOCS.save(request.files['user_file'])

    s3_client.upload_file(filename, BUCKET_NAME, s3_key)
    
    title = filename
    category = 'Migration'
    source_url = filename
    creation_date = ''
    doc = parse_document(filename, category, source_url)
    doc['stored_url'] = s3_uri
    
    index_doc(es_index, DOC_TYPE, doc)
    return jsonify(
        process_status='success',
        filename=filename,
        index=es_index
    )

@app.route("/process_remote", methods=['POST'])
def process_remote():
    return jsonify(
        request=request.form
    )
    s3_client = connect_to_s3()
    es_index = request.form['index']
    url = request.form['url']
    try:
        r = requests.get(url, verify=False, stream=True, allow_redirects=True)
        r.raw.decode_content = True
        filename = f".{TEMP_DOWNLOAD_PATH}/{get_filename(r.headers.get('content-disposition'), url)}"
        open(filename, 'wb').write(r.content)
    except Exception as e:
        print(f"Error Processing {url} - {e}")

    s3_client.upload_file(filename, BUCKET_NAME, s3_key)
    
    title = url
    category = 'Migration'
    source_url = url
    creation_date = ''
    doc = parse_document(filename, category, source_url)
    doc['stored_url'] = s3_uri
    
    index_doc(es_index, DOC_TYPE, doc)
    
    return jsonify(
        process_status='success',
        filename=filename,
        url=url,
        index=es_index
    )

if __name__ == '__main__':
    app.run(debug=True)