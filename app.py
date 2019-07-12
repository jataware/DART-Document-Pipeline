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
from utils import *

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

S3_KEY = f"{S3_BASE_KEY}{TEMP_DOWNLOAD_PATH}"
S3_URI = f"{S3_BASE_URL}/{S3_KEY}"


@app.route("/process_raw", methods=['POST'])
def process_raw():
    es_index = request.form['index']
    filename = DOCS.save(request.files['user_file'])
    upload_doc(AWS_PROFILE, REGION, filename, request.form['username'], BUCKET_NAME, S3_KEY)
    
    title = filename
    category = 'Migration'
    source_url = filename
    creation_date = ''
    doc = parse_document(filename, category, source_url)
    doc['stored_url'] = S3_URI
    
    index_doc(es_index, DOC_TYPE, doc, AWS_PROFILE, ES_HOST, REGION, SERVICE)
    return jsonify(
        process_status='success',
        filename=filename,
        index=es_index
    )

@app.route("/process_remote", methods=['POST'])
def process_remote():
    es_index = request.form['index']
    url = request.form['url']
    try:
        r = requests.get(url, verify=False, stream=True, allow_redirects=True)
        r.raw.decode_content = True
        filename = f"{TEMP_DOWNLOAD_PATH}/{get_filename(r.headers.get('content-disposition'), url)}"
        open(filename, 'wb').write(r.content)
    except Exception as e:
        print(f"Error Processing {url} - {e}")

    upload_doc(AWS_PROFILE, REGION, filename, request.form['username'], BUCKET_NAME, S3_KEY)
    
    title = url
    category = 'Migration'
    source_url = url
    creation_date = ''
    doc = parse_document(filename, category, source_url)
    doc['stored_url'] = S3_URI
    
    index_doc(es_index, DOC_TYPE, doc, AWS_PROFILE, ES_HOST, REGION, SERVICE)
    
    return jsonify(
        process_status='success',
        filename=filename,
        url=url,
        index=es_index
    )

if __name__ == '__main__':
    app.run(debug=True)