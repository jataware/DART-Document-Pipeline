# DSMT-Doc-Preparation

## What is this?

This is a Flask Service and CLI that takes a url or raw document bytes and using the extractors below extracts the document raw text.
After extraction the raw text and the metadata are stored in S3 and ElasticSearch based on the index and bucket parameters passed in.

## Document Processing Libraries

#### PDF Documents
    * pdfminer.six
    * pytesseract
    * pdf2image
    * pypdf2
    * tika

#### HTML/Text Documents
    * BeautifulSoup4
    * HTML2Text


## CLI Build Requirements
```
pip install requests-aws4auth==0.9
pip install elasticsearch==7.0.2
pip install boto3==1.9.172
pip install beautifulsoup4==4.5.3
pip install requests
pip install openpyxl
pip install configparser
pip install lxml
pip install libmagic
pip install python-magic
pip install python-magic-bin     # ubuntu: pip install python-magic-debian-bin
pip install Pillow
pip install pytesseract
pip install pdf2image
pip install pdfminer.six
pip install tika==1.19
pip install PyPDF2==1.26.0
pip install jsonschema
pip install html2text

brew install tesseract # ubuntu: sudo apt-get install tesseract-ocr
brew install poppler # ubuntu: sudo apt-get install poppler-utils
```

## Additional API Build Requirements
```
pip install Flask
pip install Flask-Uploads
```

## CLI / Flask Configuration
    We currently store configuration in `config` the schema is as follows:

    ```
        [ES]
        doc_type = [Document schema to be used in ES]
        region = [AWS Region]
        service = [Service (ex: es)]
        host = [Service host URL]
        index = [ES index name]

        [S3]
        base_url = [S3 host URL]
        base_key = [S3 bucket path]
        bucket_name = [S3 bucket name]

        [AWS]
        profile = [AWS profile name (pulled from `~/.aws/config`)]

        [LOGGING]
        fail_file=[Excel spreadsheet filename where failure debug info is stored]
    ```


## Running
```
FLASK_APP=app.py python app.py
```


## Usage

#### `/file_exists`
##### Method: POST

    Description: Checks if the file passed in the URL exists in the ElasticSearch Index
    Request Schema: ```
        {
            index: [string], // ES index name
            url: [string] // File url to check
        }
    ```
    Response Schema: ```
        {
            url: [string], // URL passed in as request param
            exists: [boolean], // true or false based on if the file exists in ES
        }
    ```

#### `/process_raw`
##### Method: POST

    Description: Processes raw file (used when passing file bytes in as opposed to a remote file url) and saves the data/metadata to the requested ElasticSearch index and saves the file to the S3 bucket
    Request Schema: ```
        {
            index: [string], // ES index name
            user_file: [file], // Raw file passed in
            username: [string], // Username to be used as part of file path in S3 (`s3_key/username/filename`)
        }
    ```
    Response Schema: ```
        {
            process_status: [string], // success or failed
            filename: [string], // Name of file uploaded
            index: [string], // Name of ES index where doc exists
        }
    ```

#### `/process_remote`
##### Method: POST

    Description: Processes remote file and saves the data/metadata to the requested ElasticSearch index and saves the file to the S3 bucket
    Request Schema: ```
        {
            index: [string], // ES index name
            url: [file], // URL of file uploaded
            username: [string], // Username to be used as part of file path in S3 (`s3_key/username/filename`)
        }
    ```
    Response Schema: ```
        {
            process_status: [string], // success or failed
            url: [string], // URL of file uploaded
            index: [string], // Name of ES index where doc exists
        }
    ```

## Command-line Script(s)

### `scripts/DSMT_Doc_Prep.py`

#### See above for PIP dependencies

Processed documents in specific sheets within Excel spreadsheet (used individual thread for each extraction library). In order to run edit script:

```
SPREADSHEET = 'Six_Twelve-Month_November_2019_Evaluation_Documents-Updated-6June2019.xlsx'
SHEET_NAMES = [
    'Six-Month Evaluation Documents',
    'Additional Six-Month Eval Docs',
    'Twelve-Month Eval Docs',
    'November 2019 SSudan Docs',
    'November 2019 Ethiopia Docs',
    'Luma-Provided Ethiopia Docs'
]
```


## Jupyter Notebook(s)

#### `notebooks/Document-Validity-Testing.ipynb`

Validates documents in S3 and ElasticSearch...


## Sample Code

#### `pip install requests`

```
import requests

payload = {
    'index': 'test_index',
    'url': 'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf'
    'username': 'clarence'
}

r = requests.post("http://localhost:5000/process_remote", data=payload)
print r.content
```
