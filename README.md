# DSMT-Doc-Preparation

## What it does

This is a Flask Service and CLI that takes a url or raw document bytes and using the extractors below extracts the document raw text.
After extraction the raw text and the metadata are stored in S3 and ElasticSearch based on the index and bucket parameters passed in.


## Installing
```
pip install -r requirements.txt
```

### Additional OSX Installation Steps
```
brew install tesseract
brew install poppler
```

### Additional Ubuntu Installation Steps
```
sudo apt-get install tesseract-ocr
sudo apt-get install poppler-utils
```


## Configuration
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
