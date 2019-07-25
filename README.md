# DSMT-Doc-Preparation

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


## Running . 
```
FLASK_APP=app.py python app.py
```
