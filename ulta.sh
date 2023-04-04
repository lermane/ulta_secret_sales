#!/bin/sh

echo 'STARTING...'

source "../../environments/ulta_env/bin/activate"

cd UltaScraper

echo 'CRAWLING ULTA...'
python3 crawl.py

cd ../

echo 'UPDATING ULTA DATABASE...'
python3 update_db.py

echo 'UPDATING GOOGLE SHEET...'
python3 update_googlesheets.py

echo 'DONE!'
