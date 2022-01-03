#!/bin/sh

echo 'STARTING...'

cd UltaScraper

echo 'CRAWLING ULTA...'
python3 crawl.py

cd ../

echo 'UPDATING ULTA DATABASE...'
python3 update_db.py

echo 'UPDATING GOOGLE SHEET...'
python3 update_googlesheets.py

echo 'DONE!'