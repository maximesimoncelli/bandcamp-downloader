# Makefile for bandcamp-downloader

.PHONY: install run clean extract-mails extract-reports extract-zips

install:
	pip install -r requirements.txt

run:
	python app.py

extract-mails:
	python commands/bandcamp_mails.py

extract-reports:
	python commands/bandcamp_reports.py

extract-zips:
	python commands/extractor.py

clean:
	rm -rf __pycache__ outputs/*/*.csv
