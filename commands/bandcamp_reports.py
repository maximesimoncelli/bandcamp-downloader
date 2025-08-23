#!/usr/bin/python3

import argparse
import csv
import glob
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime
import calendar
import sqlite3
import shutil

# These require pip installs
import requests
from requests import HTTPError
import browser_cookie3
from tqdm import tqdm
from reportlab.platypus import TableStyle

from commands.bandcamp_mails import consolidate_csv_files, get_cookies
from utils.logger import logger

USER_URL = 'https://{}.bandcamp.com/net_revenue_report.csv?id={}&begin={}&end={}&items=&region=world'
FILENAME_REGEX = re.compile('filename\\*=UTF-8\'\'(.*)')
WINDOWS_DRIVE_REGEX = re.compile(r'[a-zA-Z]:\\')
SANATIZE_PATH_WINDOWS_REGEX = re.compile(r'[<>:"/|?*]')
CONFIG = {
    'VERBOSE': False,
    'OUTPUT_DIR': None,
    'BROWSER': None,
    'FORCE': False,
    'TQDM': None,
    'MAX_URL_ATTEMPTS': 5,
    'URL_RETRY_WAIT': 5,
    'POST_DOWNLOAD_WAIT': 1,
    'DATE_BEGIN': '2025-07-01',  # Default begin date
    'DATE_END': '2025-07-31',    # Default end date
}
SUPPORTED_BROWSERS = [
    'firefox',
    'chrome',
    'chromium',
    'brave',
    'opera',
    'edge'
]


def main(config=None) -> int:
    parser = argparse.ArgumentParser(
        description='Download revenue reports CSV files from specified bandcamp artists. Requires a logged in session in a supported browser so that the browser cookies can be used to authenticate with bandcamp.')
    parser.add_argument(
        '--browser', '-b',
        type=str,
        default='firefox',
        choices=SUPPORTED_BROWSERS,
        help='The browser whose cookies to use for accessing bandcamp. Defaults to "firefox"'
    )
    parser.add_argument(
        '--directory', '-d',
        default=os.path.join(os.getcwd(), 'outputs'),
        help='The directory to download CSV files to. Defaults to the current directory.'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        default=False,
        help='Always re-download existing files, even if they already exist.',
    )
    parser.add_argument(
        '--wait-after-download',
        type=float,
        default=1,
        help='How long, in seconds, to wait after successfully completing a download before downloading the next file. Defaults to \'1\'.',
    )
    parser.add_argument(
        '--max-download-attempts',
        type=int,
        default=5,
        help='How many times to try downloading any individual files before giving up on it. Defaults to \'5\'.',
    )
    parser.add_argument(
        '--retry-wait',
        type=float,
        default=5,
        help='How long, in seconds, to wait before trying to download a file again after a failure. Defaults to \'5\'.',
    )
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument(
        '--consolidate-only',
        action='store_true',
        default=False,
        help='Skip downloads and only consolidate existing CSV files in the directory.',
    )
    parser.add_argument(
        '--begin-date',
        type=str,
        default=None,
        help='Begin date for revenue reports in YYYY-MM-DD format. Defaults to first day of current month.',
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='End date for revenue reports in YYYY-MM-DD format. Defaults to last day of current month.',
    )
    args = parser.parse_args()

    CONFIG['VERBOSE'] = args.verbose
    CONFIG['OUTPUT_DIR'] = os.path.normcase(args.directory)
    CONFIG['BROWSER'] = args.browser
    CONFIG['FORCE'] = args.force

    # Set DATE_BEGIN and DATE_END to the first and last day of the current month in YYYY-MM-DD format
    today = datetime.today()
    first_day = datetime(today.year, today.month, 1)
    last_day = datetime(today.year, today.month,
                        calendar.monthrange(today.year, today.month)[1])

    # Use command line args if provided, otherwise use current month defaults
    CONFIG['DATE_BEGIN'] = args.begin_date or first_day.strftime('%Y-%m-%d')
    CONFIG['DATE_END'] = args.end_date or last_day.strftime('%Y-%m-%d')

    if config:
        CONFIG['OUTPUT_DIR'] = config.get('directory', CONFIG['OUTPUT_DIR'])
        CONFIG['FORCE'] = config.get('force', CONFIG['FORCE'])
        CONFIG['VERBOSE'] = config.get('verbose', CONFIG['VERBOSE'])
        CONFIG['CONSOLIDATE_ONLY'] = config.get('consolidate_only', False)
        CONFIG['BROWSER'] = config.get('browser', CONFIG['BROWSER'])

        # Handle date config from web interface
        if 'date_begin' in config:
            CONFIG['DATE_BEGIN'] = config.get('date_begin')
        if 'date_end' in config:
            CONFIG['DATE_END'] = config.get('date_end')

    if args.wait_after_download < 0:
        parser.error('--wait-after-download must be at least 0.')
    if args.max_download_attempts < 1:
        parser.error('--max-download-attempts  must be at least 1.')
    if args.retry_wait < 0:
        parser.error('--retry-wait must be at least 0.')
    CONFIG['POST_DOWNLOAD_WAIT'] = args.wait_after_download
    CONFIG['MAX_URL_ATTEMPTS'] = args.max_download_attempts
    CONFIG['URL_RETRY_WAIT'] = args.retry_wait

    # Initialize TQDM
    CONFIG['TQDM'] = tqdm(total=0, unit='files',
                          disable=CONFIG['VERBOSE'] == 0)
    if CONFIG['FORCE']:
        logger(
            'WARNING: --force flag set, existing files will be overwritten.', "WARNING")

    # Clear outputs directory before starting
    clear_outputs_directory()

    if CONFIG.get('CONSOLIDATE_ONLY', args.consolidate_only):
        logger('Consolidating existing CSV files...', "INFO")
        return consolidate_csv_files()
    else:
        try:
            from artists import artists

            CONFIG['TQDM'].total = len(artists)

            for artist in artists:
                download_file(USER_URL.format(
                    artist[0], artist[1], CONFIG['DATE_BEGIN'], CONFIG['DATE_END']), 'revenues')
                CONFIG['TQDM'].update(1)

            CONFIG['TQDM'].close()
            logger(
                'Download complete. Analyzing and consolidating CSV files...', "INFO")
            return consolidate_csv_files()

        except (ImportError, UnboundLocalError):
            CONFIG['TQDM'].close()
            logger('Please add your artists in artists.py', "ERROR")
            return None


def download_file(_url: str, _to: str = '', _attempt: int = 1) -> None:
    try:
        with requests.get(
                _url,
                cookies=get_cookies(),
        ) as response:
            response.raise_for_status()

            filename_match = FILENAME_REGEX.search(
                response.headers['content-disposition'])
            filename = urllib.parse.unquote(filename_match.group(
                1)) if filename_match else _url.split('/')[-1]
            file_path = os.path.join(CONFIG['OUTPUT_DIR'], _to, filename)

            # Remove not allowed path characters
            file_path = sanitize_path(file_path)

            if os.path.exists(file_path):
                if CONFIG['FORCE']:
                    if CONFIG['VERBOSE']:
                        CONFIG['TQDM'].write(
                            '--force flag was given. Overwriting existing file at [{}].'.format(file_path))
                else:
                    # For text files, compare content length instead of just file size
                    # since encoding might affect byte count
                    try:
                        with open(file_path, 'r', encoding='utf-8') as existing_file:
                            existing_content = existing_file.read()
                            if len(existing_content) > 0:  # File has content, assume it's valid
                                if CONFIG['VERBOSE'] >= 3:
                                    CONFIG['TQDM'].write(
                                        'Skipping file that already exists: [{}]'.format(file_path))
                                return
                    except (UnicodeDecodeError, IOError):
                        # If we can't read the existing file, re-download it
                        if CONFIG['VERBOSE'] >= 2:
                            CONFIG['TQDM'].write(
                                'Existing file at [{}] is corrupted or unreadable. Re-downloading.'.format(file_path))

            if CONFIG['VERBOSE'] >= 2:
                CONFIG['TQDM'].write(
                    'File being saved to [{}]'.format(file_path))
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # Download content as text and save as proper CSV
            content = response.text
            with open(file_path, 'w', newline='', encoding='utf-8') as fh:
                fh.write(content)
    except HTTPError as e:
        if _attempt < CONFIG['MAX_URL_ATTEMPTS']:
            if CONFIG['VERBOSE'] >= 2:
                CONFIG['TQDM'].write(
                    'WARN: HTTP Error on attempt # [{}] to download the file at [{}]. Trying again...'.format(_attempt, _url))
            time.sleep(CONFIG['URL_RETRY_WAIT'])
            download_file(_url, _to, _attempt + 1)
        else:
            print_exception(
                e, 'An exception occurred trying to download file url [{}]:'.format(_url))
    except Exception as e:
        print_exception(
            e, 'An exception occurred trying to download file url [{}]:'.format(_url))


def print_exception(_e: Exception, _msg: str = '') -> None:
    CONFIG['TQDM'].write('\nERROR: {}'.format(_msg))
    CONFIG['TQDM'].write(str(_e))
    CONFIG['TQDM'].write(str(sys.exc_info()))
    CONFIG['TQDM'].write('\n')


# Windows has some picky requirements about file names
# So let's replace known bad characters with '-'
def sanitize_path(_path: str) -> str:
    if sys.platform.startswith('win'):
        # Ok, we need to leave on the ':' if it is like 'D:\'
        # otherwise, we need to remove it.
        new_path = ''
        search_path = _path
        if WINDOWS_DRIVE_REGEX.match(_path):
            new_path += _path[0:3]
            search_path = _path[3:]
        new_path += SANATIZE_PATH_WINDOWS_REGEX.sub('-', search_path)
        return new_path
    else:
        return _path


def clear_outputs_directory() -> None:
    """Clear revenue-related contents of the outputs directory before starting a fresh run."""
    outputs_dir = CONFIG['OUTPUT_DIR']

    if not os.path.exists(outputs_dir):
        logger(f'Creating outputs directory: {outputs_dir}', "INFO")
        os.makedirs(outputs_dir, exist_ok=True)
        return

    logger(
        f'Clearing revenue-related files from outputs directory: {outputs_dir}', "INFO")

    # Directories to clear for revenue reports
    revenue_dirs = ['revenues', 'reports']

    for dir_name in revenue_dirs:
        dir_path = os.path.join(outputs_dir, dir_name)
        if os.path.exists(dir_path):
            logger(f'  Clearing {dir_name}/ directory', "INFO")
            try:
                shutil.rmtree(dir_path)
                if CONFIG['VERBOSE'] >= 2:
                    logger(f'    Removed directory: {dir_name}/', "INFO")
            except Exception as e:
                logger(
                    f'WARNING: Could not remove {dir_name}/ directory: {e}', "WARNING")

    # Also remove any revenue-related files in the root outputs directory
    if os.path.exists(outputs_dir):
        for item in os.listdir(outputs_dir):
            item_path = os.path.join(outputs_dir, item)
            # Remove files that contain revenue-related keywords
            if os.path.isfile(item_path) and any(keyword in item.lower() for keyword in ['revenue', 'report', 'bandcamp_revenue']):
                try:
                    os.remove(item_path)
                    if CONFIG['VERBOSE'] >= 2:
                        logger(f'    Removed revenue file: {item}', "INFO")
                except Exception as e:
                    logger(
                        f'WARNING: Could not remove revenue file {item}: {e}', "WARNING")

    logger('Revenue-related files cleared successfully', "INFO")


def create_database_schema(db_path: str) -> None:
    """Create the SQLite database with the appropriate schema for revenue data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table with all the revenue report columns
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS revenue_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cat_no TEXT,
            upc TEXT,
            isrc TEXT,
            sku TEXT,
            item_type TEXT,
            item_name TEXT,
            container_name TEXT,
            package TEXT,
            artist_name TEXT,
            label_name TEXT,
            region TEXT,
            quantity INTEGER,
            currency TEXT,
            gross_revenue REAL,
            shipping REAL,
            taxes REAL,
            bandcamp_assessed_revenue_share REAL,
            collection_society_share REAL,
            payment_processor_fees REAL,
            net_revenue REAL,
            url TEXT,
            transaction_date_from TEXT,
            transaction_date_to TEXT,
            import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            date_range_begin TEXT,
            date_range_end TEXT,
            source_file TEXT
        )
    ''')

    # Create indexes for common queries
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_artist_name ON revenue_reports(artist_name)')
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_item_name ON revenue_reports(item_name)')
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_region ON revenue_reports(region)')
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_transaction_dates ON revenue_reports(transaction_date_from, transaction_date_to)')
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_date_range ON revenue_reports(date_range_begin, date_range_end)')

    conn.commit()
    conn.close()


def insert_revenue_data(db_path: str, data_rows: list, date_begin: str, date_end: str, source_file: str) -> int:
    """Insert revenue data rows into the SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    inserted_count = 0

    for row in data_rows:
        try:
            # Ensure we have the right number of columns (pad with None if necessary)
            while len(row) < 23:  # 23 columns in full_expected_headers
                row.append(None)

            # Convert numeric fields
            quantity = int(row[11]) if row[11] and row[11].strip() else 0
            gross_revenue = float(
                row[13]) if row[13] and row[13].strip() else 0.0
            shipping = float(row[14]) if row[14] and row[14].strip() else 0.0
            taxes = float(row[15]) if row[15] and row[15].strip() else 0.0
            bandcamp_share = float(
                row[16]) if row[16] and row[16].strip() else 0.0
            collection_share = float(
                row[17]) if row[17] and row[17].strip() else 0.0
            processor_fees = float(
                row[18]) if row[18] and row[18].strip() else 0.0
            net_revenue = float(
                row[19]) if row[19] and row[19].strip() else 0.0

            cursor.execute('''
                INSERT INTO revenue_reports (
                    cat_no, upc, isrc, sku, item_type, item_name, container_name,
                    package, artist_name, label_name, region, quantity, currency,
                    gross_revenue, shipping, taxes, bandcamp_assessed_revenue_share,
                    collection_society_share, payment_processor_fees, net_revenue,
                    url, transaction_date_from, transaction_date_to,
                    date_range_begin, date_range_end, source_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                row[7], row[8], row[9], row[10], quantity, row[12],
                gross_revenue, shipping, taxes, bandcamp_share,
                collection_share, processor_fees, net_revenue,
                row[20], row[21], row[22],
                date_begin, date_end, source_file
            ))
            inserted_count += 1

        except (ValueError, TypeError, sqlite3.Error) as e:
            if CONFIG['VERBOSE'] >= 2:
                logger(
                    f'  WARNING: Failed to insert row from {source_file}: {e}', "WARNING")
            continue

    conn.commit()
    conn.close()
    return inserted_count


def consolidate_csv_files() -> None:
    """Consolidate all CSV files in the revenues folder into a single file in the reports folder."""
    # Define the full expected column headers from Bandcamp
    full_expected_headers = [
        'Cat no.', 'UPC', 'ISRC', 'SKU', 'Item type', 'Item name', 'Container name',
        'Package', 'Artist name', 'Label name', 'Region', 'Quantity', 'Currency',
        'Gross revenue', 'Shipping', 'Taxes', 'Bandcamp assessed revenue share',
        'Collection society share', 'Payment processor fees', 'Net revenue', 'URL',
        'Transaction date from', 'Transaction date to'
    ]

    # Define columns to exclude
    columns_to_exclude = ['UPC', 'ISRC', 'SKU', 'Collection society share',
                          'Container name', 'URL', 'Transaction date from', 'Transaction date to']

    # Create filtered headers (columns we want to keep)
    expected_headers = [
        col for col in full_expected_headers if col not in columns_to_exclude]

    # Create mapping from full header indices to filtered indices
    column_mapping = {}
    for i, col in enumerate(full_expected_headers):
        if col not in columns_to_exclude:
            column_mapping[i] = expected_headers.index(col)

    # Find all CSV files in the revenues folder, excluding previous consolidated files
    revenues_dir = os.path.join(CONFIG['OUTPUT_DIR'], 'revenues')
    csv_pattern = os.path.join(revenues_dir, '*.csv')
    all_csv_files = glob.glob(csv_pattern)
    # Filter out previous consolidated files (both old timestamp format and new year-month format)
    csv_files = [f for f in all_csv_files if not os.path.basename(
        f).startswith('consolidated_bandcamp_reports_')]

    if not csv_files:
        logger('No CSV files found in the output directory.', "INFO")
        return None

    # Generate output filename with year-month (reports are monthly scoped)
    year_month = datetime.now().strftime('%Y-%m')
    consolidated_filename = 'consolidated_bandcamp_reports_{}.csv'.format(
        year_month)
    reports_dir = os.path.join(CONFIG['OUTPUT_DIR'], 'reports')
    consolidated_path = os.path.join(reports_dir, consolidated_filename)
    consolidated_path = sanitize_path(consolidated_path)

    total_rows = 0
    files_processed = 0

    if CONFIG['VERBOSE'] >= 1:
        logger('Found {} CSV files to consolidate:'.format(
            len(csv_files)), "INFO")
        for csv_file in csv_files:
            logger('  - {}'.format(os.path.basename(csv_file)), "INFO")

    try:
        # Create reports directory if it doesn't exist
        os.makedirs(reports_dir, exist_ok=True)

        # Setup SQLite database
        db_filename = 'bandcamp_revenue_reports_{}.db'.format(year_month)
        db_path = os.path.join(reports_dir, db_filename)
        db_path = sanitize_path(db_path)

        logger('Setting up SQLite database: {}'.format(
            os.path.basename(db_path)), "INFO")
        logger('DEBUG: About to create database schema...', "DEBUG")
        create_database_schema(db_path)
        logger('DEBUG: Database schema created successfully', "DEBUG")

        total_db_rows = 0

        with open(consolidated_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)

            # Write the header once
            writer.writerow(expected_headers)

            for csv_file in csv_files:
                if CONFIG['VERBOSE'] >= 2:
                    logger('Processing: {}'.format(
                        os.path.basename(csv_file)), "DEBUG")

                try:
                    # Try different encodings to handle CSV files with BOM or different encodings
                    encodings_to_try = ['utf-8-sig',
                                        'utf-8', 'latin-1', 'cp1252']
                    infile = None

                    for encoding in encodings_to_try:
                        try:
                            infile = open(
                                csv_file, 'r', newline='', encoding=encoding)
                            # Test if we can read the first line
                            pos = infile.tell()
                            infile.readline()
                            infile.seek(pos)
                            break
                        except UnicodeDecodeError:
                            if infile:
                                infile.close()
                            continue

                    if not infile:
                        raise Exception(
                            'Unable to decode file with any supported encoding')

                    try:
                        reader = csv.reader(infile)

                        # Read and validate header
                        try:
                            header = next(reader)
                        except StopIteration:
                            if CONFIG['VERBOSE'] >= 1:
                                logger('WARNING: Empty file skipped: {}'.format(
                                    os.path.basename(csv_file)), "WARNING")
                            infile.close()
                            continue

                        # Check if header matches expected full format (before filtering)
                        if header != full_expected_headers:
                            if CONFIG['VERBOSE'] >= 1:
                                logger('WARNING: Header mismatch in {}, attempting to process anyway'.format(
                                    os.path.basename(csv_file)), "WARNING")
                                if CONFIG['VERBOSE'] >= 3:
                                    logger('  Expected: {}'.format(
                                        full_expected_headers), "DEBUG")
                                    logger('  Found: {}'.format(
                                        header), "DEBUG")

                        # Copy all data rows, filtering to keep only desired columns
                        file_rows = 0
                        db_rows = []  # Collect rows for database insertion

                        for row in reader:
                            if any(cell.strip() for cell in row):  # Skip completely empty rows
                                # Ensure row has the correct number of columns (full format)
                                while len(row) < len(full_expected_headers):
                                    # Pad with empty strings if needed
                                    row.append('')
                                if len(row) > len(full_expected_headers):
                                    # Truncate if too long
                                    row = row[:len(full_expected_headers)]

                                # Store full row for database (before filtering)
                                db_rows.append(row[:])

                                # Filter row to only include desired columns for CSV
                                filtered_row = []
                                for i, cell in enumerate(row):
                                    if i < len(full_expected_headers) and full_expected_headers[i] not in columns_to_exclude:
                                        filtered_row.append(cell)

                                writer.writerow(filtered_row)
                                file_rows += 1
                                total_rows += 1

                        # Insert data into database
                        if db_rows:
                            logger(
                                f'DEBUG: About to insert {len(db_rows)} rows into database', "DEBUG")
                            db_inserted = insert_revenue_data(
                                db_path, db_rows,
                                CONFIG['DATE_BEGIN'], CONFIG['DATE_END'],
                                os.path.basename(csv_file)
                            )
                            total_db_rows += db_inserted
                            if CONFIG['VERBOSE'] >= 2:
                                logger('  Inserted {} rows into database from {}'.format(
                                    db_inserted, os.path.basename(csv_file)), "INFO")
                            logger(
                                f'DEBUG: Successfully inserted {db_inserted} rows', "DEBUG")
                        else:
                            logger('DEBUG: No db_rows to insert', "DEBUG")

                        if CONFIG['VERBOSE'] >= 2:
                            logger('  Added {} rows from {}'.format(
                                file_rows, os.path.basename(csv_file)), "INFO")

                        files_processed += 1
                    finally:
                        if infile:
                            infile.close()

                except Exception as e:
                    logger('ERROR: Failed to process {}: {}'.format(
                        os.path.basename(csv_file), str(e)), "ERROR")
                    continue

    except Exception as e:
        logger('ERROR: Failed to create consolidated file: {}'.format(str(e)), "ERROR")
        return

    logger('Consolidation complete!')
    logger('  Files processed: {} of {}'.format(
        files_processed, len(csv_files)))
    logger('  Total data rows: {}'.format(total_rows))
    logger('  Output file: {}'.format(os.path.basename(consolidated_filename)))
    logger('  Database: {}'.format(os.path.basename(db_path)))
    logger('  Database rows inserted: {}'.format(total_db_rows))

    return consolidated_path


def get_cookies():
    if CONFIG['BROWSER'] == 'firefox':
        return browser_cookie3.firefox(domain_name='bandcamp.com')
    elif CONFIG['BROWSER'] == 'chrome':
        return browser_cookie3.chrome(domain_name='bandcamp.com')
    elif CONFIG['BROWSER'] == 'brave':
        return browser_cookie3.brave(domain_name='bandcamp.com')
    elif CONFIG['BROWSER'] == 'edge':
        return browser_cookie3.edge(domain_name='bandcamp.com')
    elif CONFIG['BROWSER'] == 'chromium':
        return browser_cookie3.chromium(domain_name='bandcamp.com')
    elif CONFIG['BROWSER'] == 'opera':
        return browser_cookie3.opera(domain_name='bandcamp.com')
    else:
        raise Exception('Browser type if [{}] is unknown. Can\'t pull cookies, so can\'t authenticate with bandcamp.'.format(
            CONFIG['BROWSER']))


if __name__ == '__main__':
    sys.exit(main())
