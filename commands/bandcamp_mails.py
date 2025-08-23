#!/usr/bin/python3

import argparse
import csv
import glob
import os
import re
import sys
import time
import urllib.parse
import shutil
from datetime import datetime
from typing import Dict, List, Any, Tuple
import sqlite3
from utils.logger import logger

# These require pip installs
import requests
from requests import HTTPError
import browser_cookie3
from tqdm import tqdm
from reportlab.lib import colors
from reportlab.platypus import TableStyle

# --- Global Constants & Configuration ---

USER_URL = 'https://{}.bandcamp.com/mailing_list.csv?id={}'
FILENAME_REGEX = re.compile('filename\\*=UTF-8\'\'(.*)')
WINDOWS_DRIVE_REGEX = re.compile(r'[a-zA-Z]:\\')
SANATIZE_PATH_WINDOWS_REGEX = re.compile(r'[<>:"/|?*]')

CONFIG = {
    'VERBOSE': 0,
    'OUTPUT_DIR': None,
    'BROWSER': 'firefox',
    'FORCE': False,
    'TQDM': None,
    'MAX_URL_ATTEMPTS': 5,
    'URL_RETRY_WAIT': 5,
    'POST_DOWNLOAD_WAIT': 1,
}

SUPPORTED_BROWSERS = [
    'firefox',
    'chrome',
    'chromium',
    'brave',
    'opera',
    'edge'
]

# --- Core Functions ---


def main(config=None) -> int:
    """Parses arguments, orchestrates download and consolidation of CSV files."""
    parser = argparse.ArgumentParser(
        description='Download and consolidate mailing list CSV files from Bandcamp.')
    parser.add_argument(
        '--browser', '-b',
        type=str,
        default='firefox',
        choices=SUPPORTED_BROWSERS,
        help='The browser whose cookies to use for authentication. Defaults to "firefox".'
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
        help='Always re-download existing files, overwriting them.',
    )
    parser.add_argument(
        '--wait-after-download',
        type=float,
        default=1,
        help='How long, in seconds, to wait after a successful download. Defaults to \'1\'.',
    )
    parser.add_argument(
        '--max-download-attempts',
        type=int,
        default=5,
        help='How many times to try downloading a file before giving up. Defaults to \'5\'.',
    )
    parser.add_argument(
        '--retry-wait',
        type=float,
        default=5,
        help='How long, in seconds, to wait before retrying a failed download. Defaults to \'5\'.',
    )
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument(
        '--consolidate-only',
        action='store_true',
        default=False,
        help='Skip downloads and only consolidate existing CSV files in the directory.',
    )
    args = parser.parse_args()

    # Apply configuration from command-line arguments
    CONFIG['VERBOSE'] = args.verbose
    CONFIG['OUTPUT_DIR'] = os.path.normcase(args.directory)
    CONFIG['BROWSER'] = args.browser
    CONFIG['FORCE'] = args.force
    CONFIG['POST_DOWNLOAD_WAIT'] = args.wait_after_download
    CONFIG['MAX_URL_ATTEMPTS'] = args.max_download_attempts
    CONFIG['URL_RETRY_WAIT'] = args.retry_wait
    CONFIG['CONSOLIDATE_ONLY'] = args.consolidate_only

    if config:
        logger(str(config), "DEBUG")
        CONFIG['OUTPUT_DIR'] = config.get('directory', CONFIG['OUTPUT_DIR'])
        CONFIG['FORCE'] = config.get('force', CONFIG['FORCE'])
        CONFIG['VERBOSE'] = config.get('verbose', CONFIG['VERBOSE'])
        CONFIG['CONSOLIDATE_ONLY'] = config.get('consolidate_only', False)

    if CONFIG['FORCE']:
        logger(
            'WARNING: --force flag set, existing files will be overwritten.', "WARNING")

    # Clear outputs directory before starting
    clear_outputs_directory()

    # Initialize TQDM progress bar
    CONFIG['TQDM'] = tqdm(total=0, unit='files',
                          disable=CONFIG['VERBOSE'] == 0)

    if CONFIG['CONSOLIDATE_ONLY']:
        logger('Consolidating existing CSV files...', "INFO")
        return consolidate_csv_files()
    else:
        try:
            from artists import artists
            CONFIG['TQDM'].total = len(artists)

            for artist in artists:
                url = USER_URL.format(artist[0], artist[1])
                download_file(url, artist[0], 'mails')
                CONFIG['TQDM'].update(1)

            CONFIG['TQDM'].close()
            logger(
                'Download complete. Analyzing and consolidating CSV files...', "INFO")
            return consolidate_csv_files()

        except (ImportError, UnboundLocalError):
            CONFIG['TQDM'].close()
            logger('Please add your artists in artists.py', "ERROR")

        finally:
            logger('Done', "INFO")


def download_file(_url: str, artist: str, _to: str = '', _attempt: int = 1) -> None:
    """Downloads a file from a URL with retries."""
    try:
        with requests.get(_url, cookies=get_cookies()) as response:
            response.raise_for_status()

            # Extract filename from Content-Disposition header
            filename_match = FILENAME_REGEX.search(
                response.headers['content-disposition'])
            filename, extension = urllib.parse.unquote(
                filename_match.group(1)).split('.')
            computed_file_name = f'{filename}-{artist}.{extension}'
            file_path = os.path.join(
                CONFIG['OUTPUT_DIR'], _to, computed_file_name)
            sanitized_file_path = sanitize_path(file_path)

            if os.path.exists(sanitized_file_path) and not CONFIG['FORCE']:
                if CONFIG['VERBOSE'] >= 3:
                    CONFIG['TQDM'].write(
                        f'Skipping file that already exists: [{sanitized_file_path}]')
                return

            os.makedirs(os.path.dirname(sanitized_file_path), exist_ok=True)

            if CONFIG['VERBOSE'] >= 2:
                CONFIG['TQDM'].write(
                    f'File being saved to [{sanitized_file_path}]')

            # Download content and save as proper CSV
            content = response.text
            with open(sanitized_file_path, 'w', newline='', encoding='utf-8') as fh:
                fh.write(content)

            time.sleep(CONFIG['POST_DOWNLOAD_WAIT'])

    except HTTPError as e:
        if _attempt < CONFIG['MAX_URL_ATTEMPTS']:
            if CONFIG['VERBOSE'] >= 2:
                CONFIG['TQDM'].write(
                    f'WARN: HTTP Error on attempt #{_attempt} to download url [{_url}]. Trying again...')
            time.sleep(CONFIG['URL_RETRY_WAIT'])
            download_file(_url, _to, _attempt + 1)
        else:
            print_exception(
                e, f'An exception occurred trying to download file url [{_url}]:')
    except Exception as e:
        print_exception(
            e, f'An exception occurred trying to download file url [{_url}]:')


def print_exception(_e: Exception, _msg: str = '') -> None:
    """Prints an exception message to the console or progress bar."""
    CONFIG['TQDM'].write(f'\nERROR: {_msg}')
    CONFIG['TQDM'].write(str(_e))
    CONFIG['TQDM'].write(str(sys.exc_info()))
    CONFIG['TQDM'].write('\n')


def sanitize_path(_path: str) -> str:
    """Replaces invalid characters in a file path for Windows compatibility."""
    if sys.platform.startswith('win'):
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
    """Clear mailing-related contents of the outputs directory before starting a fresh run."""
    outputs_dir = CONFIG['OUTPUT_DIR']

    if not os.path.exists(outputs_dir):
        logger(f'Creating outputs directory: {outputs_dir}', "INFO")
        os.makedirs(outputs_dir, exist_ok=True)
        return

    logger(
        f'Clearing mailing-related files from outputs directory: {outputs_dir}', "INFO")

    # Directories to clear for mailing lists
    mail_dirs = ['mails']

    for dir_name in mail_dirs:
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

    # Also clear any reports directory files related to mailing lists
    reports_dir = os.path.join(outputs_dir, 'reports')
    if os.path.exists(reports_dir):
        for item in os.listdir(reports_dir):
            item_path = os.path.join(reports_dir, item)
            # Remove files that contain mailing-related keywords
            if os.path.isfile(item_path) and any(keyword in item.lower() for keyword in ['mailing', 'mail', 'consolidated_bandcamp_mailing']):
                try:
                    os.remove(item_path)
                    if CONFIG['VERBOSE'] >= 2:
                        logger(
                            f'    Removed mailing file from reports/: {item}', "INFO")
                except Exception as e:
                    logger(
                        f'WARNING: Could not remove mailing file {item}: {e}', "WARNING")

    # Also remove any mailing-related files in the root outputs directory
    if os.path.exists(outputs_dir):
        for item in os.listdir(outputs_dir):
            item_path = os.path.join(outputs_dir, item)
            # Remove files that contain mailing-related keywords
            if os.path.isfile(item_path) and any(keyword in item.lower() for keyword in ['mailing', 'mail', 'bandcamp_mailing']):
                try:
                    os.remove(item_path)
                    if CONFIG['VERBOSE'] >= 2:
                        logger(f'    Removed mailing file: {item}', "INFO")
                except Exception as e:
                    logger(
                        f'WARNING: Could not remove mailing file {item}: {e}', "WARNING")

    logger('Mailing-related files cleared successfully', "INFO")


def get_cookies():
    """Retrieves cookies from a specified browser for Bandcamp authentication."""
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
        raise Exception(
            f"Browser type '{CONFIG['BROWSER']}' is unknown. Cannot pull cookies.")

# --- Consolidation and Reporting Functions ---


def get_csv_files(directory: str) -> List[str]:
    """Finds all non-consolidated mailing list CSV files in a directory."""
    csv_pattern = os.path.join(directory, 'mailing_list*.csv')
    all_csv_files = glob.glob(csv_pattern)
    return [f for f in all_csv_files if not os.path.basename(f).startswith('consolidated_bandcamp_mailing_list')]


def process_single_csv(file_path: str) -> List[Dict[str, Any]]:
    """Reads a single CSV file, handling different encodings and returning a list of dictionaries."""
    encodings_to_try = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
    records = []

    for encoding in encodings_to_try:
        try:
            with open(file_path, 'r', newline='', encoding=encoding) as infile:
                reader = csv.reader(infile)

                # Check for empty file
                try:
                    header = next(reader)
                except StopIteration:
                    if CONFIG['VERBOSE'] >= 1:
                        logger(
                            f'WARNING: Empty file skipped: {os.path.basename(file_path)}', "WARNING")
                    return []

                for row in reader:
                    if any(cell.strip() for cell in row):
                        records.append(dict(zip(header, row)))
            return records
        except UnicodeDecodeError:
            continue

    raise Exception(
        f'Unable to decode file with any supported encoding: {os.path.basename(file_path)}')


def write_consolidated_csv(file_path: str, headers: List[str], data: Dict[str, Any]) -> None:
    """Writes the consolidated email data to a new CSV file."""
    with open(file_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers)

        # Sort emails by the oldest date added for consistent output
        sorted_emails = sorted(data.items(), key=lambda x: x[1]['parsed_date'])

        for email, record_data in sorted_emails:
            final_row = record_data['record'][:]
            final_row[7] = str(record_data['total_purchases'])
            final_row[0] = email
            writer.writerow(final_row)


def create_mailing_database_schema(db_path: str) -> None:
    """Create the SQLite database with the appropriate schema for mailing list data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table with all the mailing list columns
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mailing_lists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            fullname TEXT,
            firstname TEXT,
            lastname TEXT,
            date_added TEXT,
            country TEXT,
            postal_code TEXT,
            num_purchases INTEGER,
            total_purchases INTEGER,
            import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_file TEXT,
            UNIQUE(email, source_file)
        )
    ''')

    # Create indexes for common queries
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_email ON mailing_lists(email)')
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_country ON mailing_lists(country)')
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_date_added ON mailing_lists(date_added)')
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_purchases ON mailing_lists(num_purchases)')

    conn.commit()
    conn.close()


def insert_mailing_data(db_path: str, email_data: Dict[str, Any], source_files: List[str]) -> int:
    """Insert mailing list data into the SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    inserted_count = 0

    for email, record_data in email_data.items():
        try:
            record = record_data['record']

            # Convert purchases to integer
            num_purchases = int(
                record[7]) if record[7] and record[7].strip() else 0
            total_purchases = record_data['total_purchases']

            # Determine source file (use first file where this email was found)
            # For now, just list all sources
            source_file = ', '.join(source_files)

            cursor.execute('''
                INSERT OR REPLACE INTO mailing_lists (
                    email, fullname, firstname, lastname, date_added,
                    country, postal_code, num_purchases, total_purchases, source_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                email, record[1], record[2], record[3], record[4],
                record[5], record[6], num_purchases, total_purchases, source_file
            ))
            inserted_count += 1

        except (ValueError, TypeError, sqlite3.Error) as e:
            if CONFIG['VERBOSE'] >= 2:
                print(f'  WARNING: Failed to insert email {email}: {e}')
            continue

    conn.commit()
    conn.close()
    return inserted_count


def consolidate_csv_files() -> None:
    """Consolidates all mailing list CSV files into a single file."""
    full_expected_headers = ['email', 'fullname', 'firstname',
                             'lastname', 'date added', 'country', 'postal code', 'num purchases']

    mails_dir = os.path.join(CONFIG['OUTPUT_DIR'], 'mails')
    reports_dir = os.path.join(CONFIG['OUTPUT_DIR'], 'reports')

    csv_files = get_csv_files(mails_dir)
    if not csv_files:
        logger(f'No mailing list CSV files found in {mails_dir}.', "WARNING")
        return

    if CONFIG['VERBOSE'] >= 1:
        logger(f'Found {len(csv_files)} CSV files to consolidate:', "INFO")
        for csv_file in csv_files:
            logger(f'  - {os.path.basename(csv_file)}', "INFO")

    email_data: Dict[str, Any] = {}
    files_processed = 0

    for csv_file in csv_files:
        try:
            records = process_single_csv(csv_file)
            files_processed += 1

            for row_dict in records:
                # Use a default dict with empty strings to prevent key errors
                row = [row_dict.get(h, '') for h in full_expected_headers]
                email = row[0].strip().lower()
                if not email:
                    continue

                date_added_str = row[4].strip()
                try:
                    parsed_date = datetime.strptime(
                        date_added_str, '%b %d %Y %I:%M %p UTC')
                except ValueError:
                    parsed_date = datetime.now()
                    if CONFIG['VERBOSE'] >= 2:
                        logger(
                            f'  WARNING: Could not parse date "{date_added_str}" for email {email}', "WARNING")

                try:
                    num_purchases = int(row[7]) if row[7].strip() else 0
                except (ValueError, IndexError):
                    num_purchases = 0

                if email in email_data:
                    existing_data = email_data[email]
                    if parsed_date < existing_data['parsed_date']:
                        email_data[email] = {
                            'record': row[:],
                            'parsed_date': parsed_date,
                            'total_purchases': existing_data['total_purchases'] + num_purchases
                        }
                    else:
                        existing_data['total_purchases'] += num_purchases
                else:
                    email_data[email] = {
                        'record': row[:],
                        'parsed_date': parsed_date,
                        'total_purchases': num_purchases
                    }

            if CONFIG['VERBOSE'] >= 2:
                logger(
                    f'  Processed {len(records)} rows from {os.path.basename(csv_file)}', "INFO")

        except Exception as e:
            logger(
                f'ERROR: Failed to process {os.path.basename(csv_file)}: {e}', "ERROR")
            continue

    os.makedirs(reports_dir, exist_ok=True)
    consolidated_filename = 'consolidated_bandcamp_mailing_list.csv'
    consolidated_path = sanitize_path(
        os.path.join(reports_dir, consolidated_filename))

    # Setup SQLite database
    db_filename = 'bandcamp_mailing_lists.db'
    db_path = os.path.join(reports_dir, db_filename)
    db_path = sanitize_path(db_path)

    logger(f'Setting up SQLite database: {os.path.basename(db_path)}', "INFO")
    create_mailing_database_schema(db_path)

    write_consolidated_csv(
        consolidated_path, full_expected_headers, email_data)

    # Insert data into database
    db_inserted = insert_mailing_data(
        db_path, email_data, [os.path.basename(f) for f in csv_files])

    unique_emails = len(email_data)
    total_purchases = sum(data['total_purchases']
                          for data in email_data.values())

    logger('Consolidation complete!', "INFO")
    logger(f'  Files processed: {files_processed} of {len(csv_files)}', "INFO")
    logger(f'  Unique email addresses: {unique_emails}', "INFO")
    logger(f'  Total purchases: {total_purchases}', "INFO")
    logger(f'  Output file: {os.path.basename(consolidated_path)}', "INFO")
    logger(f'  Database: {os.path.basename(db_path)}', "INFO")
    logger(f'  Database rows inserted: {db_inserted}', "INFO")

    return consolidated_path


if __name__ == '__main__':
    sys.exit(main())
