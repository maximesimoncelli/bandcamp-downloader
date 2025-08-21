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
from typing import Dict, List, Any, Tuple

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
        print(config)
        CONFIG['OUTPUT_DIR'] = config.get('directory', CONFIG['OUTPUT_DIR'])
        CONFIG['FORCE'] = config.get('force', CONFIG['FORCE'])
        CONFIG['VERBOSE'] = config.get('verbose', CONFIG['VERBOSE'])
        CONFIG['CONSOLIDATE_ONLY'] = config.get('consolidate_only', False)

    if CONFIG['FORCE']:
        print('WARNING: --force flag set, existing files will be overwritten.')

    # Initialize TQDM progress bar
    CONFIG['TQDM'] = tqdm(total=0, unit='files',
                          disable=CONFIG['VERBOSE'] == 0)

    if CONFIG['CONSOLIDATE_ONLY']:
        print('Consolidating existing CSV files...')
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
            print('Download complete. Analyzing and consolidating CSV files...')
            return consolidate_csv_files()

        except (ImportError, UnboundLocalError):
            CONFIG['TQDM'].close()
            print('Please add your artists in artists.py')

        finally:
            print('Done')


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
                        print(
                            f'WARNING: Empty file skipped: {os.path.basename(file_path)}')
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


def consolidate_csv_files() -> None:
    """Consolidates all mailing list CSV files into a single file."""
    full_expected_headers = ['email', 'fullname', 'firstname',
                             'lastname', 'date added', 'country', 'postal code', 'num purchases']

    mails_dir = os.path.join(CONFIG['OUTPUT_DIR'], 'mails')
    reports_dir = os.path.join(CONFIG['OUTPUT_DIR'], 'reports')

    csv_files = get_csv_files(mails_dir)
    if not csv_files:
        print(f'No mailing list CSV files found in {mails_dir}.')
        return

    if CONFIG['VERBOSE'] >= 1:
        print(f'Found {len(csv_files)} CSV files to consolidate:')
        for csv_file in csv_files:
            print(f'  - {os.path.basename(csv_file)}')

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
                        print(
                            f'  WARNING: Could not parse date "{date_added_str}" for email {email}')

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
                print(
                    f'  Processed {len(records)} rows from {os.path.basename(csv_file)}')

        except Exception as e:
            print(
                f'ERROR: Failed to process {os.path.basename(csv_file)}: {e}')
            continue

    os.makedirs(reports_dir, exist_ok=True)
    consolidated_filename = 'consolidated_bandcamp_mailing_list.csv'
    consolidated_path = sanitize_path(
        os.path.join(reports_dir, consolidated_filename))

    write_consolidated_csv(
        consolidated_path, full_expected_headers, email_data)

    unique_emails = len(email_data)
    total_purchases = sum(data['total_purchases']
                          for data in email_data.values())

    print('Consolidation complete!')
    print(f'  Files processed: {files_processed} of {len(csv_files)}')
    print(f'  Unique email addresses: {unique_emails}')
    print(f'  Total purchases: {total_purchases}')
    print(f'  Output file: {os.path.basename(consolidated_path)}')

    print('Generating PDF report...')
    return generate_pdf_report(consolidated_path)


def generate_pdf_report(csv_path: str) -> None:
    """Generates a PDF report from the consolidated mailing list CSV file."""
    try:
        from reportlab.lib.pagesizes import landscape, A4
        from reportlab.platypus import SimpleDocTemplate, Table, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
    except ImportError:
        print('PDF generation requires reportlab. Install with: pip install reportlab')
        return

    try:
        pdf_path = sanitize_path(csv_path.replace('.csv', '.pdf'))

        table_data = []
        total_rows = 0
        total_purchases = 0
        countries = {}

        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
            table_data.append(header)

            for row in reader:
                if any(cell.strip() for cell in row):
                    table_data.append(row)
                    total_rows += 1

                    try:
                        purchases = int(row[7]) if row[7].strip() else 0
                        total_purchases += purchases
                    except ValueError:
                        pass

                    country = row[5].strip() if len(row) > 5 else 'Unknown'
                    countries[country] = countries.get(country, 0) + 1

        if total_rows == 0:
            print('WARNING: No data rows found in CSV file.')
            return

        doc = SimpleDocTemplate(pdf_path, pagesize=landscape(
            A4), rightMargin=0.5*inch, leftMargin=0.5*inch, topMargin=0.5*inch, bottomMargin=0.5*inch)

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle', parent=styles['Heading1'], fontSize=12, spaceAfter=20, alignment=1)

        story = []
        title_text = f'Bandcamp Mailing List Report - {datetime.now().strftime("%B %Y")}'
        story.append(Paragraph(title_text, title_style))
        story.append(Spacer(1, 12))

        table = Table(table_data, repeatRows=1)
        table.setStyle(get_mailing_list_table_style())
        story.append(table)

        story.append(Spacer(1, 20))

        summary_data = [['Summary', '']]
        summary_data.append(['Total Email Addresses', str(total_rows)])
        summary_data.append(['Total Purchases', str(total_purchases)])

        if countries:
            summary_data.append(['', ''])
            summary_data.append(['Top Countries', ''])
            sorted_countries = sorted(
                countries.items(), key=lambda x: x[1], reverse=True)[:5]
            for country, count in sorted_countries:
                summary_data.append([country, str(count)])

        summary_table = Table(summary_data)
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkgrey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ALIGN', (1, 1), (1, -1), 'RIGHT'),
        ]))

        story.append(summary_table)
        doc.build(story)

        print(f'PDF report generated: {os.path.basename(pdf_path)}')
        return os.path.basename(pdf_path)

    except Exception as e:
        print(f'ERROR: Failed to generate PDF report: {e}')
        if CONFIG['VERBOSE'] >= 2:
            import traceback
            traceback.print_exc()


def get_mailing_list_table_style() -> TableStyle:
    """Generates table style for mailing list data."""
    style_commands = [
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 7),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.beige, colors.lightgrey]),
        ('ALIGN', (7, 1), (7, -1), 'RIGHT'),
    ]
    return TableStyle(style_commands)


if __name__ == '__main__':
    sys.exit(main())
