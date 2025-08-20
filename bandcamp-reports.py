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

# These require pip installs
import requests
from requests import HTTPError
import browser_cookie3
from tqdm import tqdm
from reportlab.platypus import TableStyle

USER_URL = 'https://{}.bandcamp.com/net_revenue_report.csv?id={}&begin=2025-07-01&end=2025-07-31&items=&region=world'
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
}
SUPPORTED_BROWSERS = [
    'firefox',
    'chrome',
    'chromium',
    'brave',
    'opera',
    'edge'
]


def main() -> int:
    parser = argparse.ArgumentParser(description='Download revenue reports CSV files from specified bandcamp artists. Requires a logged in session in a supported browser so that the browser cookies can be used to authenticate with bandcamp.')
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
    args = parser.parse_args()

    CONFIG['VERBOSE'] = args.verbose
    CONFIG['OUTPUT_DIR'] = os.path.normcase(args.directory)
    CONFIG['BROWSER'] = args.browser
    CONFIG['FORCE'] = args.force

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
        print('WARNING: --force flag set, existing files will be overwritten.')

    if args.consolidate_only:
        print('Consolidating existing CSV files...')
        consolidate_csv_files()
    else:
        try:
            from artists import artists
            CONFIG['TQDM'].total = len(artists)

            for artist in artists:
                download_file(USER_URL.format(artist[0], artist[1]), 'revenues')
                CONFIG['TQDM'].update(1)

            CONFIG['TQDM'].close()
            print('Download complete. Analyzing and consolidating CSV files...')
            consolidate_csv_files()
        
            print('Done.')
        except (ImportError, UnboundLocalError):
            CONFIG['TQDM'].close()
            print('Please add your artists in artists.py')



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
    columns_to_exclude = ['UPC', 'ISRC', 'SKU', 'Collection society share', 'Container name', 'URL', 'Transaction date from', 'Transaction date to']
    
    # Create filtered headers (columns we want to keep)
    expected_headers = [col for col in full_expected_headers if col not in columns_to_exclude]
    
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
    csv_files = [f for f in all_csv_files if not os.path.basename(f).startswith('consolidated_bandcamp_reports_')]
    
    if not csv_files:
        print('No CSV files found in the output directory.')
        return
    
    # Generate output filename with year-month (reports are monthly scoped)
    year_month = datetime.now().strftime('%Y-%m')
    consolidated_filename = 'consolidated_bandcamp_reports_{}.csv'.format(year_month)
    reports_dir = os.path.join(CONFIG['OUTPUT_DIR'], 'reports')
    consolidated_path = os.path.join(reports_dir, consolidated_filename)
    consolidated_path = sanitize_path(consolidated_path)
    
    total_rows = 0
    files_processed = 0
    
    if CONFIG['VERBOSE'] >= 1:
        print('Found {} CSV files to consolidate:'.format(len(csv_files)))
        for csv_file in csv_files:
            print('  - {}'.format(os.path.basename(csv_file)))
    
    try:
        # Create reports directory if it doesn't exist
        os.makedirs(reports_dir, exist_ok=True)
        
        with open(consolidated_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            
            # Write the header once
            writer.writerow(expected_headers)
            
            for csv_file in csv_files:
                if CONFIG['VERBOSE'] >= 2:
                    print('Processing: {}'.format(os.path.basename(csv_file)))
                
                try:
                    # Try different encodings to handle CSV files with BOM or different encodings
                    encodings_to_try = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
                    infile = None
                    
                    for encoding in encodings_to_try:
                        try:
                            infile = open(csv_file, 'r', newline='', encoding=encoding)
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
                        raise Exception('Unable to decode file with any supported encoding')
                    
                    try:
                        reader = csv.reader(infile)
                        
                        # Read and validate header
                        try:
                            header = next(reader)
                        except StopIteration:
                            if CONFIG['VERBOSE'] >= 1:
                                print('WARNING: Empty file skipped: {}'.format(os.path.basename(csv_file)))
                            infile.close()
                            continue
                        
                        # Check if header matches expected full format (before filtering)
                        if header != full_expected_headers:
                            if CONFIG['VERBOSE'] >= 1:
                                print('WARNING: Header mismatch in {}, attempting to process anyway'.format(os.path.basename(csv_file)))
                                if CONFIG['VERBOSE'] >= 3:
                                    print('  Expected: {}'.format(full_expected_headers))
                                    print('  Found: {}'.format(header))
                        
                        # Copy all data rows, filtering to keep only desired columns
                        file_rows = 0
                        for row in reader:
                            if any(cell.strip() for cell in row):  # Skip completely empty rows
                                # Ensure row has the correct number of columns (full format)
                                while len(row) < len(full_expected_headers):
                                    row.append('')  # Pad with empty strings if needed
                                if len(row) > len(full_expected_headers):
                                    row = row[:len(full_expected_headers)]  # Truncate if too long
                                
                                # Filter row to only include desired columns
                                filtered_row = []
                                for i, cell in enumerate(row):
                                    if i < len(full_expected_headers) and full_expected_headers[i] not in columns_to_exclude:
                                        filtered_row.append(cell)
                                
                                writer.writerow(filtered_row)
                                file_rows += 1
                                total_rows += 1
                        
                        if CONFIG['VERBOSE'] >= 2:
                            print('  Added {} rows from {}'.format(file_rows, os.path.basename(csv_file)))
                        
                        files_processed += 1
                    finally:
                        if infile:
                            infile.close()
                        
                except Exception as e:
                    print('ERROR: Failed to process {}: {}'.format(os.path.basename(csv_file), str(e)))
                    continue
    
    except Exception as e:
        print('ERROR: Failed to create consolidated file: {}'.format(str(e)))
        return
    
    print('Consolidation complete!')
    print('  Files processed: {} of {}'.format(files_processed, len(csv_files)))
    print('  Total data rows: {}'.format(total_rows))
    print('  Output file: {}'.format(os.path.basename(consolidated_filename)))
    
    # Generate PDF version
    print('Generating PDF report...')
    generate_pdf_report(consolidated_path)


def generate_pdf_report(csv_path: str) -> None:
    """Generate a PDF report from the consolidated CSV file."""
    # Check if we have reportlab only (simpler approach without pandas)
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import landscape, A4
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
    except ImportError:
        print('PDF generation requires reportlab. Install with: pip install reportlab')
        return
    
    try:
        # Generate PDF filename
        pdf_filename = csv_path.replace('.csv', '.pdf')
        pdf_path = sanitize_path(pdf_filename)
        
        # First read the CSV header to determine monetary column positions
        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
        
        # Find monetary column indices based on the filtered header
        monetary_column_names = [
            'Gross revenue', 'Shipping', 'Taxes', 'Bandcamp assessed revenue share',
            'Payment processor fees', 'Net revenue'
        ]
        
        monetary_column_indices = []
        for col_name in monetary_column_names:
            try:
                idx = header.index(col_name)
                monetary_column_indices.append(idx)
            except ValueError:
                # Column not found in filtered headers, skip it
                pass
        
        # Read CSV file manually
        table_data = []
        total_rows = 0
        monetary_totals = [0.0] * len(monetary_column_indices)
        
        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
            
            # Add header with formatting
            table_data.append(header)
            
            # Process data rows
            for row in reader:
                if any(cell.strip() for cell in row):  # Skip empty rows
                    formatted_row = row[:]
                    
                    # Format monetary columns with Euro signs
                    for i, col_idx in enumerate(monetary_column_indices):
                        if col_idx < len(formatted_row):
                            try:
                                value = float(formatted_row[col_idx]) if formatted_row[col_idx] else 0.0
                                formatted_row[col_idx] = '€{:.2f}'.format(value)
                                monetary_totals[i] += value
                            except (ValueError, TypeError):
                                formatted_row[col_idx] = '€0.00'
                    
                    table_data.append(formatted_row)
                    total_rows += 1
        
        if total_rows == 0:
            print('WARNING: No data rows found in CSV file.')
            return
        
        # Create the PDF document in landscape mode
        doc = SimpleDocTemplate(pdf_path, pagesize=landscape(A4),
                               rightMargin=0.5*inch, leftMargin=0.5*inch,
                               topMargin=0.5*inch, bottomMargin=0.5*inch)
        
        # Get styles
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=12,
            spaceAfter=20,
            alignment=1  # Center alignment
        )
        
        # Create story (content) for the PDF
        story = []
        
        # Add title
        title_text = 'Bandcamp Revenue Report - {}'.format(datetime.now().strftime('%B %Y'))
        story.append(Paragraph(title_text, title_style))
        story.append(Spacer(1, 12))
        
        # Split table into multiple pages (8 columns max per page)
        max_cols_per_page = 8
        total_cols = len(table_data[0]) if table_data else 0
        
        if total_cols <= max_cols_per_page:
            # Table fits on one page
            table = Table(table_data, repeatRows=1)
            table.setStyle(get_table_style(0, min(total_cols, max_cols_per_page), header))
            story.append(table)
        else:
            # Split table across multiple pages
            page_num = 1
            for start_col in range(0, total_cols, max_cols_per_page):
                end_col = min(start_col + max_cols_per_page, total_cols)
                
                # Create table for this page
                page_data = []
                for row in table_data:
                    page_data.append(row[start_col:end_col])
                
                # Add page subtitle
                if page_num > 1:
                    story.append(Spacer(1, 20))
                    subtitle = Paragraph('Columns {} - {} (Page {})'.format(start_col + 1, end_col, page_num), styles['Heading2'])
                    story.append(subtitle)
                    story.append(Spacer(1, 12))
                else:
                    subtitle = Paragraph('Columns {} - {} (Page {})'.format(start_col + 1, end_col, page_num), styles['Heading2'])
                    story.append(subtitle)
                    story.append(Spacer(1, 12))
                
                # Create table with header slice for this page
                page_header = header[start_col:end_col]
                table = Table(page_data, repeatRows=1)
                table.setStyle(get_table_style(start_col, end_col, page_header))
                story.append(table)
                
                page_num += 1
        
        # Add summary information
        story.append(Spacer(1, 20))
        
        # Calculate totals for monetary columns
        summary_data = []
        summary_data.append(['Summary', ''])
        summary_data.append(['Total Records', str(total_rows)])
        
        # Only include summary totals for columns that actually exist in our filtered data
        for i, col_idx in enumerate(monetary_column_indices):
            col_name = header[col_idx]
            if i < len(monetary_totals):
                summary_data.append(['Total {}'.format(col_name), '€{:.2f}'.format(monetary_totals[i])])
        
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
        
        # Build PDF
        doc.build(story)
        
        print('PDF report generated: {}'.format(os.path.basename(pdf_path)))
        
    except Exception as e:
        print('ERROR: Failed to generate PDF report: {}'.format(str(e)))
        if CONFIG['VERBOSE'] >= 2:
            import traceback
            traceback.print_exc()


def get_table_style(start_col: int, end_col: int, header: list = None) -> 'TableStyle':
    """Generate table style with appropriate column alignment based on column range."""
    from reportlab.lib import colors
    from reportlab.platypus import TableStyle
    
    style_commands = [
        # Header styling
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        
        # Data rows styling
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 7),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        
        # Alternate row colors
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.beige, colors.lightgrey]),
    ]
    
    # If header is provided, find monetary columns and right-align them
    if header:
        monetary_column_names = [
            'Gross revenue', 'Shipping', 'Taxes', 'Bandcamp assessed revenue share',
            'Payment processor fees', 'Net revenue'
        ]
        
        # For page headers (sliced), we need to check against the sliced header
        for i, col_name in enumerate(header):
            if col_name in monetary_column_names:
                style_commands.append(('ALIGN', (i, 1), (i, -1), 'RIGHT'))
    
    return TableStyle(style_commands)


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
