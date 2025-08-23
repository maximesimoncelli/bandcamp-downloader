"""
Database service module for handling all database operations
"""
import os
import sqlite3
import glob


class DatabaseService:
    """Service class for database operations"""

    @staticmethod
    def get_revenue_reports_data():
        """Query the SQLite database for revenue reports data"""
        # Look for database files in outputs directory and subdirectories
        outputs_dir = os.path.join(os.getcwd(), 'outputs')

        # Search in both outputs/ and outputs/reports/ directories
        db_patterns = [
            os.path.join(outputs_dir, 'bandcamp_revenue_reports*.db'),
            os.path.join(outputs_dir, 'reports',
                         'bandcamp_revenue_reports*.db'),
            os.path.join(outputs_dir, '**', 'bandcamp_revenue_reports*.db')
        ]

        db_files = []
        for pattern in db_patterns:
            db_files.extend(glob.glob(pattern, recursive=True))

        if not db_files:
            return None, "No revenue reports database found"

        # Use the most recent database file
        latest_db = max(db_files, key=os.path.getmtime)

        try:
            conn = sqlite3.connect(latest_db)
            conn.row_factory = sqlite3.Row  # This allows accessing columns by name
            cursor = conn.cursor()

            # Query all revenue reports data
            cursor.execute('''
                SELECT 
                    artist_name,
                    item_name,
                    item_type,
                    label_name,
                    region,
                    quantity,
                    currency,
                    gross_revenue,
                    net_revenue,
                    transaction_date_from,
                    transaction_date_to,
                    date_range_begin,
                    date_range_end,
                    import_date
                FROM revenue_reports 
                ORDER BY import_date DESC, artist_name, item_name
            ''')

            rows = cursor.fetchall()
            conn.close()

            # Convert rows to list of dictionaries for easier template handling
            data = []
            for row in rows:
                data.append({
                    'artist_name': row['artist_name'],
                    'item_name': row['item_name'],
                    'item_type': row['item_type'],
                    'label_name': row['label_name'],
                    'region': row['region'],
                    'quantity': row['quantity'],
                    'currency': row['currency'],
                    'gross_revenue': row['gross_revenue'],
                    'net_revenue': row['net_revenue'],
                    'transaction_date_from': row['transaction_date_from'],
                    'transaction_date_to': row['transaction_date_to'],
                    'date_range_begin': row['date_range_begin'],
                    'date_range_end': row['date_range_end'],
                    'import_date': row['import_date']
                })

            return data, os.path.basename(latest_db)

        except sqlite3.Error as e:
            return None, f"Database error: {str(e)}"

    @staticmethod
    def get_mailing_lists_data():
        """Query the SQLite database for mailing lists data"""
        # Look for database files in outputs directory and subdirectories
        outputs_dir = os.path.join(os.getcwd(), 'outputs')

        # Search in both outputs/ and outputs/reports/ directories
        db_patterns = [
            os.path.join(outputs_dir, 'bandcamp_mailing_lists*.db'),
            os.path.join(outputs_dir, 'reports', 'bandcamp_mailing_lists*.db'),
            os.path.join(outputs_dir, '**', 'bandcamp_mailing_lists*.db')
        ]

        db_files = []
        for pattern in db_patterns:
            db_files.extend(glob.glob(pattern, recursive=True))

        if not db_files:
            return None, "No mailing lists database found"

        # Use the most recent database file
        latest_db = max(db_files, key=os.path.getmtime)

        try:
            conn = sqlite3.connect(latest_db)
            conn.row_factory = sqlite3.Row  # This allows accessing columns by name
            cursor = conn.cursor()

            # Query all mailing lists data
            cursor.execute('''
                SELECT 
                    email,
                    fullname,
                    firstname,
                    lastname,
                    date_added,
                    country,
                    postal_code,
                    num_purchases,
                    total_purchases,
                    import_date,
                    source_file
                FROM mailing_lists 
                ORDER BY import_date DESC, email
            ''')

            rows = cursor.fetchall()
            conn.close()

            # Convert rows to list of dictionaries for easier template handling
            data = []
            for row in rows:
                data.append({
                    'email': row['email'],
                    'fullname': row['fullname'],
                    'firstname': row['firstname'],
                    'lastname': row['lastname'],
                    'date_added': row['date_added'],
                    'country': row['country'],
                    'postal_code': row['postal_code'],
                    'num_purchases': row['num_purchases'],
                    'total_purchases': row['total_purchases'],
                    'import_date': row['import_date'],
                    'source_file': row['source_file']
                })

            return data, os.path.basename(latest_db)

        except sqlite3.Error as e:
            return None, f"Database error: {str(e)}"
