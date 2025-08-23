"""
Artist service module for handling artist-related operations
"""
import os
import sqlite3
import glob
from .database_service import DatabaseService


class ArtistService:
    """Service class for artist-related operations"""

    @staticmethod
    def get_unique_artists_from_databases():
        """Get unique artist names from both revenue and mailing databases"""
        artists_set = set()

        # Get artists from revenue database
        try:
            data, _ = DatabaseService.get_revenue_reports_data()
            if data:
                for row in data:
                    artists_set.add(row['artist_name'])
        except Exception as e:
            print(f"Error getting artists from revenue database: {e}")

        # Get artists from mailing database
        try:
            data, _ = DatabaseService.get_mailing_lists_data()
            if data:
                for row in data:
                    # Extract artist name from source_file if it follows the pattern
                    # mailing_list-{artist}.csv
                    source_file = row.get('source_file', '')
                    if source_file.startswith('mailing_list-') and source_file.endswith('.csv'):
                        # Remove 'mailing_list-' and '.csv'
                        artist_name = source_file[13:-4]
                        artists_set.add(artist_name)
        except Exception as e:
            print(f"Error getting artists from mailing database: {e}")

        # Convert to sorted list
        return sorted(list(artists_set))

    @staticmethod
    def get_artist_revenue_stats(artist_name):
        """Get revenue statistics for a specific artist from the database"""
        try:
            # Find the most recent database file
            db_files = glob.glob(os.path.join(
                'outputs', '**', 'bandcamp_revenue_reports*.db'), recursive=True)
            if not db_files:
                return None

            db_path = max(db_files, key=os.path.getctime)

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Get total revenue and transaction count for this artist
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_transactions,
                    SUM(gross_revenue) as total_gross,
                    SUM(net_revenue) as total_net,
                    SUM(quantity) as total_quantity
                FROM revenue_reports 
                WHERE artist_name = ?
            """, (artist_name,))

            result = cursor.fetchone()
            conn.close()

            if result and result[0] > 0:
                return {
                    'total_transactions': result[0],
                    'total_gross': result[1] or 0,
                    'total_net': result[2] or 0,
                    'total_quantity': result[3] or 0
                }
        except Exception as e:
            print(f"Error getting revenue stats for {artist_name}: {e}")

        return None

    @staticmethod
    def get_artist_detailed_revenue_stats(artist_name):
        """Get detailed revenue statistics for a specific artist"""
        try:
            # Find the most recent database file
            db_files = glob.glob(os.path.join(
                'outputs', '**', 'bandcamp_revenue_reports*.db'), recursive=True)
            if not db_files:
                return None

            db_path = max(db_files, key=os.path.getctime)

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Get comprehensive revenue statistics
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_transactions,
                    SUM(gross_revenue) as total_gross,
                    SUM(net_revenue) as total_net,
                    SUM(quantity) as total_quantity,
                    MIN(transaction_date_from) as first_transaction,
                    MAX(transaction_date_to) as last_transaction,
                    COUNT(DISTINCT item_name) as unique_items,
                    AVG(gross_revenue) as avg_transaction_value
                FROM revenue_reports 
                WHERE artist_name = ?
            """, (artist_name,))

            result = cursor.fetchone()
            conn.close()

            if result and result[0] > 0:
                return {
                    'total_transactions': result[0],
                    'total_gross': result[1] or 0,
                    'total_net': result[2] or 0,
                    'total_quantity': result[3] or 0,
                    'first_transaction': result[4],
                    'last_transaction': result[5],
                    'unique_items': result[6] or 0,
                    'avg_transaction_value': result[7] or 0
                }
        except Exception as e:
            print(
                f"Error getting detailed revenue stats for {artist_name}: {e}")

        return None

    @staticmethod
    def get_artist_mailing_stats(artist_name):
        """Get mailing list statistics for a specific artist from the database"""
        try:
            # Find the most recent database file
            db_files = glob.glob(os.path.join(
                'outputs', '**', 'bandcamp_mailing_lists*.db'), recursive=True)
            if not db_files:
                return None

            db_path = max(db_files, key=os.path.getctime)

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Get subscriber count and total purchases for this artist
            # Use source_file pattern to match artist name: mailing_list-{artist}.csv
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT email) as unique_subscribers,
                    SUM(num_purchases) as total_purchases,
                    COUNT(DISTINCT country) as countries
                FROM mailing_lists 
                WHERE source_file = ?
            """, (f"mailing_list-{artist_name}.csv",))

            result = cursor.fetchone()
            conn.close()

            if result and result[0] > 0:
                return {
                    'unique_subscribers': result[0],
                    'total_purchases': result[1] or 0,
                    'countries': result[2] or 0
                }
        except Exception as e:
            print(f"Error getting mailing stats for {artist_name}: {e}")

        return None

    @staticmethod
    def get_artist_albums_data(artist_name):
        """Get album-specific data for an artist"""
        try:
            # Find the most recent database file
            db_files = glob.glob(os.path.join(
                'outputs', '**', 'bandcamp_revenue_reports*.db'), recursive=True)
            if not db_files:
                return []

            db_path = max(db_files, key=os.path.getctime)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get album-specific statistics
            cursor.execute("""
                SELECT 
                    item_name,
                    item_type,
                    SUM(quantity) as total_quantity,
                    SUM(gross_revenue) as total_gross,
                    SUM(net_revenue) as total_net,
                    COUNT(*) as transaction_count,
                    MIN(transaction_date_from) as first_sale,
                    MAX(transaction_date_to) as last_sale
                FROM revenue_reports 
                WHERE artist_name = ?
                GROUP BY item_name, item_type
                ORDER BY total_net DESC
            """, (artist_name,))

            results = cursor.fetchall()
            conn.close()

            albums = []
            for row in results:
                albums.append({
                    'name': row['item_name'],
                    'type': row['item_type'],
                    'total_quantity': row['total_quantity'],
                    'total_gross': row['total_gross'],
                    'total_net': row['total_net'],
                    'transaction_count': row['transaction_count'],
                    'first_sale': row['first_sale'],
                    'last_sale': row['last_sale']
                })

            return albums

        except Exception as e:
            print(f"Error getting albums data for {artist_name}: {e}")
            return []

    @staticmethod
    def build_artist_info(artist_name):
        """Build complete artist information object"""
        # Get basic stats from databases if available
        revenue_stats = ArtistService.get_artist_revenue_stats(artist_name)
        mailing_stats = ArtistService.get_artist_mailing_stats(artist_name)

        # Construct email and URLs
        email = f"bandcamp@noisefilter.ovh"
        bandcamp_url_temp = artist_name.replace(
            '-', ' ').replace('_', ' ').replace(' ', '').replace('&', 'and').lower()

        return {
            'name': artist_name,
            'display_name': artist_name.replace('-', ' ').replace('_', ' ').title(),
            'email': email,
            'bandcamp_url': f"https://{bandcamp_url_temp}.bandcamp.com",
            'bandcamp_tools_url': f"https://{artist_name}.bandcamp.com/tools",
            'revenue_stats': revenue_stats,
            'mailing_stats': mailing_stats
        }

    @staticmethod
    def build_detailed_artist_info(artist_name):
        """Build detailed artist information for individual artist page"""
        # Get artist albums and detailed stats
        albums_data = ArtistService.get_artist_albums_data(artist_name)
        revenue_stats = ArtistService.get_artist_detailed_revenue_stats(
            artist_name)
        mailing_stats = ArtistService.get_artist_mailing_stats(artist_name)

        # Construct email and URLs
        email = f"bandcamp@noisefilter.ovh"
        bandcamp_url_temp = artist_name.replace(
            '-', ' ').replace('_', ' ').replace(' ', '').replace('&', 'and').lower()

        return {
            'name': artist_name,
            'display_name': artist_name.replace('-', ' ').replace('_', ' ').title(),
            'email': email,
            'bandcamp_url': f"https://{bandcamp_url_temp}.bandcamp.com",
            'bandcamp_tools_url': f"https://{artist_name}.bandcamp.com/tools",
            'albums': albums_data,
            'revenue_stats': revenue_stats,
            'mailing_stats': mailing_stats
        }
