"""
Utility service module for handling background utility operations
"""
import threading
import os
from datetime import datetime
import calendar
from utils.logger import logger

# Import utility functions
from commands.bandcamp_mails import main as run_mails_utility, CONFIG as MAILS_CONFIG, SUPPORTED_BROWSERS
from commands.bandcamp_reports import main as run_reports_utility, CONFIG as REPORTS_CONFIG


class UtilityService:
    """Service class for utility operations"""

    # Global variables to track utility status and results for both utilities
    utility_status = {
        'mails': {
            'running': False,
            'completed': False,
            'pdf_filename': None,
            'output_directory': None
        },
        'reports': {
            'running': False,
            'completed': False,
            'pdf_filename': None,
            'output_directory': None
        }
    }

    @staticmethod
    def run_in_thread(config, utility_type='mails'):
        """Run utility in background thread"""
        # Reset status for the specific utility
        UtilityService.utility_status[utility_type]['running'] = True
        UtilityService.utility_status[utility_type]['completed'] = False
        UtilityService.utility_status[utility_type]['pdf_filename'] = None
        UtilityService.utility_status[utility_type]['output_directory'] = config['directory']

        logger(f"Starting {utility_type} utility", "INFO")
        try:
            if utility_type == 'mails':
                run_mails_utility(config)
            elif utility_type == 'reports':
                run_reports_utility(config)
            else:
                raise ValueError(f"Unknown utility type: {utility_type}")

            UtilityService.utility_status[utility_type]['completed'] = True
        except Exception as e:
            logger(f"{utility_type.title()} utility failed: {e}", "ERROR")
            UtilityService.utility_status[utility_type]['completed'] = True
        finally:
            UtilityService.utility_status[utility_type]['running'] = False
        logger(f"{utility_type.title()} utility finished.", "INFO")

    @staticmethod
    def start_mails_utility(form_data):
        """Start mails utility with configuration from form data"""
        config = {
            'browser': form_data.get('browser', MAILS_CONFIG['BROWSER']),
            'directory': form_data.get('directory', os.path.join(os.getcwd(), 'outputs')),
            'force': 'force' in form_data,
            'consolidate_only': 'consolidate_only' in form_data,
            'verbose': 1,
        }

        # Start the utility process in a new thread
        utility_thread = threading.Thread(
            target=UtilityService.run_in_thread, args=(config, 'mails'))
        utility_thread.start()

        return "Mailing list utility started in the background. Check your console for progress!"

    @staticmethod
    def start_reports_utility(form_data):
        """Start reports utility with configuration from form data"""
        # Calculate default dates (current month)
        today = datetime.today()
        first_day = datetime(today.year, today.month, 1)
        last_day = datetime(today.year, today.month,
                            calendar.monthrange(today.year, today.month)[1])
        default_begin_date = first_day.strftime('%Y-%m-%d')
        default_end_date = last_day.strftime('%Y-%m-%d')

        config = {
            # Use MAILS_CONFIG as fallback
            'browser': form_data.get('browser', MAILS_CONFIG['BROWSER']),
            'directory': form_data.get('directory', os.path.join(os.getcwd(), 'outputs')),
            'force': 'force' in form_data,
            'consolidate_only': 'consolidate_only' in form_data,
            'verbose': 1,
            'date_begin': form_data.get('date_begin', default_begin_date),
            'date_end': form_data.get('date_end', default_end_date),
        }

        # Start the utility process in a new thread
        utility_thread = threading.Thread(
            target=UtilityService.run_in_thread, args=(config, 'reports'))
        utility_thread.start()

        return "Revenue reports utility started in the background. Check your console for progress!"

    @staticmethod
    def get_utility_status(utility_type):
        """Get status for a specific utility type"""
        return UtilityService.utility_status.get(utility_type)

    @staticmethod
    def get_all_utility_status():
        """Get status for all utilities"""
        return UtilityService.utility_status

    @staticmethod
    def get_default_dates():
        """Get default date range for current month"""
        today = datetime.today()
        first_day = datetime(today.year, today.month, 1)
        last_day = datetime(today.year, today.month,
                            calendar.monthrange(today.year, today.month)[1])
        return {
            'default_begin_date': first_day.strftime('%Y-%m-%d'),
            'default_end_date': last_day.strftime('%Y-%m-%d')
        }

    @staticmethod
    def get_supported_browsers():
        """Get list of supported browsers"""
        return SUPPORTED_BROWSERS
