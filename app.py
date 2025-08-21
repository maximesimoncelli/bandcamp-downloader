from flask import Flask, render_template, request, send_from_directory, jsonify
import threading
import os
import time

# Import both utilities
from bandcamp_mails import main as run_mails_utility, CONFIG as MAILS_CONFIG, SUPPORTED_BROWSERS
from bandcamp_reports import main as run_reports_utility, CONFIG as REPORTS_CONFIG

app = Flask(__name__)

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

# This function will run the utility in the background


def run_in_thread(config, utility_type='mails'):
    global utility_status

    # Reset status for the specific utility
    utility_status[utility_type]['running'] = True
    utility_status[utility_type]['completed'] = False
    utility_status[utility_type]['pdf_filename'] = None
    utility_status[utility_type]['output_directory'] = config['directory']

    print(f"Starting {utility_type} utility")
    try:
        if utility_type == 'mails':
            result = run_mails_utility(config)
        elif utility_type == 'reports':
            result = run_reports_utility(config)
        else:
            raise ValueError(f"Unknown utility type: {utility_type}")

        if result:  # If a PDF was generated
            utility_status[utility_type]['pdf_filename'] = result
        utility_status[utility_type]['completed'] = True
    except Exception as e:
        print(f"{utility_type.title()} utility failed: {e}")
        utility_status[utility_type]['completed'] = True
    finally:
        utility_status[utility_type]['running'] = False
    print(f"{utility_type.title()} utility finished.")


@app.route('/')
def index():
    """Main page with navigation to both utilities"""
    return render_template('index.html',
                           browsers=SUPPORTED_BROWSERS,
                           utility_status=utility_status)


@app.route('/mails', methods=['GET', 'POST'])
def mails():
    """Mailing list utility page"""
    global utility_status
    status = ""

    if request.method == 'POST':
        # Get form data from the user's submission
        form_data = request.form

        # Build the configuration dictionary for the mails utility
        config = {
            'browser': form_data.get('browser', MAILS_CONFIG['BROWSER']),
            'directory': form_data.get('directory', os.path.join(os.getcwd(), 'outputs')),
            'force': 'force' in form_data,
            'consolidate_only': 'consolidate_only' in form_data,
            'verbose': 1,
        }

        # Start the utility process in a new thread
        utility_thread = threading.Thread(
            target=run_in_thread, args=(config, 'mails'))
        utility_thread.start()

        status = "Mailing list utility started in the background. Check your console for progress!"

    return render_template('mails.html',
                           status=status,
                           browsers=SUPPORTED_BROWSERS,
                           utility_status=utility_status['mails'])


@app.route('/reports', methods=['GET', 'POST'])
def reports():
    """Revenue reports utility page"""
    global utility_status
    status = ""

    if request.method == 'POST':
        # Get form data from the user's submission
        form_data = request.form

        # Build the configuration dictionary for the reports utility
        config = {
            # Use MAILS_CONFIG as fallback
            'browser': form_data.get('browser', MAILS_CONFIG['BROWSER']),
            'directory': form_data.get('directory', os.path.join(os.getcwd(), 'outputs')),
            'force': 'force' in form_data,
            'consolidate_only': 'consolidate_only' in form_data,
            'verbose': 1,
        }

        # Start the utility process in a new thread
        utility_thread = threading.Thread(
            target=run_in_thread, args=(config, 'reports'))
        utility_thread.start()

        status = "Revenue reports utility started in the background. Check your console for progress!"

    return render_template('reports.html',
                           status=status,
                           browsers=SUPPORTED_BROWSERS,
                           utility_status=utility_status['reports'])


@app.route('/status/<utility_type>')
def get_status(utility_type):
    """API endpoint to check utility status for a specific utility"""
    if utility_type in utility_status:
        return jsonify(utility_status[utility_type])
    else:
        return jsonify({'error': 'Unknown utility type'}), 400


@app.route('/download/<utility_type>/<filename>')
def download_file(utility_type, filename):
    """Serve PDF files from the reports directory for a specific utility"""
    if utility_type in utility_status and utility_status[utility_type]['output_directory']:
        reports_dir = os.path.join(
            utility_status[utility_type]['output_directory'], 'reports')
        return send_from_directory(reports_dir, filename)
    else:
        return "File not found", 404


if __name__ == '__main__':
    app.run(debug=True)
