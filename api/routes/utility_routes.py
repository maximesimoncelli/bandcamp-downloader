"""
Utility routes for mails and reports functionality
"""
from flask import Blueprint, render_template, request, send_from_directory, jsonify
import os
from ..services.utility_service import UtilityService

utilities_bp = Blueprint('utilities', __name__)


@utilities_bp.route('/mails', methods=['GET', 'POST'])
def mails():
    """Mailing list utility page"""
    status = ""

    if request.method == 'POST':
        status = UtilityService.start_mails_utility(request.form)

    return render_template('emails.html',
                           status=status,
                           browsers=UtilityService.get_supported_browsers(),
                           utility_status=UtilityService.get_utility_status('mails'))


@utilities_bp.route('/reports', methods=['GET', 'POST'])
def reports():
    """Revenue reports utility page"""
    status = ""
    dates = UtilityService.get_default_dates()

    if request.method == 'POST':
        status = UtilityService.start_reports_utility(request.form)

    return render_template('reports.html',
                           status=status,
                           browsers=UtilityService.get_supported_browsers(),
                           utility_status=UtilityService.get_utility_status(
                               'reports'),
                           default_begin_date=dates['default_begin_date'],
                           default_end_date=dates['default_end_date'])


@utilities_bp.route('/status/<utility_type>')
def get_status(utility_type):
    """API endpoint to check utility status for a specific utility"""
    status = UtilityService.get_utility_status(utility_type)
    if status:
        return jsonify(status)
    else:
        return jsonify({'error': 'Unknown utility type'}), 400
