"""
Main routes for the application
"""
from flask import Blueprint, render_template
from ..services.utility_service import UtilityService

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def index():
    """Main page with navigation to both utilities"""
    return render_template('index.html',
                           browsers=UtilityService.get_supported_browsers(),
                           utility_status=UtilityService.get_all_utility_status())
