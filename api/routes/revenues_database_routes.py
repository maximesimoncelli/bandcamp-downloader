"""
Database routes for viewing database contents
"""
from flask import Blueprint, render_template
from ..services.database_service import DatabaseService

database_bp = Blueprint('database', __name__)


@database_bp.route('/database')
def view_database():
    """Display revenue reports data from the database in a table"""
    data, db_info = DatabaseService.get_revenue_reports_data()

    if data is None:
        return render_template('revenues_database.html', error=db_info, data=None, db_name=None)

    return render_template('revenues_database.html', data=data, db_name=db_info, error=None)


@database_bp.route('/mail-database')
def view_mail_database():
    """Display mailing lists data from the database in a table"""
    data, db_info = DatabaseService.get_mailing_lists_data()

    if data is None:
        return render_template('emails_database.html', error=db_info, data=None, db_name=None)

    return render_template('emails_database.html', data=data, db_name=db_info, error=None)
