"""
Artist routes for artist overview and detail pages
"""
from flask import Blueprint, render_template
from ..services.artist_service import ArtistService

artists_bp = Blueprint('artists', __name__)


@artists_bp.route('/artists-overview')
def artists_overview():
    """Display an overview of all artists with summary cards"""
    artist_data = []

    # Get unique artist names from the databases
    db_artists = ArtistService.get_unique_artists_from_databases()

    for artist_name in db_artists:
        artist_info = ArtistService.build_artist_info(artist_name)
        artist_data.append(artist_info)

    return render_template('artists_overview.html', artists=artist_data)


@artists_bp.route('/artist/<artist_name>')
def artist_detail(artist_name):
    """Display detailed information for a specific artist"""
    artist_info = ArtistService.build_detailed_artist_info(artist_name)
    return render_template('artist_detail.html', artist=artist_info)
