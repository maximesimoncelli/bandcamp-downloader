import os
from flask import Blueprint, render_template

albums_bp = Blueprint('albums', __name__)


@albums_bp.route('/albums')
def albums():
    base_path = os.path.join('outputs', 'albums', 'extracted')
    albums_by_artist = {}
    if os.path.exists(base_path):
        for artist in os.listdir(base_path):
            artist_path = os.path.join(base_path, artist)
            if os.path.isdir(artist_path):
                albums = []
                for album in os.listdir(artist_path):
                    album_path = os.path.join(artist_path, album)
                    cover_path = os.path.join(album_path, 'cover.jpg')
                    if os.path.isdir(album_path) and os.path.exists(cover_path):
                        albums.append(album)
                if albums:
                    albums_by_artist[artist] = albums
    return render_template('albums.html', albums_by_artist=albums_by_artist)
