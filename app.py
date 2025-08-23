"""
Bandcamp Utilities Web Application
Restructured with modular API organization
"""
from flask import Flask, send_from_directory
import os

# Import route blueprints
from api.routes.main_routes import main_bp
from api.routes.utility_routes import utilities_bp
from api.routes.revenues_database_routes import database_bp
from api.routes.artist_routes import artists_bp
from api.routes.albums_routes import albums_bp
from api.routes.music_routes import music_bp


def create_app():
    """Application factory pattern"""
    app = Flask(__name__)

    # Register blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(utilities_bp)
    app.register_blueprint(database_bp)
    app.register_blueprint(artists_bp)
    app.register_blueprint(albums_bp)
    app.register_blueprint(music_bp)

    return app


# Create the Flask application
app = create_app()


@app.route('/albums/extracted/<artist>/<album>/<filename>')
def serve_album_cover(artist, album, filename):
    base_path = os.path.join('outputs', 'albums', 'extracted', artist, album)
    return send_from_directory(base_path, filename)


if __name__ == '__main__':
    app.run(debug=True)
