from flask import Blueprint, render_template

music_bp = Blueprint('music', __name__)


@music_bp.route('/music')
def music():
    return render_template('music.html')
