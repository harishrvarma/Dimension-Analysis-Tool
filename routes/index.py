from flask import Blueprint, render_template

from routes.dimension.chart import chart_bp
from routes.dimension.grid import grid_bp
from routes.dimension.outlier import outlier_bp
from routes.dimension.analyzer import analyzer_bp
from routes.import_csv import import_bp

# Home blueprint
home_bp = Blueprint("home_bp", __name__)

@home_bp.route("/")
def index():
    return render_template("home.html", active_page="home")

# Function to register all blueprints
def register_blueprints(app):
    app.register_blueprint(home_bp)
    app.register_blueprint(chart_bp)
    app.register_blueprint(grid_bp)
    app.register_blueprint(outlier_bp)
    app.register_blueprint(analyzer_bp)
    app.register_blueprint(import_bp)
