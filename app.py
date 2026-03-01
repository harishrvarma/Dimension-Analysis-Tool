import config  # loads .env immediately
import os
from flask import Flask
from routes.index import register_blueprints
from models.core.session import Session

app = Flask(__name__)
app.config["APP_NAME"] = os.getenv("APP_NAME")
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY")
register_blueprints(app)

if __name__ == "__main__":
    app.run(host=os.getenv('APP_HOST'), port=os.getenv('APP_PORT'), debug=True)
