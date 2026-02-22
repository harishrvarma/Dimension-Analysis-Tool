import config  # loads .env immediately
import os
from flask import Flask
from routes.index import register_blueprints


app = Flask(__name__)
register_blueprints(app)

if __name__ == "__main__":
    app.run(host=os.getenv('APP_HOST'), port=os.getenv('APP_PORT'), debug=True)
