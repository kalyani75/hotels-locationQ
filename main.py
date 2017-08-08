import os

from flask import Flask
from flask_cors import CORS

from location import location

app = Flask(__name__)

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

app.register_blueprint(location, url_prefix='/hotels.com')

port = os.getenv('PORT', '9011')
if __name__ == "__main__":
  app.run(host='0.0.0.0', port=int(port), debug=True)