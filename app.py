#!flask/bin/python
from flask import Flask, make_response
from prometheus import *

app = Flask(__name__)

@app.route('/metrics')
def index():
    response = make_response(prometheus(), 200)
    response.mimetype = "text/plain"
    return response

if __name__ == '__main__':
    app.run(host="0.0.0.0")
