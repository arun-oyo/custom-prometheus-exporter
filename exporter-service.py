from flask import Flask, make_response

app = Flask(__name__)

BASE_PATH = "/Users/oyo"

METRICS_FILE = BASE_PATH + "/kafka-exporter/files/metrics"

@app.route('/metrics')
def index():
    result = ""
    try:
        with open(METRICS_FILE, "r") as file:
            result = file.read()
        with open(METRICS_FILE, "w") as file:
            file.write("")
        response = make_response(result, 200)
        response.mimetype = "text/plain"
        return response
    except IOError:
        response = make_response("IOError in file: " + METRICS_FILE, 500)
        return response

if __name__ == '__main__':
    from waitress import serve
    serve(app, host="0.0.0.0", port=9095)
