from flask import Flask, Response
import json

app = Flask(__name__)


@app.route('/')
def hello():
    return 'Hello, World!'

@app.route('/dr-methods', methods=['GET'])
def list_dr_methods():
    methods = ['PCA', 'UMAP']
    resp = Response(response=json.dumps(methods), status=200, mimetype="text/plain")
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

