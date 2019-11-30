from flask import Flask, jsonify, request
from flask_restful import Api

app = Flask(__name__)
api = Api(app)

@app.route('/submit', methods=["POST"])
def Check_data():
    #get data from Back-End post
    data_format= request.get_json()
    # return jsonify(data_format)

    source = data_format["source"]
    testcase = data_format["testcase"]
    Languageld = data_format["Languageld"]
    token = data_format["token"]
    timelimit = data_format["timeLimit"]
    meomorylimit = data_format["meomoryLimit"]
    submissionld = data_format["submissionld"]

    if True:
        return jsonify({"message" : "Successful submit"}), 200
    elif missing_element:
        return jsonify({"message" : "Request body property missing"}), 400
    elif token_error:
        return jsonify({"message" : "Request body property missing"}), 403
    elif server_error:
        return jsonify({"message" : "Request body property missing"}), 500

@app.route('/response')
def result():
    pass


if __name__=="__main__":
    app.run(debug=True)
