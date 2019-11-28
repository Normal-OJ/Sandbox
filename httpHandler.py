from flask import Flask, jsonify, request
from flask_restful import Api, Resourse

app = Flask(__name__)
api = Api(app)

def checkPostedData (postedData, functionName):

    if (functionName == "extract"):
        if not_meet_requirement:
            return ERROR
        else:
            return 200

class extract(Resourse):
    def post(self):

        #get posted data
        postedData = request.get_json()

        #verify posted data
        status_code = checkPostedData(postedData, "extract")
        if (status_code!=200):
            retJson{
                "Massage": "An error happend",
                "status code":status_code
            }
            return jsonify(retJson)
        # status_code == 200


api.add_resource(extract, "/exract")

@app.route('/submission', methods=["POST"])
def submission():
    pass

@app.route('/response')
def result():
    pass

if __name__=="__main__":
    app.run(debug=True)
