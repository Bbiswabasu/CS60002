from flask import Flask, jsonify, request
import os
from manager import Manager

app = Flask(__name__)


class DB:
    DB_instance = {}
    _instance = None

    def __new__(self):
        if not self._instance:
            self._instance = super(DB, self).__new__(self)
        return self._instance

    def addShard(self, shardName):
        self.DB_instance[shardName] = []

    def getStatus(self):
        shardNames = []

        for shard in self.DB_instance.keys():
            shardNames.append(shard)

        return shardNames


managers = []


@app.route("/config", methods=["POST"])
def config():
    payload = request.json

    # db=DB()
    message = ""
    error = ""
    for shardName in payload["shards"]:
        try:
            managers.append(
                Manager(
                    shardName, payload["schema"]["columns"], payload["schema"]["dtypes"]
                )
            )
            message += f"Server0: {shardName}, "
        except Exception as e:
            error += f"Server0: {shardName},"

    message += "configured |"
    message += error

    response = {"message": message, "status": "successful"}

    return response, 200


@app.route("/heartbeat", methods=["GET"])
def heartbeat():
    return jsonify({}), 200


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
