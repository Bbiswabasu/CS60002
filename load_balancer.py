from flask import Flask, jsonify, request
import uuid
import requests
import os
import random

app = Flask(__name__)


def generate_random_id():
    unique_id = uuid.uuid4()
    return int(unique_id.int)


class Server:
    _instance = None

    SERVER_ID = 0
    RING_SIZE = 512
    VIRTUAL_INSTANCE = 9

    hashRing = []  # [ 0 0 0 0]

    serverMap = []  # [  { server_id,server_name,virtual_loc } ]

    def __new__(self):
        if not self._instance:
            self._instance = super(Server, self).__new__(self)
            self.hashRing = [-1 for _ in range(self.RING_SIZE)]

        return self._instance

    def request_hash(self, i):
        return (i**2 + 2 * i + 17) % self.RING_SIZE

    def virtual_server_hash(self, i, j):
        return (i**2 + j**2 + 2 * j + 25) % self.RING_SIZE

    def vacantRingSpot(self, current_server_id, virtual_hash):
        while self.hashRing[virtual_hash] >= 0:
            virtual_hash += 1
            virtual_hash %= self.RING_SIZE

        self.hashRing[virtual_hash] = current_server_id
        return virtual_hash

    def addServers(self, serversToAdd):
        for new_server_name in serversToAdd:
            random_id = generate_random_id()

            self.SERVER_ID += 1

            virtual_loc = []
            for loop in range(0, self.VIRTUAL_INSTANCE):
                virtual_hash = self.virtual_server_hash(random_id, loop + 1)
                virtual_loc.append(self.vacantRingSpot(random_id, virtual_hash))

            res = os.popen(
                f"sudo docker run --name {new_server_name} --network pub --network-alias {new_server_name} -e SERVER_ID={self.SERVER_ID} -d ds_server:latest"
            ).read()

            if len(res) == 0:
                raise

            self.serverMap.append(
                {
                    "server_id": random_id,
                    "server_name": new_server_name,
                    "virtual_loc": virtual_loc,
                }
            )

    def removeServers(self, serversToDel):
        # Clean Hash Ring
        for serverIndi in serversToDel:
            for virtual_hash in serverIndi["virtual_loc"]:
                self.hashRing[virtual_hash] = -1

        # Stopping the containers
        for serverIndi in serversToDel:
            res = os.popen(
                f"sudo docker stop {serverIndi['server_name']} && sudo docker rm {serverIndi['server_name']}"
            ).read()

            if len(res) == 0:
                raise

        serversRem = [
            serverIndi
            for serverIndi in self.serverMap
            if serverIndi not in serversToDel
        ]
        self.serverMap = serversRem

    def __del__(self):
        self.removeServers(self.serverMap, [])

    def getServerName(self, serverId):
        for server in self.serverMap:
            if serverId == server["server_id"]:
                return server["server_name"]
        return None

    def mapRequest(self, requestId):
        requestHash = self.request_hash(requestId)
        while self.hashRing[requestHash] == -1:
            requestHash += 1
            requestHash %= self.RING_SIZE
        serverName = self.getServerName(self.hashRing[requestHash])
        return serverName


@app.route("/rep", methods=["GET"])
def rep():
    server = Server()
    try:
        response = {
            "message": {
                "N": len(server.serverMap),
                "replicas": [
                    indiServer.get("server_name") for indiServer in server.serverMap
                ],
            },
            "status": "successful",
        }
        return jsonify(response), 200
    except:
        response = {"message": "Error in /rep in load_balancer.py", "status": "failure"}

        return jsonify(response), 500


@app.route("/add", methods=["POST"])
def add():
    try:
        server = Server()
        payload = request.json

        if payload["n"] < len(payload["hostnames"]):
            raise

        tmp_server_id = server.SERVER_ID

        while payload["n"] > len(payload["hostnames"]):
            tmp_server_id += 1
            payload["hostnames"].append(f"pub_server_{tmp_server_id}")

        server.addServers(payload["hostnames"])

        return rep()
    except:
        response = {
            "message": "<Error> Length of hostname list is more than newly added instances",
            "status": "failure",
        }
        return jsonify(response), 400


@app.route("/rm", methods=["DELETE"])
def rem():
    try:
        server = Server()
        payload = request.json

        if payload["n"] < len(payload["hostnames"]):
            raise

        serversToDel = []

        for serverIndi in server.serverMap:
            if serverIndi["server_name"] in payload["hostnames"]:
                serversToDel.append(serverIndi)

        for serverIndi in server.serverMap:
            if len(serversToDel) == payload["n"]:
                break

            if serverIndi["server_name"] not in serversToDel:
                serversToDel.append(serverIndi)

        # Remove Servers and Clean up the state
        server.removeServers(serversToDel)

        return rep()
    except:
        response = {
            "message": "<Error> Length of hostname list is more than removable instances",
            "status": "failure",
        }

        return jsonify(response), 400


@app.route("/<path:path>", methods=["GET"])
def balancer(path):
    try:
        server = Server()
        hostname = server.mapRequest(random.randint(int(1e5), int(1e6) - 1))
        req_url = f"http://{hostname}:5000/{path}"
        response = requests.get(req_url)
        return response.json(), response.status_code
    except:
        response = {
            "message": f"<Error> {path} endpoint does not exist in server replicas",
            "status": "unsuccessful",
        }
        return jsonify(response), 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
