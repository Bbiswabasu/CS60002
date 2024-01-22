from flask import Flask, request, g
import sys
import os
import requests

app = Flask(__name__)
newServerId = 1
hashToServer = {}
serverToHash = {}
allHosts = []
M = 512
K = 9


def H(i):
    return (i**2 + 2 * i + 17) % M


def phi(i, j):
    return (i**2 + j**2 + 2 * j + 25) % M


@app.route("/rep", methods=["GET"])
def rep():
    response = {
        "message": {"N": len(allHosts), "replicas": allHosts},
        "status": "successful",
    }
    return response, 200


@app.route("/add", methods=["POST"])
def add():
    global newServerId
    payload = request.json
    numberOfServers = payload["n"]
    hostNames = payload["hostnames"]
    for i in range(numberOfServers):
        hostName = "pub_" + str(newServerId)
        if (i < len(hostNames)) and (hostNames[i] not in allHosts):
            hostName = hostNames[i]
        allHosts.append(hostName)
        res = os.popen(
            f"sudo docker run --name {hostName} --network pub --network-alias {hostName} -e serverId={newServerId} -d ds_server:latest"
        ).read()
        for virtualServer in range(K):
            pos = phi(newServerId, virtualServer)
            # Handle when it is full
            while pos in hashToServer.keys():
                pos += 1
                pos %= M
            hashToServer[pos] = newServerId
            if newServerId not in serverToHash.keys():
                serverToHash[newServerId] = []
            serverToHash[newServerId].append(pos)
        if len(res) == 0:
            print(f"Error starting the container")
            return {"message": "Unable to create server instance(s)!"}, 500
        else:
            print(f"Container for Server ID: {newServerId} started")
            newServerId += 1
    return {
        "message": {"N": len(allHosts), "replicas": allHosts},
        "status": "successful",
    }, 200


@app.route("/rm", methods=["DELETE"])
def remove():
    pass

@app.route('/<path:path>', methods=['GET'])
def catch_all(path):
    hostname = "pub_2"
    req_url = f'http://{hostname}:5000/{path}'
    response = requests.get(req_url)
    print(response.text)
    return ""
    # return response.json()

# @app.route("/:i")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
