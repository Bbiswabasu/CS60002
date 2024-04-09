from flask import Flask, jsonify, request
import requests
import threading
import time
import os

app = Flask(__name__)


def periodic_heart_beat():

    while True:
        shardManager = ShardManager()
        shardNameToServerMap = shardManager.getShardNameToServerMap()

        for shardName in shardNameToServerMap:
            serverMap = shardNameToServerMap[shardName]

            serverMap.runPrimaryElection(shardName)

            primaryServerName = serverMap.getPrimaryServerName()

            req_body = {"shard": shardName}

            WAL_log = requests.get(
                f"http://{primaryServerName}:5000/get_wal", json=req_body
            ).json()

            serversList = serverMap.getServersList()
            for server in serversList:
                try:
                    res = requests.get(f"http://{server}:5000/heartbeat")
                    continue
                except:
                    pass

                try:
                    res = os.popen(
                        f"sudo docker run --platform linux/x86_64 --name {server} --network pub --network-alias {server} -d ds_server:latest"
                    ).read()
                    if len(res) == 0:
                        raise

                    req_body = {"logRequests": WAL_log, "shards": [shardName]}

                    while True:
                        try:
                            res = requests.post(
                                f"http://{server}:5000/config", json=req_body
                            )
                            break
                        except Exception as e:
                            print(e)
                            time.sleep(3)
                except:
                    print("Error in spawning new server")

        time.sleep(15)


class ServerMap:
    def __init__(self):
        self.primaryServerName = None
        self.serversList = []

    def printIt(self):
        for server in self.serversList:
            print(server, flush=True)

    def addServer(self, serverName):
        self.serversList.append(serverName)

    def removeServer(self, serverName):
        if serverName in self.serversList:
            self.serversList.remove(serverName)

    def runPrimaryElection(self, shardName):
        try:
            res = requests.get(f"http://{self.primaryServerName}:5000/heartbeat")
            return
        except:
            pass

        wal_count = -2
        new_server_name = None

        for serverName in self.serversList:
            try:
                req_body = {"shard": shardName}
                res = requests.get(
                    f"http://{serverName}:5000/get_wal_count", json=req_body
                ).json()

                if res["count"] > wal_count:
                    wal_count = res["count"]
                    new_server_name = serverName
            except:
                pass

        self.primaryServerName = new_server_name

    def getPrimaryServerName(self):
        return self.primaryServerName

    def getServersList(self):
        return self.serversList


class ShardManager:

    _instance = None

    def __new__(self):
        if not self._instance:
            self._instance = super(ShardManager, self).__new__(self)
            self.shardNameToServerMap = {}

        return self._instance

    def addServerToShard(self, shardName, serverName):
        if shardName not in self.shardNameToServerMap:
            self.shardNameToServerMap[shardName] = ServerMap()

        self.shardNameToServerMap[shardName].addServer(serverName)

    def getPrimaryServerForShard(self, shardName):
        serverMap = self.shardNameToServerMap[shardName]
        serverMap.runPrimaryElection()

        return serverMap.getPrimaryServerName()

    def getShardNameToServerMap(self):
        return self.shardNameToServerMap

    def removeServer(self, serverName):

        for shardName in self.shardNameToServerMap:
            serverMap = self.shardNameToServerMap[shardName]
            serverMap.removeServer(serverName)

            serverMap.runPrimaryElection()

    def printIt(self):
        for shardName, serverMap in self.shardNameToServerMap.items():
            serverMap.printIt()


@app.route("/primary-elect", methods=["GET"])
def primary_elect():
    payload = request.json

    shardManager = ShardManager()

    return {
        "primary-elect": shardManager.getPrimaryServerForShard(payload["Shard_id"])
    }, 200


@app.route("/add", methods=["POST"])
def add():
    payload = request.json
    shardManager = ShardManager()

    for serverName, shardsList in payload["servers"].items():
        for shardName in shardsList:
            shardManager.addServerToShard(shardName, serverName)
    return {"message": "Successful"}, 200


@app.route("/rm", methods=["DELETE"])
def rm():
    payload = request.json

    shardManager = ShardManager()

    try:
        for serverName in payload["servers"]:
            shardManager.removeServer(serverName)
    except:
        return {"message": "Failure in /rm route of shard-manager"}, 400

    return {"message": "Successful in /rm route of shard-manager"}, 200


# @app.route("/check", methods=["GET"])
# def check():
#     return {"message": "Success"}, 200


if __name__ == "__main__":

    thread = threading.Thread(target=periodic_heart_beat)
    thread.daemon = True  # Daemonize the thread
    thread.start()

    app.run(host="0.0.0.0", port=5000)
