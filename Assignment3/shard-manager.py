from flask import Flask, jsonify, request
import requests


app = Flask(__name__)


class ServerMap:
    def __init__(self):
        self.primaryServerName=None
        self.serversList=[]
    
    def addServer(self,serverName):
        self.serversList.append(serverName)
    
    def removeServer(self,serverName):
        if serverName in self.serversList:
            self.serversList.remove(serverName)
    
    def runPrimaryElection(self):
        try:
            res = requests.get(f"http://{self.primaryServerName}:5000/heartbeat")
        except:
            for serverName in self.serversList:
                try:
                    res = requests.get(f"http://{serverName}:5000/heartbeat")
                    self.primaryServerName=serverName
                    return
                except:
                    pass
    
    def getPrimaryServerName(self):
        return self.primaryServerName


class ShardManager:
    def __new__(self):
        if not self._instance:
            self.shardNameToServerMap={}
        
        return self._instance
    
    def addServerToShard(self,shardName,serverName):
        if shardName not in self.shardNameToServerMap:
            self.shardNameToServerMap[shardName]=ServerMap()
        
        self.shardNameToServerMap[shardName].addServer(serverName)
    
    def getPrimaryServerForShard(self,shardName):
        serverMap=self.shardNameToServerMap[shardName]
        serverMap.runPrimaryElection

        return serverMap.getPrimaryServerName()

    def removeServer(self,serverName):

        for shardName in self.shardNameToServerMap:
            serverMap=self.shardNameToServerMap[shardName]
            serverMap.removeServer(serverName)

            serverMap.runPrimaryElection()


@app.route("/primary-elect", methods=["GET"])
def primary_elect():
    payload=request.json

    shardManager=ShardManager()
    
    return {
        "primary-elect":shardManager.getPrimaryServerForShard(payload["Shard_id"])
    },200

@app.route("/add",methods=["POST"])
def add():
    payload=request.json
    ## "servers" : {"Server4":["sh3","sh5"]}
    
    shardManager=ShardManager()
    
    for serverName,shardsList in payload['server'].items():
        for shardName in shardsList:
            shardManager.addServerToShard(shardName,serverName)
    
    return {"message":"Successful"},200

@app.route("/rm",methods=["DELETE"])
def rm():
    payload=request.json

    shardManager=ShardManager()
    
    try:
        for serverName in payload['servers']:
            shardManager.removeServer(serverName)
    except:
        return {
            "message":"Failure in /rm route of shard-manager"
        },400
    
    return {
        "message":"Successful in /rm route of shard-manager"
        },200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)



