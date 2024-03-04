from flask import Flask, jsonify, request
import os

app = Flask(__name__)

class DB:
    DB_instance={}
    _instance=None

    def __new__(self):
        if not self._instance:
            self._instance = super(DB, self).__new__(self)
        return self._instance
    
    def addShard(self,shardName):
        self.DB_instance[shardName]=[]
        
    def addDataToShard(self,shardName,entries):
        self.DB_instance[shardName].extend(entries)
        
    def updateEntryInShard(self,shardName,entryId,newData):
        for idx, entry in enumerate(self.DB_instance[shardName]):
            if entry["Stud_id"] == entryId:
                self.DB_instance[shardName][idx] = newData
                return
        
    def removeEntryFromShard(self,shardName,entryId):
        for idx, entry in enumerate(self.DB_instance[shardName]):
            if entry["Stud_id"] == entryId:
                self.DB_instance[shardName].pop(idx)
                return
    
    def getStatus(self):
        shardNames=[]

        for shard in self.DB_instance.keys():
            shardNames.append(shard)
        
        return shardNames

    def displayEntries(self):
        return self.DB_instance


@app.route("/config",methods=["POST"])
def config():
    payload=request.json
    
    db=DB()
    for shardName in payload['shards']:
        db.addShard(shardName)
    
    shards=db.getStatus()
    
    message=""

    for shardName in shards:
        message+=f"Server0: {shardName}, "
    
    message+="configured"

    response={
        "message":message,
        "status":'successful'
    }

    return response,200

@app.route("/heartbeat",methods=["GET"])
def heartbeat():
    return jsonify({}), 200

@app.route("/write",methods=["POST"])
def write():
    payload = request.json
    shard = payload["shard"]
    entries = payload["data"]
    
    print(entries)
    db = DB()
    db.addDataToShard(shard,entries)
    print(db.displayEntries(),flush=True)
    
    response = {
        "message": "Data entries added",
        "status": "success"
    }
    return response, 200
    
@app.route("/update",methods=["PUT"])
def update():
    payload = request.json
    shard = payload["shard"]
    entryId = payload["Stud_id"]
    newData = payload["data"]
    
    db = DB()
    db.updateEntryInShard(shard,entryId,newData)
    print(db.displayEntries(),flush=True)
    
    response = {
        "message": f"Data entry with Stud_id:{entryId} updated",
        "status": "success"
    }
    
    return response, 200
    
@app.route("/del",methods=["DELETE"])
def delete():
    payload = request.json
    shard = payload["shard"]
    entryId = payload["Stud_id"]
    
    db = DB()
    db.removeEntryFromShard(shard,entryId)
    print(db.displayEntries(),flush=True)
    
    response = {
        "message": f"Data entry with Stud_id:{entryId} removed",
        "status": "success"
    }
    
    return response, 200

if __name__ == "__main__":
    app.run(debug=True,host="0.0.0.0", port=5000)
