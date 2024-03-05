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

            self.DB_instance={
                "sh1":[
                    {"Stud_id":1232,"Stud_name":"ABC","Stud_marks":25}
                ],
                "sh2":[
                    {"Stud_id":2255,"Stud_name":"GHI","Stud_marks":27}
                ]
            }
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
        shardNames = []

        for shard in self.DB_instance.keys():
            shardNames.append(shard)

        return shardNames
    
    def getData(self,shardName):
        if shardName in self.DB_instance:
            return {shardName:self.DB_instance[shardName]}
        
        return {shardName:[]}
    
    def getRecords(self,query):
        
        shardName=query["shard"]
        if shardName not in self.DB_instance:
            return []
        
        id_low=int(query["Stud_id"]["low"])
        id_high=int(query["Stud_id"]["high"])

        result=[record for record in self.DB_instance[shardName] if record['Stud_id']>=id_low and record['Stud_id']<=id_high]

        return result

    def displayEntries(self):
        return self.DB_instance


managers = []


@app.route("/config", methods=["POST"])
def config():
    payload = request.json

    # db=DB()
    message = ""
    for shardName in payload["shards"]:
        try:
            managers.append(
                Manager(
                    shardName, payload["schema"]["columns"], payload["schema"]["dtypes"]
                )
            )
            message += f"Server0: {shardName}, "
        except Exception as e:
            print(e)

    message += "configured"

    response = {"message": message, "status": "successful"}

    return response, 200


@app.route("/copy",methods=["GET"])
def copy():
    payload=request.json

    db=DB()
    
    message=[]

    for shardName in payload["shards"]:
        message.append(db.getData(shardName))
    
    response={
        "message":message,
        "status":"success"
    }
    
    return response,200


@app.route("/read",methods=["GET"])
def read():
    payload=request.json

    db=DB()

    data=db.getRecords(payload)

    response={
        "data":data,
        "status":"success"
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
    app.run(debug=True, host="0.0.0.0", port=5000)
