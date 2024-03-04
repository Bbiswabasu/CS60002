from flask import Flask, jsonify, request
import os

app = Flask(__name__)

class DB:
    DB_instance={}
    _instance=None

    def __new__(self):
        if not self._instance:
            self._instance = super(DB, self).__new__(self)

            self.DB_instance={
                "sh1":[
                    {"stud_id":1232,"Stud_name":"ABC","Stud_marks":25}
                ],
                "sh2":[
                    {"stud_id":2255,"Stud_name":"GHI","Stud_marks":27}
                ]
            }
        return self._instance
    
    def addShard(self,shardName):
        self.DB_instance[shardName]=[]
    
    def getStatus(self):
        shardNames=[]

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
        
        id_low=int(query["stud_id"]["low"])
        id_high=int(query["stud_id"]["high"])

        result=[record for record in self.DB_instance[shardName] if record['stud_id']>=id_low and record['stud_id']<=id_high]

        return result


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


if __name__ == "__main__":
    app.run(debug=True,host="0.0.0.0", port=5000)
