from flask import Flask, jsonify, request
import threading
import uuid


app = Flask(__name__)

def generate_random_id():
    unique_id = uuid.uuid4()
    return int(unique_id.int)

class MultiLockDict:
    _instance=None

    def __new__(self):
        if not self._instance:
            self._instance = super(MultiLockDict, self).__new__(self)
            self.lock_dict = {}
            self.global_lock = threading.Lock()

        return self._instance

    def acquire_lock(self, key):
        with self.global_lock:
            if key not in self.lock_dict:
                self.lock_dict[key] = threading.Lock()
        self.lock_dict[key].acquire()

    def release_lock(self, key):
        self.lock_dict[key].release()


class Server:

    shardsToDB={}
    server_id=-1

    def __init__(self,id):
        self.server_id=id
        self.shardsToDB={}
    
    def addShard(self,shard_id):
        self.shardsToDB[shard_id]="Database Instance"
    
    def updateData(self,shard_id,data):
        ## Implement the updating logic

        print(shard_id,data)
        print("--------")
    
    def delData(self,shard_id,stud_id):

        ## Implement the deleting logic

        print(shard_id,stud_id)
        print("------")

    def insertData(self,shard_id,data):
        ## Implement the inserting logic
        print(shard_id,data)
        print("-------")

    def getData(self,shard_id,id_limits):
        ## Implement the logic

        return [
            {"Stud_id":1000,"Stud_name":"PQR","Stud_marks":23},
            {"Stud_id":1001,"Stud_name":"STV","Stud_marks":22},
            {"Stud_id":8888,"Stud_name":"ZQN","Stud_marks":65},
            {"Stud_id":8889,"Stud_name":"BYHS","Stud_marks":76}
        ]
    def getStatus(self):
        res=[]
        for key in self.shardsToDB:
            res.append(key)
        
        return res
    
    def __str__(self):
        res=f"server_id - {self.server_id}\n"

        for key,value in self.shardsToDB.items():
            res+=f"{key} - {value}\n"
        res+="]\n"
        return res

        
class ServerMap:

    _instance=None

    nameToIdMap={}
    idToServer={}

    def __new__(self):
        if not self._instance:
            self._instance = super(ServerMap, self).__new__(self)
            self.nameToIdMap={}
            self.idToServer={}

        return self._instance
    
    def getServersCount(self):
        return len(self.nameToIdMap)
    
    def addServer(self,server_name):
        unique_id=generate_random_id()
        self.nameToIdMap[server_name]=unique_id
        self.idToServer[unique_id]=Server(unique_id)
    
    def addShardToServer(self,server_id,shard_id):
        server=self.idToServer[server_id]
        server.addShard(shard_id)

    def getIdFromName(self,server_name):
        return self.nameToIdMap[server_name]
    
    def getData(self,shardFragment,id_limits):

        server=self.idToServer[shardFragment["server_id"]]

        return server.getData(shardFragment["shard_id"],id_limits)
    
    def getStatus(self):
        res={}

        for key,value in self.nameToIdMap.items():
            res[key]=self.idToServer[value].getStatus()
        
        return res
    
    def insertBulkData(self,serversList,shard_id,data):
        for server_id in serversList:
            server=self.idToServer[server_id]
            server.insertData(shard_id,data)
    
    def updateData(self,serversList,shard_id,data):
        for server_id in serversList:
            server=self.idToServer[server_id]
            server.updateData(shard_id,data)
    
    def delData(self,serversList,shard_id,stud_id):
        for server_id in serversList:
            server=self.idToServer[server_id]
            server.delData(shard_id,stud_id)

    def __str__(self):
        res="NameToIDMap - [ \n "

        for key,value in self.nameToIdMap.items():
            res+=f"{key} - {value} \n"
        
        res+="]\n"
        
        
        res+="IDToServer - [\n"

        for key,value in self.idToServer.items():
            res+=f"{key} - {value.__str__()}\n"
        
        res+="]\n"
        return res

    
class Shard:
    
    shard_id=-1
    student_id_low=-1
    shard_size=0

    RING_SIZE = 512
    VIRTUAL_INSTANCE = 9

    hashRing = []

    def __init__(self,shard_id,student_id_low,shard_size):
        self.shard_id=shard_id
        self.student_id_low=student_id_low
        self.shard_size=shard_size
        self.hashRing = [-1 for _ in range(self.RING_SIZE)]
    
    def isDataPresent(self,id_limits):
        id_low=self.student_id_low
        id_high=self.student_id_low+self.shard_size

        if id_low>id_limits['high'] or id_high<id_limits['low']:
            return False
        
        return True
    
    def getStudentIdLow(self):
        return self.student_id_low
    
    def getShardSize(self):
        return self.shard_size

    
    def request_hash(self, i):
        return (i**2 + 2 * i + 17) % self.RING_SIZE

    def virtual_server_hash(self, i, j):
        return (i**2 + j**2 + 2 * j + 25) % self.RING_SIZE

    def vacantRingSpot(self,virtual_hash):
        while self.hashRing[virtual_hash] >= 0:
            virtual_hash += 1
            virtual_hash %= self.RING_SIZE

        return virtual_hash
    
    def addServer(self,server_id):
        for loop in range(0, self.VIRTUAL_INSTANCE):
            virtual_hash = self.virtual_server_hash(server_id, loop + 1)
            emptyRingSpot=self.vacantRingSpot(virtual_hash)
            self.hashRing[emptyRingSpot]=server_id
    
    def getLoadBalancedServerId(self,request_id):
        mapped_index=request_id%self.RING_SIZE

        while self.hashRing[mapped_index]<0:
            mapped_index+=1
            mapped_index%=self.RING_SIZE
        
        return self.hashRing[mapped_index]

    def getAllServers(self):

        serversDict={}
        for server_id in self.hashRing:
            if server_id>=0:
                serversDict[server_id]=1
        
        return list(serversDict.keys())

    def __str__(self):
        return f'shard_id - {self.shard_id} \n student_id_low - {self.student_id_low} \n shard_size - {self.shard_size}'



class ShardMap:

    _instance=None

    nameToIdMap={}
    idToShard={}

    def __new__(self):
        if not self._instance:
            self._instance = super(ShardMap, self).__new__(self)
            self.nameToIdMap={}
            self.idToShard={}

        return self._instance
    
    def getIdFromName(self,shard_name):
        return self.nameToIdMap[shard_name]
    
    def getAllServersFromShardId(self,shard_id):
        return self.idToShard[shard_id].getAllServers()
    
    def getShardIdFromStudId(self,student_id):

        for shard_id,shard in self.idToShard.items():
            id_limits={
                "low":student_id,
                "high":student_id
            }

            if shard.isDataPresent(id_limits):
                return shard_id
            
    def getNameFromId(self,shard_id):
        
        for key,value in self.nameToIdMap.items():
            if shard_id==value:
                return key
            
        return "NA Shard"

    def addShard(self,shard):
        shard_name=shard['Shard_id']
        student_id_low=shard['Stud_id_low']
        shard_size=shard['Shard_size']

        unique_id=generate_random_id()
        self.nameToIdMap[shard_name]=unique_id
        self.idToShard[unique_id]=Shard(unique_id,student_id_low,shard_size)
    
    def addServerToShard(self,shard_name,server_id):
        shard_id=self.nameToIdMap[shard_name]
        shard=self.idToShard[shard_id]

        shard.addServer(server_id)

    
    def getStatus(self):
        
        res=[]
        for key,value in self.nameToIdMap.items():

            currRes={
                "Shard_id":key,
                "Stud_id_low":self.idToShard[value].getStudentIdLow(),
                "Shard_size":self.idToShard[value].getShardSize()
            }

            res.append(currRes)
        
        return res

    def getShardFragments(self,id_limits):
        
        shardFragments=[]

        for shard_id,shard in self.idToShard.items():
            if shard.isDataPresent(id_limits):
                request_id=generate_random_id()
                shardFragment={
                    "shard_id":shard_id,
                    "server_id":shard.getLoadBalancedServerId(request_id)
                }
                shardFragments.append(shardFragment)
        
        return shardFragments

    def __str__(self):
        res="NameToID - [\n "

        for key,value in self.nameToIdMap.items():
            res+=f"{key} - {value},\n "
        res+=" ] \n"
        
        res+="IDToShard - [ \n"

        for key,value in self.idToShard.items():
            res+=f"{key} - {value.__str__()}, \n"
        
        res+=" ] \n"
        
        return res




@app.route("/init",methods=["POST"])
def init():
    payload=request.json
    
    shardMap=ShardMap()
    serverMap=ServerMap()


    for shard in payload['shards']:
        shardMap.addShard(shard)
    
    for server_name,shards in payload['servers'].items():
        serverMap.addServer(server_name)
        
        server_id=serverMap.getIdFromName(server_name)
        

        for shard in shards:
            shardMap.addServerToShard(shard,server_id)

            shard_id=shardMap.getIdFromName(shard)
            
            serverMap.addShardToServer(server_id,shard_id)
    

    response={
        "message":"Configured Database",
        "status":"success"
    }

    return response,200


@app.route("/status",methods=["GET"])
def status():
    
    serverMap=ServerMap()
    shardMap=ShardMap()

    shards=shardMap.getStatus()
    servers=serverMap.getStatus()

    newServers={}

    for key,value in servers.items():
        shardNames=[]
        for shardId in value:
            shardNames.append(shardMap.getNameFromId(shardId))
        
        newServers[key]=shardNames

    response={
        "shards":shards,
        "servers":newServers
    }

    return response,200

@app.route("/add",methods=["POST"])
def add():
    try:
        payload=request.json

        serverMap=ServerMap()
        shardMap=ShardMap() 
        
        if payload['n']>len(payload['servers']):
            raise Exception(
             "<Error> Number of new servers (n) is greater than newly added instances"
            )
        
        for shard in payload['new_shards']:
            shardMap.addShard(shard)
        
        addedServerNames=[]

        for server_name,shards in payload['servers'].items():
             
            if "[" in server_name:
                server_name=f"Server{generate_random_id()%10000}"

            addedServerNames.append(server_name)

            serverMap.addServer(server_name)
            
            server_id=serverMap.getIdFromName(server_name)
            
            for shard in shards:
                shardMap.addServerToShard(shard,server_id)
                shard_id=shardMap.getIdFromName(shard)
                serverMap.addShardToServer(server_id,shard_id)
        response={}

        response["N"]=serverMap.getServersCount()
        
        message="Added "
        for server in addedServerNames:
            message+=f"{server}, "
        message+="successfully"

        response["message"]=message
        response["status"]="successfull"

        return response,200
    except Exception as e:
        response={
            "message":str(e),
            "status":"failure"
        }

        return response,400
        

@app.route("/read",methods=["GET"])
def read():
    payload=request.json

    shardMap=ShardMap()
    shardFragments=shardMap.getShardFragments(payload["Stud_id"])
    
    result=[]
    
    serverMap=ServerMap()

    for shardFragment in shardFragments:
        data=serverMap.getData(shardFragment,payload['Stud_id'])
        for _ in data:
            result.append(_)
            

    response={
        "shards_queried":[],
        "data":result,
        "status":"success"
    }

    for shardFragment in shardFragments:
        response["shards_queried"].append(shardMap.getNameFromId(shardFragment["shard_id"]))
    
    return response,200

@app.route("/write",methods=["POST"])
def write():
    payload=request.json

    shardWiseData={}
    
    shardMap=ShardMap()
    serverMap=ServerMap()

    for data in payload["data"]:
        shard_id=shardMap.getShardIdFromStudId(data["Stud_id"])

        if shard_id not in shardWiseData:
            shardWiseData[shard_id]=[data]
        else:
            shardWiseData[shard_id].append(data)
    
    multi_lock_dict = MultiLockDict()

    for shard_id,data in shardWiseData.items():

        multi_lock_dict.acquire_lock(shard_id)

        try:
            serversList=shardMap.getAllServersFromShardId(shard_id)
            serverMap.insertBulkData(serversList,shard_id,data)
        except:
            multi_lock_dict.release_lock(shard_id)
    
    response={
        "message":f"{len(payload['data'])} Data entries added",
        "status":"success"
    }

    return response,200

@app.route("/update",methods=["PUT"])
def update():
    payload=request.json
    
    shardMap=ShardMap()
    serverMap=ServerMap()

    shard_id=shardMap.getShardIdFromStudId(payload["Stud_id"])
    
    multi_lock_dict = MultiLockDict()
    multi_lock_dict.acquire_lock(shard_id)

    try:
        serversList=shardMap.getAllServersFromShardId(shard_id)
        serverMap.updateData(serversList,shard_id,payload["data"])
    except:
        multi_lock_dict.release_lock(shard_id)
    
    response={
        "message":f"Data entry for Stud_id - {payload['Stud_id']} updated",
        "status":"success"
    }

    return response,200

@app.route("/del",methods=["DELETE"])
def delete():
    payload=request.json
    
    shardMap=ShardMap()
    serverMap=ServerMap()

    shard_id=shardMap.getShardIdFromStudId(payload["Stud_id"])
    
    multi_lock_dict = MultiLockDict()
    multi_lock_dict.acquire_lock(shard_id)

    try:
        serversList=shardMap.getAllServersFromShardId(shard_id)
        serverMap.delData(serversList,shard_id,payload["Stud_id"])
    except:
        multi_lock_dict.release_lock(shard_id)
    
    response={
        "message":f"Data entry for Stud_id - {payload['Stud_id']} removed from all replicas",
        "status":"success"
    }

    return response,200



if __name__ == "__main__":
    app.run(debug=True,host="0.0.0.0", port=5000)