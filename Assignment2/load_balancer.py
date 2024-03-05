from flask import Flask, jsonify, request
import uuid


app = Flask(__name__)

def generate_random_id():
    unique_id = uuid.uuid4()
    return int(unique_id.int)


class Server:

    shardsToDB={}
    server_id=-1

    def __init__(self,id):
        self.server_id=id
    
    def addShard(self,shard_id):
        self.shardsToDB[shard_id]="Database Instance"

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

        return self._instance
    
    def addServer(self,server_name):
        unique_id=generate_random_id()
        self.nameToIdMap[server_name]=unique_id
        self.idToServer[unique_id]=Server(unique_id)
    
    def addShardToServer(self,server_id,shard_id):
        server=self.idToServer[server_id]
        server.addShard(shard_id)

    def getIdFromName(self,server_name):
        return self.nameToIdMap[server_name]
    
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
    serverMap = []

    def __init__(self,shard_id,student_id_low,shard_size):
        self.shard_id=shard_id
        self.student_id_low=student_id_low
        self.shard_size=shard_size
        self.hashRing = [-1 for _ in range(self.RING_SIZE)]

    
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
    
    def __str__(self):
        return f'shard_id - {self.shard_id} \n student_id_low - {self.student_id_low} \n shard_size - {self.shard_size}'



class ShardMap:

    _instance=None

    nameToIdMap={}
    idToShard={}

    def __new__(self):
        if not self._instance:
            self._instance = super(ShardMap, self).__new__(self)

        return self._instance
    
    def getIdFromName(self,shard_name):
        return self.nameToIdMap[shard_name]
    
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

    
    print(shardMap)
    print(serverMap)
    
    # print(shardMap.nameToIdMap)
    # print(serverMap.nameToIdMap)

    response={
        "message":"Configured Database",
        "status":"success"
    }

    return response,200



if __name__ == "__main__":
    app.run(debug=True,host="0.0.0.0", port=5000)