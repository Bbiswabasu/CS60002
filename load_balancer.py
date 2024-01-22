from flask import Flask, jsonify, request
import os

app = Flask(__name__)

class Server:
    _instance=None
    SERVER_ID=0
    RING_SIZE=512
    VIRTUAL_INSTANCE=9

    hashRing=[]  # [ 0 0 0 0]
    
    serverMap=[] # [  { server_id,server_name,virtual_loc } ]
    
    def __init__(self):
        self.hashRing=[-1 for _ in range(self.RING_SIZE)]

    def __new__(self):
        if not self._instance:
            self._instance=super(Server,self).__new__(self)
        
        return self._instance
    
    def request_hash(self,i):
        return (i**2 + 2 * i + 17) % self.RING_SIZE
    
    def virtual_server_hash(self,i,j):
        return (i**2 + j**2 + 2 * j + 25) % self.RING_SIZE
    
    def vacantRingSpot(self,current_server_id,virtual_hash):
        while(self.hashRing[virtual_hash]>=0):
            virtual_hash+=1
            virtual_hash%=self.RING_SIZE
        
        self.hashRing[virtual_hash]=current_server_id
        return virtual_hash
    
    def removeServers(self,serversToDel,serversRem):
        
        # Clean Hash Ring
        for serverIndi in serversToDel:
            for virtual_hash in serverIndi['virtual_loc']:
                self.hashRing[virtual_hash]=-1
        
        self.serverMap=serversRem


@app.route("/rep", methods=["GET"])
def rep():
    server=Server()
    try:
        response={
            "message":{
                "N":len(server.serverMap),
                "replicas":[indiServer.get("server_name") for indiServer in server.serverMap]
            },
            "status":"successful"
        }
        return jsonify(response),200
    except:
        response={
            "message":"Error in /rep in load_balancer.py",
            "status":"failure"
        }

        return jsonify(response),500

@app.route("/add",methods=["POST"])
def add():
    try:
        server=Server()
        payload=request.json
        
        if payload["n"]<len(payload["hostnames"]):
            raise 
    
        tmp_server_id=server.SERVER_ID

        while payload["n"]>len(payload["hostnames"]):
            tmp_server_id+=1
            payload['hostnames'].append(f'pub_server_{tmp_server_id}')
        
        for new_server_name in payload["hostnames"]:
            server.SERVER_ID+=1

            virtual_loc=[]
            for loop in range(0,server.VIRTUAL_INSTANCE):
                virtual_hash=server.virtual_server_hash(server.SERVER_ID,loop+1)
                virtual_loc.append(server.vacantRingSpot(server.SERVER_ID,virtual_hash))
            
            res = os.popen(f"sudo docker run --name {new_server_name} --network pub --network-alias {new_server_name} -e SERVER_ID={server.SERVER_ID} -d ds_server:latest").read()
            
            if len(res) == 0:
                raise
                     
            server.serverMap.append({
                "server_id":server.SERVER_ID,
                "server_name":new_server_name,
                "virtual_loc":virtual_loc
            })
        return rep()
    except:
        response={
            "message":"Error in /add in load_balancer.py",
            "status":"unsuccessful"
        }

        return jsonify(response),500


@app.route("/rm",methods=["DELETE"])
def rem():
    try:
        server=Server()
        payload=request.json
        
        if payload['n']<len(payload['hostnames']):
            raise

        serversToDel=[]

        for serverIndi in server.serverMap:
            if serverIndi['server_name'] in payload['hostnames']:
                serversToDel.append(serverIndi)
        
        for serverIndi in server.serverMap:
            if len(serversToDel)==payload['n']:
                break

            if serverIndi['server_name'] not in serversToDel:
                serversToDel.append(serverIndi)
        
        serversRem=[serverIndi for serverIndi in server.serverMap if serverIndi not in serversToDel]
         
        # Remove Servers and Clean up the state
        server.removeServers(serversToDel,serversRem)
        
        # Stopping the containers
        for serverIndi in serversToDel:
            res = os.popen(f"sudo docker stop {serverIndi['server_name']}").read()
            
            if len(res) == 0:
                raise
        
        return rep()
    except:
        response={
            "message":"Error in /rm in load_balancer.py",
            "status":"unsuccessful"
        }
        
        return jsonify(response),500
    
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
