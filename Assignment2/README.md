# CS60002: Distributed Systems (Spring 2023)

## System Design

![image](https://github.com/Bbiswabasu/CS60002/assets/77141319/e12c5241-ba8f-4633-9e4e-a27704ee0dd7)
The repository implements a distributed load balancer with sharding in Python programming language using Flask for handling HTTP requests to interact with it over the network. Our architecture uses four classes namely ServerMap, Server, ShardMap and Shard to handle all the incoming requests and hold metadata.

<ol>
<li>ServerMap: ServerMap is a singleton class that maps server IDs to the corresponding server instances.</li>
<li>Server: Server is a class that holds the necessary information concerning the server instance - the details about the shards and the corresponding databases where they are present. This class handles the CRUD operations for the user requested data.</li>
<li>ShardMap: ShardMap is a singleton class that maps shard IDs to the corresponding shard instances.</li>
<li>Shard: Shard is a class that implements load balancing using consistent hashing for the incoming requests to the dynamically allocated servers.</li>
</ol>

### Persistent Storage Handler

We have created a database access object (DAO) called Manager that provides an abstraction for handling connections to the persistent layer. The abstraction is achieved by using three primary classes:

<ol>
<li>SQLHandler: The SQLHandler is a wrapper object that encapsulates the database connection. It uses the provided database schema and credentials to create a database table. It also houses functions for handling retrieval, insertion, deletion an updation of data.</li>
<li>DataHandler: The DataHandler allows us access to all the useful CRUD operations provided by the SQLHandler. This abstraction helps us to migrate across different database solutions such as SQL or NoSQL as well as over different providers such as MySQL, MariaDB, MongoDB, Cassandra, etc.</li>
<li>Manager: The Manager class provides the service layer object that is exposed for the controller endpoints. It is a wrapper over the DataHandler and SQLHandler.</li>
</ol>

## Assumptions

--- assumptions ---

## Challenges

<ul>
<li>MySQL database supports sequential query processing. Therefore, the concurrent requests from the producer and consumer objects poses a great issue. This leads to multiple failed attempts to test the persistent version of the broker. Eventually, the implementation synchronous Process Pool of size 1 to make the SQL queries sequential.</li>
<li>However, the above design choice may increase the latency for each request to be served.</li>
<li>Finding a way to spawn new docker containers (brokers in the cluster) in the host computer from another privileged container (Primary Manager) was bit hectic</li>
</ul>

## Prerequisites

<ol>
<li> docker: latest [Docker version 25.0.0, build e758fe5]</li>

```
sudo apt-get update

sudo apt-get install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo \
"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
$(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update

sudo apt-get install docker-ce docker-ce-cli containerd.io
```

<li> docker-compose version v2.24.2 </li>

```
sudo curl -SL https://github.com/docker/compose/releases/download/v2.15.1/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose

sudo chmod +x /usr/local/bin/docker-compose

sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
```

<li> GNU Make 4.3</li>

```
sudo apt-get -y install make
```

</ol>

## Installation Steps

Deploying Load Balancer Container: Creating Docker image, network, DNS and running the load balancer.

```
make all
```

Shutting down the load balancer and servers, deleting the network, removing images and clearing other cache.

```
make clean
```
