# PotionDB

A geo-distributed database featuring partial replication with transactional causal consistency.
PotionDB leverages on Conflict-free Replicated Datatypes (CRDTs) to offer a rich library of datatypes that stay always consistent.
Furthermore, PotionDB offers efficient materialized views which can even be built upon data that is partitioned across multiple servers.
The size of materialized views stays small compared to the size of the data it refers, through the usage of non-uniform replication techniques.

Clients can interact with PotionDB using its protobuf interface.
PotionDB's protobuf interface extends AntidoteDB's interface, thus AntidoteDB clients implementing its protobuf interface can be used in PotionDB with no changes required.
AntidoteDB clients can be found here: https://github.com/orgs/AntidoteDB/repositories.
A PotionDB specific client implementing the [TPC-H benchmark](https://www.tpc.org/tpch/) can be found here: https://github.com/AndreRijo/TPCH-Client.

Objects in the database are uniquely identified by a triple (*id*, *type*, *bucket*).
*Id* is the name intended for the object, *type* represents the object's datatype while *bucket* represents to which "group" the object should belong.
Each PotionDB server replicates a subset (potentially all) of all buckets existing in the system, thus achieving fine-grained partial replication.

#### Notes for Eurosys reviewers

The version used to produce the results in our paper is ready in the Docker image andrerj/potiondb. Please use said version to reproduce any results obtained in the paper. Instructions on how to run PotionDB can be found [here](#Getting-Started) and [here](#Running-multiple-instances-of-PotionDB). You will also want to check the [PotionDB's TPC-H Client](https://github.com/AndreRijo/TPCH-Client), which implements TPC-H's benchmark and includes instructions on how to prepare and run a TPC-H workload.
Furthermore, to reproduce the PostgreSQL experiments in our paper, please refer to our [PostgreSQL TPC-H Go Client](https://github.com/AndreRijo/postgres-tpch-client).

If building from source is a must, please refer to the instructions in [Building from source](#Building-from-source) and use the master branch
Usage of other branches may contain modifications/future work not included in the paper which may affect the results.
The master branch will remain stable during the review process of the paper.
Any future modification to the master branch after the review process will be proceeded by a copy of the current state of master to another branch.

List of other relevant repositories:

- [PotionDB's TPC-H Client](https://github.com/AndreRijo/TPCH-Client)
- [TPCH Locality Tool](https://github.com/AndreRijo/TPCH-LocalityTool)
- [TPCH Data Processor](https://github.com/AndreRijo/tpch-data-processor/)
- [PostgreSQL TPC-H Go Client](https://github.com/AndreRijo/postgres-tpch-client)
- [PostgreSQL TPC-H GO Library](https://github.com/AndreRijo/postgres-tpch-go-lib)
- [DockerManager](https://github.com/AndreRijo/DockerManager)
- [Test configurations repository](https://github.com/AndreRijo/potiondb-vldb-configs-rep)
- [PotionDB SQL](https://github.com/AndreRijo/potiondbSQL)
- [Go Tools](https://github.com/AndreRijo/go-tools)

## Index
- [Setting up TPC-H's dataset](#setting-up-tpc-hs-dataset)
- [Getting started](#getting-started)
- [Customizing PotionDB](#customizing-potiondb)
- [Building from source](#building-from-source)
- [Building and running from source without docker](#building-and-running-from-source-without-docker)
- [Running multiple instances of PotionDB](#running-multiple-instances-of-potiondb)
- [Configuring partial replication](#configuring-partial-replication)
- [Simulating latency between servers](#simulating-latency-between-servers)

## Setting up TPC-H's dataset

Note: If you do not intend to evaluate PotionDB with the TPC-H benchmark, you can skip to [Getting Started](#getting-started).

In order to evaluate PotionDB with the TPC-H benchmark, it is first necessary to generate its dataset.
The dataset can be loaded into PotionDB either by PotionDB itself or by [PotionDB's TPC-H Client](https://github.com/AndreRijo/TPCH-Client).
Setting up TPC-H's dataset can be summarized into the following tasks:

1. Download [TPC-H tools](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) and generate the dataset;
2. (Optionally) Modify the dataset to give locality to the data (For the motivation and explanation on this, please check [here])(https://github.com/AndreRijo/TPCH-LocalityTool).

**Note**: For Eurosys reviewers, step 2 is obligatory in order to reproduce the results reported in the paper.

### TPC-H tool and dataset generation

First, download the TPC-H tools [here](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
This will be necessary to generate the TPC-H dataset.

Afterwards, you will need to build the tool.
Refer to the README file included with the tool for instructions, as well as the [TPC-H specification](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).

For the parameters of the makefile, use the following:

```
DATABASE=ORACLE
WORKLOAD=TPCH
```

To generate the dataset and the set of updates to be used, run, respectively:

```
dbgen -s 1
dbgen -s 1 -u 1000
```

Note: If testing PotionDB on a single machine with modest hardware, you may want to test with a lower `-s` (for example, 0.1).
While the tool works for values under 1, said values are not officially supported by TPC-H.

Finally, organize the dataset and updates using the following folder structure:

```
tpch_data\
    tables\$sSF\
    upds\$sSF\
    headers\
```

Where `$s`corresponds to the value used for `-s` when generating the dataset.
For example, for `-s 1`:

```
tpch_data\
    tables\1SF\
    upds\1SF\
    headers\
```

Put all generated tables and updates, respectively, under `tables\$sSF` and `upds\$sSF`.
Fill the `headers` folder with the contents of the [tpch_headers folder](./tpch_headers) in this repository.

Remark: While PotionDB has no use for the files generated in `upds`, they are required for [PotionDB's TPC-H client](https://github.com/AndreRijo/TPCH-Client) to issue updates on the dataset.

### Modifying the dataset for locality

Use the TPC-H Locality tool that we have made, available here: https://github.com/AndreRijo/TPCH-LocalityTool.

After the new dataset and updates are generated by the TPC-H Locality tool, replace the contents of the folders `tables` and `upds` with the new data.
The new data will be inside a folder named `_mod` that is inside both `tables` and `upds`.

## Getting Started

The easiest way to test PotionDB is to use its pre-compiled Docker image, which already contains all dependencies. To install Docker, please refer to https://docs.docker.com/engine/install/

Afterwards, run:

```
docker pull andrerj/potiondb
```

To start one instance of PotionDB:

```
docker run -p 8087:8087 -p 5672:5672 --name potiondb andrerj/potiondb
```

This will start an instance of PotionDB named *potiondb*. Port 8087 is used for clients to communicate with, while port 5672 is used for replication purposes. Parameter -p associates the internal ports of the container to the ports of your device. The ports that get associated to your device can be changed if so is desired (https://docs.docker.com/engine/reference/commandline/run/). The ports of PotionDB itself can also be changed through its configuration files in the [configs folder](configs).

The following message will appear when PotionDB is ready to serve clients:

```
PotionDB started at port 8087 with ReplicaID 23781
```

`ReplicaID` will vary between executions, as said ID is generated randomly.
To stop PotionDB, execute the following in another terminal:

```
docker stop -t 1 potiondb
```

## Customizing PotionDB

#### PotionDB with pre-made configuration files

To run PotionDB with one of the pre-made configuration files:

```
docker run -p 8087:8087 -p 5672:5672 -e CONFIG=/go/bin/configs/cluster/default --name potiondb andrerj/potiondb 
```

You can swap `cluster/default` with the path to any of the configuration files present in the [configs folder](configs).

#### Custom-made configurations

To run PotionDB with a custom made configuration, without having to rebuild the image:

```
docker run -p 8087:8087 -p 5672:5672 -v /FULL_PATH_TO_YOUR_FOLDER_WITH_CONFIG/:/go/bin/extern/ -e CONFIG=/go/bin/extern/ --name potiondb andrerj/potiondb
```

Parameter -v shares the folder with the desired configuration file with the container.
PotionDB will automatically use the configuration file inside the shared folder. 
Depending on your operating system and Docker installation, you may need to set up your Docker to allow sharing of folders.

Many different settings can be changed through configuration files - check [protoServer.go](src/main/protoServer.go), method `loadConfigs()` for a list of settings.
It is suggested to use one of the existing configuration files as a starting point.
[These ones are a good starting point](configs/singlePC/docker), as they showcase how to have multiple servers connected and achieve partial replication.

You can find out [here how to run multiple PotionDB instances](Running-multiple-instances-of-PotionDB), or [how to configure PotionDB's partial replication](Configuring-partial-replication).


#### Advanced usage

For advanced users, it is also possible to pass specific PotionDB parameters by setting environment variables on the creation of a docker container.
A list of possible configurations and their environment names can be found on [src/main/protoServer.go](src/main/protoServer.go), method `loadConfigs()` and [dockerStuff/start.sh](dockerStuff/start.sh).
For instructions on how to use environment variables (-e) on docker, as well as how to share folders/files with Docker (-v), please check https://docs.docker.com/engine/reference/commandline/run/.
For most users, however, modifying one of the existing configuration files should be simpler and cleaner.

A complete example:

```
docker run -p 8087:8087 -p 5672:5672 -v /home/myname/myPotionDBConfigs/myConfig1/:/go/bin/extern/ -e CONFIG=/go/bin/extern/ -e REPLICA_ID=12345 --name potiondb andrerj/potiondb
```


## Building from source

First, prepare a folder to hold all the required files.
We'll refer to such folder as the `baseFolder`.

Afterwards, go inside the `baseFolder` and clone the following repositories:

```
git clone https://github.com/AndreRijo/potionDB.git potionDB
git clone https://github.com/AndreRijo/tpch-data-processor.git tpch_data_processor
git clone https://github.com/AndreRijo/potiondbSQL sqlToKeyValue
```

Please ensure all repositories are on the master branch.
If unsure, go into the folder of each repository and run:

```
git checkout master
```

If not already done, install Docker. for reference: https://docs.docker.com/engine/install/

Afterwards, from the `baseFolder`, build the Docker image:

```
docker build -f potionDB/Dockerfile . -t mypotiondb
```

This will build a docker image named `mypotiondb` from the source code.
Afterwards, follow the steps (skip the step with `docker pull`) in [Getting Started](#Getting-Started) to run your own image.
Simply replace `andrerj/potiondb` with `mypotiondb`.

## Building and running from source without docker

**Note**: this involves a fairly lengthy amount of steps.
Please consider using the pre-built docker image or building your own image if you desire to experiment with changes to the code.

Make sure you have Go installed with the following command:

```
go version
```

If not, check https://go.dev/doc/install on how to install Go.

After Go is installed, install RabbitMQ or use its docker image (recommended): https://www.rabbitmq.com/download.html

Afterwards, configure RabbitMQ to create the vhost */crdts* and configure the user *guest* to have access to it.
Alternatively, alter the PotionDB configuration file that you desire to use and add the following line:

```
rabbitVHost = /
```

Whenever you decide to go with RabbitMQ's docker image or install RabbitMQ, make sure to respectively run a container or the RabbitMQ server. The link above contains instructions for both.

After RabbitMQ is started, go inside the folder `potionDB/potionDB/` and run:

```
go run ./main/protoServer.go --config=../configs/cluster/default
```

You can use one of the other provided configuration files or use your own.
For a list of PotionDB's supported arguments:

```
go run ./main/protoServer.go --help
```

## Running multiple instances of PotionDB

Some starting notes:

- This tutorial assumes that Docker will be used, but you can adapt the example to not use docker. Check [Building and running from source without docker](#Building-and-running-from-source-without-docker) on how to run a single PotionDB without Docker.
- At the end of this tutorial, you will have PotionDB running and ready for the TPC-H benchmark.
You can find PotionDB's TPC-H client [here](https://github.com/AndreRijo/TPCH-Client).
- Check [Configuring partial replication](#Configuring-partial-replication) on how to configure PotionDB's partial replication for a different setup.

#### In the same device

First, obtain or build the image:

```
docker pull andrerj/potiondb
OR
docker build -f potionDB/Dockerfile . -t mypotiondb
```

Create a docker network to easily connect the 5 instances of PotionDB:

```
docker network create potiondb-net
```

Now, start the servers as follows:

```
docker run -d -p 8087:8087 -p 5672:5672 --network potiondb-net --cap-add=NET_ADMIN -e CONFIG=/go/bin/configs/singlePC/docker/R1 --name potiondb1 andrerj/potiondb
docker run -d -p 8088:8087 -p 5673:5672 --network potiondb-net --cap-add=NET_ADMIN -e CONFIG=/go/bin/configs/singlePC/docker/R2 --name potiondb2 andrerj/potiondb
docker run -d -p 8089:8087 -p 5674:5672 --network potiondb-net --cap-add=NET_ADMIN -e CONFIG=/go/bin/configs/singlePC/docker/R3 --name potiondb3 andrerj/potiondb
docker run -d -p 8090:8087 -p 5675:5672 --network potiondb-net --cap-add=NET_ADMIN -e CONFIG=/go/bin/configs/singlePC/docker/R4 --name potiondb4 andrerj/potiondb
docker run -d -p 8091:8087 -p 5676:5672 --network potiondb-net --cap-add=NET_ADMIN -e CONFIG=/go/bin/configs/singlePC/docker/R5 --name potiondb5 andrerj/potiondb
```

This will start 5 instances of PotionDB, ready to serve clients.

To have the servers load the TPC-H dataset and views by themselves, add the following to the commands above (SCALE stands for SF):

```
-v "/FULL_PATH_TO_TPCH_DATA/tpch_data/:/go/data/"
-e DO_DATALOAD=true -e DO_INDEXLOAD=true -e SCALE=0.1
```

**Note**: Eurosys reviewers must include the above when replicating the experiments in the paper.

This will load the TPC-H dataset and prepare the necessary views.
PotionDB will then be immediately ready to be queried, updated or benchmarked by the [PotionDB's TPC-H Client](https://github.com/AndreRijo/TPCH-Client).

You can modify the configuration files to run a different number of servers.
If doing so, check the next subsection on how to use the modified configuration files.

You can check the log of each PotionDB with the following command:

```
docker logs potiondb1
```

Alternatively, you can do `docker run` without `-d` in order to see the output directly.
If so, however, one terminal instance will be needed per PotionDB.

#### Across multiple devices

First and foremost, open the configuration files in [configs/cluster/normal](configs/cluster/normal).
In each configuration file, modify the line "remoteRabbitMQAddresses =" to include the IP addresses of each device except the self, appended with ":5672".

For example, if running three servers with addresses 192.168.10.1, 192.168.10.2, 192.168.10.3:
```
R1: remoteRabbitMQAddresses = 192.168.10.2:5672 192.168.10.3:5672
R2: remoteRabbitMQAddresses = 192.168.10.1:5672 192.168.10.3:5672
R3: remoteRabbitMQAddresses = 192.168.10.1:5672 192.168.10.2:5672
```

The order of the servers is not relevant.
You will also need to modify the line regarding the server's own IP (localPotionDBAddress), and append the IP with ":8087". For example, for server 192.168.10.1:
```
localPotionDBAddress = 192.168.10.1:8087
```

After modifying the configurations and making said configurations reachable in the devices to be used, proceed as follows.

First, obtain or build the image in each device:

```
docker pull andrerj/potiondb
OR
docker build -f potionDB/Dockerfile . -t mypotiondb
```

Then run the following commands, each on its respective device:

```
docker run -p 8087:8087 -p 5672:5672 --cap-add=NET_ADMIN -v "/FULL_PATH_TO_FOLDER_WITH_CONFIGS/cluster/normal/R1:/go/bin/extern/" -e CONFIG=/go/bin/extern/ -v "/FULL_PATH_TO_TPCH_DATA/tpch_data/:/go/data/" -e DO_DATALOAD=true -e DO_INDEXLOAD=true -e SCALE=1 --name potiondb1 andrerj/potiondb
docker run -p 8087:8087 -p 5672:5672 --cap-add=NET_ADMIN -v "/FULL_PATH_TO_FOLDER_WITH_CONFIGS/cluster/normal/R2:/go/bin/extern/" -e CONFIG=/go/bin/extern/ -v "/FULL_PATH_TO_TPCH_DATA/tpch_data/:/go/data/" -e DO_DATALOAD=true -e DO_INDEXLOAD=true -e SCALE=1 --name potiondb2 andrerj/potiondb
docker run -p 8087:8087 -p 5672:5672 --cap-add=NET_ADMIN -v "/FULL_PATH_TO_FOLDER_WITH_CONFIGS/cluster/normal/R3:/go/bin/extern/" -e CONFIG=/go/bin/extern/ -v "/FULL_PATH_TO_TPCH_DATA/tpch_data/:/go/data/" -e DO_DATALOAD=true -e DO_INDEXLOAD=true -e SCALE=1 --name potiondb3 andrerj/potiondb
docker run -p 8087:8087 -p 5672:5672 --cap-add=NET_ADMIN -v "/FULL_PATH_TO_FOLDER_WITH_CONFIGS/cluster/normal/R4:/go/bin/extern/" -e CONFIG=/go/bin/extern/ -v "/FULL_PATH_TO_TPCH_DATA/tpch_data/:/go/data/" -e DO_DATALOAD=true -e DO_INDEXLOAD=true -e SCALE=1 --name potiondb4 andrerj/potiondb
docker run -p 8087:8087 -p 5672:5672 --cap-add=NET_ADMIN -v "/FULL_PATH_TO_FOLDER_WITH_CONFIGS/cluster/normal/R5:/go/bin/extern/" -e CONFIG=/go/bin/extern/ -v "/FULL_PATH_TO_TPCH_DATA/tpch_data/:/go/data/" -e DO_DATALOAD=true -e DO_INDEXLOAD=true -e SCALE=1 --name potiondb5 andrerj/potiondb
```

The setup above starts 5 instances of PotionDB, ready for TPC-H benchmarking with [PotionDB's TPC-H Client](https://github.com/AndreRijo/TPCH-Client).



## Configuring partial replication

PotionDB's partial replication is configured by associating objects to buckets, and then for each server defining the subset of buckets it replicates.

Objects are uniquely identified by the triple (*id*, *type*, *bucket*).
Thus, associating an object to a bucket consists of, on the object's creation, to set *bucket* to the desired value.

To set the list of buckets for a given server to replicate, simply modify one of the configuration files and, on the line starting with `buckets = `, put the list of buckets desired, each separated by a space.

For example:

```
buckets = myBucket1, myBucket2
buckets = *
```

The first one replicates two buckets, *myBucket1* and *myBucket2*.
The latter replicates all buckets.

Make sure when starting PotionDB to pass as a parameter the modified configuration file, as explained previously [here](#Customizing-PotionDB).

## Simulating latency between servers

Through the usage of [tc](), it is possible to impose artificial latency in communications between PotionDB servers.
If you desire to use `tc`, **please make sure to run PotionDB with Docker**, otherwise this will mess up with your computer(s)' network configurations.

To be able to simulate latency, open the desired PotionDB configuration file and scroll until its end.
You will find something like this:

```
useTC = false
tcIPs = 172.19.0.2 172.19.0.3 172.19.0.4 172.19.0.5 172.19.0.6
tcMyPos = 0
tcLatency = 0 41.43 42.98 108.38 61.92
```

- `useTC (true/false)`: Whenever to have added latency or not;
- `tcIPs`: List of IPs of each PotionDB. Make sure all IPs appear by the same order in all server's configuration files;
- `tcMyPos`: the position of the current server in the list above, from 0 to #servers - 1;
- `tcLatency`: one-way latency from the current server to each of the other servers, following the same order as in `tcIPs`.

PotionDB will set up `tc` automatically when `useTC` is true.cd ..
