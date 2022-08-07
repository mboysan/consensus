![build](https://github.com/mboysan/consensus/actions/workflows/build.yml/badge.svg)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=mboysan_consensus&metric=alert_status)](https://sonarcloud.io/dashboard?id=mboysan_consensus)

---------------- Draft ----------------

This readme is currently in draft and will be improved over time.

License: TBD (but MIT for now)

# About

This repository contains reference implementations of [Raft](https://raft.github.io/raft.pdf) and [Bizur](https://arxiv.org/pdf/1702.04242.pdf)
consensus algorithms in java with the main purpose of showing their application in distributed Key-Value stores
where consistency is favored over availability in the CAP theorem context.

This is the main repository for my [MSc. thesis](TODO://attach_relevant_papers) 
in which I explore these algorithms from the perspectives of performance, usability and ease of understanding in the 
mentioned context. Of course, the application of these algorithms are not limited to distributed key-value stores, and 
can be used in different distributed concepts such as distributed sets, distributed locks, atomic operations etc. where 
I believe this project would serve as an educational reference as it is highly pruned of production grade complexity.

The repository is designed to be used in two-fold. First, as a standalone application to start your own cluster of
distributed key-value store and finally to be used in java applications as a library where the key-value store can
be started from within your code. Following sections describe these options.

## Module Descriptions

Following are the descriptions of each module in this repository (TODO):
- consensus-raft:
- consensus-bizur:
- consensus-core:
- consensus-...:

# Quick Start

## Running the Distributed Key-Value Store Cluster

This section describes how to run a 3 node cluster of a distributed key-value store as a standalone java application
locally using the compiled jar executable.

Following are the dependencies needed for this section:
- Java Development Kit (JDK) version >= 17
- Maven version >= 3.8.5
- (Optional): A linux operating system (adapt the commands for any other OS if needed)

First, let's try to compile the project and produce the jar executable needed.
```
# compile (without running tests).
mvn clean package -DskipTests

# switch to the directory of the jar executable.
cd consensus-distribution/target

# rename the jar for easier referencing.
mv *-jar-with-dependencies.jar consensus.jar
```
At this point, we have switched to our working directory where we have an executable jar file called `consensus.jar` 
which will be used to run the nodes and client.

Let's start the nodes. Execute each command in different terminal windows.

```
# node-0
java -cp consensus.jar com.mboysan.consensus.KVStoreServerCLI \
    --node node.id=0 port=33330 protocol=raft destinations=0-localhost:33330,1-localhost:33331,2-localhost:33332 \
    --store port=33340

# node-1
java -cp consensus.jar com.mboysan.consensus.KVStoreServerCLI \
    --node node.id=1 port=33331 protocol=raft destinations=0-localhost:33330,1-localhost:33331,2-localhost:33332 \
    --store port=33341

# node-2
java -cp consensus.jar com.mboysan.consensus.KVStoreServerCLI \
    --node node.id=2 port=33332 protocol=raft destinations=0-localhost:33330,1-localhost:33331,2-localhost:33332 \
    --store port=33342
```
At this point, we have given an id for each of our nodes with parameter `node.id` and we are using raft as our consensus
protocol (`protocol=raft`). The `destinations` parameter defines for each node where the other nodes are located
(see [TcpTransportConfig](./consensus-network/src/main/java/com/mboysan/consensus/configuration/TcpTransportConfig.java)
for more info on the format). Also notice that each node uses 2 different ports. The first port is used for nodes to 
communicate to each other. And the second one is used to serve client requests.

Finally, we'll start our client for sending commands to the nodes.

```
# client
java -cp consensus.jar com.mboysan.consensus.KVStoreClientCLI destinations=0-localhost:33340,1-localhost:33341,2-localhost:33342
```
As you can see, we have given the node destinations for our client where each node accepts client requests (notice the
port numbers used).

Now you can start sending commands from the client terminal window by typing the following.
- To set a new key and value type the following and hit enter:
```
set <key> <value>
```
- To get the value of the key set:
```
get <key>
```
- To delete the key:
```
delete <key>
```
- To get all defined keys:
```
iterateKeys
```

You can now start killing nodes by exiting terminal windows. You should be able to see the consensus algorithm
in action, i.e. as long as the majority of all nodes remain active in the cluster, the nodes are able to tolerate
failures by selecting a new leader and forming consensus on the latest state of the whole cluster, acting as a single 
unit.

## Using The Project as a Library

The project is deployed to a [separate repository](https://github.com/mboysan/mvn-repo) as a library which can be 
used as a maven dependency in your own projects. Just add the necessary `<repository>` portion in your pom.xml
as described in that page, and in dependencies add:
```
<dependency>
    <groupId>com.mboysan.consensus</groupId>
    <artifactId>consensus-distribution</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
Reload your maven project and start using the classes. You can refer to unit and integration tests for how to use
each class.

TODO: example usage.

## About Logging

Using the project through the Command Line Interface ([consensus-cli module](consensus-cli)) automatically provides all
the necessary logging implementation and configuration (log4j), which by default logs all the output (INFO level) 
to console. To change the configuration, first create your own `log4j.properties` file and run the java executable 
with the following command:

```
java -Dlog4j.configuration=file:log4j.properties -cp consensus.jar <main class and additional properties>
```

However, if you choose to use the project as a library, the choice of logging implementation (and its configuration) is 
fully customizable and your own responsibility given that your own project uses slf4j bindings.
