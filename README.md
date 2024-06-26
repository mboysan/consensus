![build](https://github.com/mboysan/consensus/actions/workflows/build.yml/badge.svg)

[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=mboysan_consensus&metric=coverage)](https://sonarcloud.io/summary/new_code?id=mboysan_consensus)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=mboysan_consensus&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=mboysan_consensus)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=mboysan_consensus&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=mboysan_consensus)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=mboysan_consensus&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=mboysan_consensus)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=mboysan_consensus&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=mboysan_consensus)

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
be started from within your code. Relevant information for each option can be found in the following sections.

# Quick Start

## Running the Distributed Key-Value Store Cluster

This section describes how to run a 3 node cluster of a distributed key-value store as a standalone java application
locally using the compiled jar executable.

Following are the dependencies needed for this project to run locally:
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
By running the commands above, we have given an id for each of our nodes with parameter `node.id` and we are using raft
as our consensus protocol (`protocol=raft`). The `destinations` parameter defines for each node where the other nodes 
are located (see [TcpTransportConfig](./consensus-network/src/main/java/com/mboysan/consensus/configuration/TcpTransportConfig.java) for more info on the format). Also notice that each node uses 2 different
ports. The first port is used for nodes to communicate to each other. And the second one is used to serve client
requests.

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
set key=<key> value=<value>

# alternative:
set k=<key> v=<value>
```
- To get the value of the key set:
```
get key=<key>

# alternative:
get k=<key>
```
- To delete the key:
```
delete key=<key>

# alternative:
delete k=<key>
```
- To get all defined keys:
```
iterateKeys
```

You can now start killing nodes one at a time by exiting terminal windows. You should be able to see the consensus
algorithm in action, i.e. as long as the majority of all nodes remain active in the cluster, the nodes are able to
tolerate failures by selecting a new leader and forming consensus on the latest state of the whole cluster,
acting as a single unit.

# Sending Custom Commands

To send custom commands from the client's command line interface (CLI), use the following format:
```
command=<command> arguments=<arguments> routeTo=<(optional) node-id to route the command to from the node that your client connected>

# short forms:
cmd=<command> args=<arguments> to=<(optional) id of the node to route the request to>
cmd=<command> arg=<arguments> to=<(optional) id of the node to route the request to>
<command> args=<arguments> to=<(optional) id of the node to route the request to>
```

### `checkIntegrity`:
`checkIntegrity` Checks the Integrity of all the nodes or just prints the state of a single node.
The `level` parameter defines the level of node state information and can be 1, 2, 10 or 20
where 1 is the informative state, 2 is the verbose state, 10 is to run an integrity check on all nodes with
informative state information and 20 to run an integrity check on all nodes with verbose state information.
The `routeTo` parameter is optional and is used to route the command to a specific node in the cluster.

```
# examples:

# to get the informative state of a node:
command=checkIntegrity level=1

# to get the verbose state of a node:
command=checkIntegrity level=2

# to run an integrity check on all nodes with results combined on the initiating node
command=checkIntegrity level=10
# same with verbose state:
command=checkIntegrity level=20

# to run an integrity check on all nodes initiated by node-2 (i.e. request routed to node-2):
command=checkIntegrity level=10 routeTo=2

# short form:
cmd=checkIntegrity level=10 to=2
checkIntegrity level=10 to=2
```

# More Info

## Module Descriptions

Following are the descriptions of each module in this repository (TODO):
- consensus-raft:
- consensus-bizur:
- consensus-core:
- consensus-...:

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

## About Failing Tests

At the time of writing this README document, we noticed that some tests are failing from time to time. Following are the
most commonly failing tests and the reasons for their failures:
- [RaftNodeTest.testReElection()](./consensus-raft/src/test/java/com/mboysan/consensus/RaftNodeTest.java):
The main reason for this test failing is due to the scheduled update task being triggered on two nodes around the same
time, causing deadlocks on the nodes.

These are known issues and will be fixed in the future. In the meantime, if you observe any test failing, you can
skip the tests by adding `-DskipTests` to the maven command as described in the above sections.
