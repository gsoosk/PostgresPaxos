# PostgresPaxos
In this repository, we implement a **replicated Postgres** instance using **multi-Paxos** protocol.

## Architecture
This implementation uses gRPC for communication, and clients will run gRPC procedures in order to interact with the servers.

![arch](./images/Architecture.jpg)


## Supported Input Procedures
```protobuf
rpc Put(Data) returns (Result) {}
```
Puts a `<key, value>` into the DB.
```protobuf
rpc Get(Key) returns (Result) {}
```
Gets the corresponding value for a specific key.
```protobuf
rpc Delete(Key) returns (Result) {}
```
Deletes a `<key,value>` from the DB.
```protobuf
rpc Clear(Partition) returns (Result) {}
```
Deletes all `<key,value>` from the DB.
```protobuf
rpc Batch(Values) returns (Result) {}
```
Put multiple `<key, value>`s to the DB in batch.

## How to run
### Ansible
We provide two ansible playbooks to install requirements and build/deploy applications on a distributed setup.
For more information, please refer to their [README](./ansible/README.md).

### Build
In order to run the following programs, their code should be built by maven: 
```
mvn clean install
```
### Server

**Step1.**  Configuration


Change the `./resources/config.properties` to the desired cluster. For example:
```java
discovery.nodes=localhost:8001,localhost:8002,localhost:8003
```
**Step2.** Run the servers
```
java -jar ./target/PaxosKV-server.jar <server_port> <postgres_port> <partition_number>
```
### Client
We implemented a simple client with `CLI` support. To run this client:

**Run the Client**
```
java -jar ./target/PaxosKV-client.jar <server_ip> <server_port> 
```

### Performance
This program benchmarks a cluster by putting a desired load on the system based on the provided configuration. 

#### How to use
```
java -jar ./target/PaxosKV-performance.jar --help                                                                                                       


usage: producer-performance [-h] --address ADDRESS --port PORT [--result-file RESULT-FILE] [--metric-file METRIC-FILE] --partition-id PARTITION-ID [--batch-size BATCH-SIZE] [--payload-delimiter PAYLOAD-DELIMITER] --throughput THROUGHPUT --interval INTERVAL
                            [--timeout TIMEOUT] [--max-retry MAXRETRY] [--dynamic-batch-size DYNAMICBATCHSIZE] [--dynamic-batch-time DYNAMICBATCHTIME] (--record-size RECORD-SIZE | --payload-file PAYLOAD-FILE) (--num-records NUM-RECORDS |
                            --benchmark-time BENCHMARK-TIME)

This tool is used to verify the producer performance.

named arguments:
  -h, --help             show this help message and exit
  --address ADDRESS      leader's address
  --port PORT            leader's port
  --result-file RESULT-FILE
                         a csv file containing the total result of benchmark
  --metric-file METRIC-FILE
                         a csv file containing the timeline result of benchmark
  --partition-id PARTITION-ID
                         Id of the partition that you want to put load on
  --batch-size BATCH-SIZE
                         batch size in bytes. This producer batches records in this size and send them to kv store
  --payload-delimiter PAYLOAD-DELIMITER
                         provides delimiter to be used when --payload-file is provided. Defaults to new line. Note that this parameter will be ignored if --payload-file is not provided. (default: \n)
  --throughput THROUGHPUT
                         throttle maximum message throughput to *approximately* THROUGHPUT messages/sec. Set this to -1 to disable throttling.
  --interval INTERVAL    interval between each packet.  Set this -1 to send packets blocking
  --timeout TIMEOUT      timeout of each batch request. It is two times of interval by default
  --max-retry MAXRETRY   Maximum number of times a request can be retried (default: -1)
  --dynamic-batch-size DYNAMICBATCHSIZE
                         dynamic batch size until a specific time
  --dynamic-batch-time DYNAMICBATCHTIME
                         deadline for a dynamic batch size

  either --record-size or --payload-file must be specified but not both.

  --record-size RECORD-SIZE
                         message size in bytes. Note that you must provide exactly one of --record-size or --payload-file.
  --payload-file PAYLOAD-FILE
                         file to read the message payloads from. This works only for UTF-8 encoded text files. Payloads will be read  from  this  file  and  a payload will be randomly selected when sending messages. Note that you must provide exactly one of --
                         record-size or --payload-file.

  either --num-records or --benchmark-time must be specified but not both.

  --num-records NUM-RECORDS
                         number of messages to produce
  --benchmark-time BENCHMARK-TIME
                         benchmark time in seconds
                                                                                                       
```
### Automated Benchmark
We provide some python scripts run `server` and `benchmark` programs automatically. For more information 
please refer to [benchmarks](./benchmarks)
