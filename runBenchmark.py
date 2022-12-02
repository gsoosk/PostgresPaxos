from http import client
import subprocess
import sys
import time
import os
import shutil

servers = [
    {
        "port": "8001",
        "postgres": "5430",
        "partition": "1"
    },
    {
        "port": "8002",
        "postgres": "5431",
        "partition": "2"
    },
    {
        "port": "8003",
        "postgres": "5432",
        "partition": "3"
    }
]

server_jar = "./target/PaxosKV-server.jar"
performance_jar = "./target/PaxosKV-performance.jar"

batch_siize_start = 4000
batch_size_end = 300000
batch_size_step = 10000
number_of_clients = 3
benchmark_time = 15
result_folder = "./result"
intervals = [300]


def runServers():
    s = []
    for server in servers:
        print(f'starting server {server["port"]}')
        s.append(
            subprocess.Popen(["java", "-jar", server_jar, server["port"], server["postgres"], server["partition"]]))
        time.sleep(1)
    return s


def runBenchmark():
    if os.path.exists(result_folder):
        shutil.rmtree(result_folder)
    os.mkdir(result_folder)

    for interval in intervals:
        for batch_size in range(batch_siize_start, batch_size_end, batch_size_step):
            print(f"starting {number_of_clients} benchmark clients for batch size {batch_size}, interval: {interval}")
            clients = []
            for i in range(number_of_clients):
                client = subprocess.Popen(
                    [
                        "java", "-jar", performance_jar,
                        "--address", "127.0.0.1",
                        "--port", servers[i]['port'],
                        "--benchmark-time", str(benchmark_time),
                        "--throughput", "-1",
                        "--record-size", "255",
                        "--partition-id", str(i + 1),
                        "--batch-size", str(batch_size),
                        "--interval", str(interval),
                        "--timeout", str(2000),
                        "--result-file", f'./result/result_partition{i + 1}.csv'
                    ]
                )
                clients.append(client)
            for p in clients:
                p.wait()
            time.sleep(3)


all_servers_process = runServers()
time.sleep(2)
runBenchmark()
for p in all_servers_process:
    p.kill()

# while(True):
#     time.sleep(2)
