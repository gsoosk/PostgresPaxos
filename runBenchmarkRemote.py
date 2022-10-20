from http import client
from pydoc import cli
import subprocess
import sys
import time
import os
import shutil
import signal
import pexpect
import paramiko

servers = [
    {
        "address":"129.114.109.1",
        "port": "8001",
        "postgres": "5432",
        "partition": "1"
    },
    {
        "address":"129.114.109.112",
        "port": "8002",
        "postgres": "5432",
        "partition": "2"
    },
    {
        "address":"129.114.108.105",
        "port": "8003",
        "postgres": "5432",
        "partition": "3"
    }
]
client_address = "129.114.108.225"



server_jar = "PaxosKV-server.jar"
performance_jar = "PaxosKV-performance.jar"

batch_siize_start = 4000
batch_size_end = 600000
batch_size_step = 4000
number_of_clients = 3
benchmark_time = 20
result_folder = "./result"
intervals = [300, 400, 500]


def runServers():
    s = []
    for server in servers:
        print(f'starting server {server["port"]}')
        k = subprocess.Popen(["ssh", f'cc@{server["address"]}', 'pkill', 'java'])
        k.wait()
        s.append(
            subprocess.Popen(["ssh", f'cc@{server["address"]}', "cd", "paxos", "&&" 
            "java", "-jar", server_jar, server["port"], server["postgres"], server["partition"], server["address"]])
        )
        time.sleep(1)
    return s




def runBenchmark():
    if os.path.exists(result_folder):
        shutil.rmtree(result_folder)
    os.mkdir(result_folder)
    subprocess.Popen(["ssh", f'cc@{client_address}', "sudo", "rm", "-rf", "~/paxos/result",]).wait()
    subprocess.Popen(["ssh", f'cc@{client_address}', "sudo", "mkdir", "~/paxos/result",]).wait()

    for interval in intervals:
        for batch_size in range(batch_siize_start, batch_size_end, batch_size_step):
            all_servers_process = runServers()
            time.sleep(1) 

            print(f"starting {number_of_clients} benchmark clients for batch size {batch_size}, interval: {interval}")
            clients = []
            for i in range(number_of_clients):
                client = subprocess.Popen(
                    [
                        "ssh", f'cc@{client_address}', "cd", "paxos", "&&", "sudo",
                        "java", "-jar", performance_jar,
                        "--address", servers[i]['address'],
                        "--port", servers[i]['port'],
                        "--benchmark-time", str(benchmark_time),
                        "--throughput", "-1",
                        "--record-size", "255",
                        "--partition-id", str(i + 1),
                        "--batch-size", str(batch_size),
                        "--interval", str(interval),
                        "--timeout", str(700), 
                        "--result-file", f'~/paxos/result/result_partition{i + 1}.csv'
                    ]
                )
                clients.append(client)
            for p in clients:
                p.wait()
            
            for p in all_servers_process:
                p.kill()
            # time.sleep(3)
    
    subprocess.Popen(["scp", "-r", f'cc@{client_address}:~/paxos/result', "."]).wait


# all_servers_process = runServers()
time.sleep(2)
runBenchmark()
# for p in all_servers_process:
#     p.kill()