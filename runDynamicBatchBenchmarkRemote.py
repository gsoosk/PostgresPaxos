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
        "postgres": "9000",
        "partition": "1"
    },
    {
        "address":"129.114.109.112",
        "port": "8002",
        "postgres": "9000",
        "partition": "2"
    },
    {
        "address":"129.114.108.105",
        "port": "8003",
        "postgres": "9000",
        "partition": "3"
    }
]
client_address = "129.114.108.225"



server_jar = "PaxosKV-server.jar"
performance_jar = "PaxosKV-performance.jar"

# [(batch_size, time)]
dynamic_batch_sizes = [(300000, 40), (350000, 40), (400000, 40), (450000, 40), (490000, 40), (400000, 40), (350000, 40), (300000, 40), (100, 40)]
number_of_clients = 3
benchmark_time = 400
result_folder = "./result"
interval = 300
max_retry = 2

dummy_interval = 300
dummy_batch_size = 1000


def runServers():
    s = []
    for server in servers:
        print(f'starting server {server["port"]}')
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
    print("preparing folders in remote...")
    subprocess.Popen(["ssh", f'cc@{client_address}', "sudo", "rm", "-rf", "~/paxos/result",]).wait()
    subprocess.Popen(["ssh", f'cc@{client_address}', "sudo", "mkdir", "~/paxos/result",]).wait()

    print(f"interval is {interval}")
    dynamic_inputs = []
    for batch_size in dynamic_batch_sizes:
        dynamic_inputs += ["--dynamic-batch-size", str(batch_size[0])]
        dynamic_inputs += ["--dynamic-batch-time", str(batch_size[1])]


    print(f"cleaning up previous run")
    runClients(dummy_interval, dummy_batch_size, dummy=True)
    time.sleep(10)
    print(f"starting {number_of_clients} benchmark clients for batch size {batch_size}, interval: {interval}")
    runClients(interval, dynamic_inputs)
    
    subprocess.Popen(["scp", "-r", f'cc@{client_address}:~/paxos/result', "."]).wait

def runClients(interval, batch_size, dummy=False):
    clients = []
    for i in range(number_of_clients):
        result_file = f'~/paxos/result/result_partition{i + 1}.csv' if not dummy else f'/tmp/result_dummy{i + 1}.csv'
        metric_file = f'~/paxos/result/metrics_partition{i + 1}.csv' if not dummy else f'/tmp/result_dummy{i + 1}.csv' 
        time = str(benchmark_time) if not dummy else str(10)
        process = [
                        "ssh", f'cc@{client_address}', "cd", "paxos", "&&", "sudo",
                        "java", "-jar", performance_jar,
                        "--address", servers[i]['address'],
                        "--port", servers[i]['port'],
                        "--benchmark-time", time,
                        "--throughput", "-1",
                        "--record-size", "255",
                        "--partition-id", str(i + 1),
                        "--interval", str(interval),
                        "--timeout", str(600), 
                        "--result-file", result_file,
                        "--metric-file", f'~/paxos/result/metrics_partition{i + 1}.csv',
                        "--max-retry", str(max_retry)
                    ]
        if dummy:
            process += ["--batch-size", str(batch_size)]
        else: 
            process += batch_size

        client = subprocess.Popen(process)
        clients.append(client)
    for p in clients:
        p.wait()


def killAllOpenProcesses():
    print("killing opened processes")
    subprocess.Popen(["ssh", f'cc@{client_address}', 'sudo', 'pkill', '--signal', 'SIGKILL',  'java']).wait()
    for server in servers:
        subprocess.Popen(["ssh", f'cc@{server["address"]}', 'sudo', 'pkill', '--signal', 'SIGKILL',  'java']).wait()

def exitHandler(signum, frame):
    killAllOpenProcesses()
    exit(1)
 
 
signal.signal(signal.SIGINT, exitHandler)
killAllOpenProcesses()
all_servers_process = runServers()
time.sleep(5)
runBenchmark()
killAllOpenProcesses()