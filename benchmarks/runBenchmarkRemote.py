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
        "address":"129.114.109.177",
        "ip":"10.52.3.170",
        "port": "8001",
        "postgres": "9000",
        "partition": "3"
    },
    {
        "address":"129.114.109.100",
        "ip":"10.52.0.31",
        "port": "8002",
        "postgres": "9000",
        "partition": "2"
    },
    {
        "address":"129.114.108.193",
        "ip":"10.52.0.245",
        "port": "8003",
        "postgres": "9000",
        "partition": "1"
    }

]
client_address = "129.114.109.169"



server_jar = "PaxosKV-server.jar"
performance_jar = "PaxosKV-performance.jar"

batch_siize_start =300000
batch_size_end = 600000
batch_size_step = 10000 
number_of_clients = 3
benchmark_time = 60
result_folder = "./result"
intervals = [300]
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

    for interval in intervals:
        print(f"interval is {interval}")
        for batch_size in range(batch_siize_start, batch_size_end, batch_size_step):
            print(f"cleaning up previous run")
            runClients(dummy_interval, dummy_batch_size, dummy=True)
            time.sleep(10)
            print(f"starting {number_of_clients} benchmark clients for batch size {batch_size}, interval: {interval}")
            runClients(interval, batch_size)
    
    subprocess.Popen(["scp", "-r", f'cc@{client_address}:~/paxos/result', "."]).wait

def runClients(interval, batch_size, dummy=False):
    clients = []
    for i in range(number_of_clients):
        result_file = f'~/paxos/result/result_partition{i + 1}.csv' if not dummy else f'/tmp/result_dummy{i + 1}.csv'
        time = str(benchmark_time) if not dummy else str(10)
        client = subprocess.Popen(
                    [
                        "ssh", f'cc@{client_address}', "cd", "paxos", "&&", "sudo",
                        "java", "-jar", performance_jar,
                        "--address", servers[i]['ip'],
                        "--port", servers[i]['port'],
                        "--benchmark-time", time,
                        "--throughput", "-1",
                        "--record-size", "255",
                        "--partition-id", str(i + 1),
                        "--batch-size", str(batch_size),
                        "--interval", str(interval),
                        "--timeout", str(700), 
                        "--result-file", result_file,
                        "--max-retry", str(max_retry) 
                    ]
                )
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
# for p in all_servers_process:
#     p.kill()