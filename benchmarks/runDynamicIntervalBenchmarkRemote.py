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
        "address":"129.114.109.72",
        "ip":"10.52.3.90",
        "port": "8001",
        "postgres": "9000",
        "partition": "3"
    },
    {
        "address":"129.114.109.195",
        "ip":"10.52.2.181",
        "port": "8002",
        "postgres": "9000",
        "partition": "2"
    },
    {
        "address":"129.114.108.48",
        "ip":"10.52.3.72",
        "port": "8003",
        "postgres": "9000",
        "partition": "1"
    }

]
client_address = "129.114.109.41"



server_jar = "PaxosKV-server.jar"
performance_jar = "PaxosKV-performance.jar"

# interval = 300 and timeout 600
# [(batch_size, time)]
# dynamic_batch_sizes = [(200000, 180), (220000, 180), (250000, 180), (300000, 180), (250000, 180), (220000, 180), (200000, 180), (150000, 180), (120000, 180), (100000, 180), (70000, 180), (50000, 180), (10000, 180)]


# dynamic_batch_sizes = []
# for i in range(150000, 280000, 1200):
#     dynamic_batch_sizes.append((i, 10))

# for i in range(280000, 100000, -1000):
#     dynamic_batch_sizes.append((i, 10))

# for i in range(100000, 2000, -25000):
#     dynamic_batch_sizes.append((i, 180))


# interval = 500 and timeout 700
dynamic_intervals = [(125, 180), (71

                                  , 60), (125, 180)]

print(f"dynamic interval is {dynamic_intervals}")
benchmark_time = 0
for interval in dynamic_intervals:
    benchmark_time += interval[1]
print(f"benchmark time is {benchmark_time}")

number_of_clients = 1

result_folder = "./result"
batch_size = 100000
timeout = 1000
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

    print(f"batch is {batch_size}")
    dynamic_inputs = []
    for interval in dynamic_intervals:
        dynamic_inputs += ["--dynamic-interval", str(interval[0])]
        dynamic_inputs += ["--dynamic-interval-time", str(interval[1])]


    print(f"cleaning up previous run")
    runClients(dummy_batch_size, dummy_interval, dummy=True)
    time.sleep(10)
    print(f"starting {number_of_clients} benchmark clients for batch size {batch_size}, interval: {interval}")
    runClients(batch_size, dynamic_inputs)
    
    subprocess.Popen(["scp", "-r", f'cc@{client_address}:~/paxos/result', "."]).wait

def runClients(batch_size, interval, dummy=False):
    clients = []
    for i in range(number_of_clients):
        result_file = f'~/paxos/result/result_partition{i + 1}.csv' if not dummy else f'/tmp/result_dummy{i + 1}.csv'
        metric_file = f'~/paxos/result/metrics_partition{i + 1}.csv' if not dummy else f'/tmp/result_dummy{i + 1}.csv' 
        time = str(benchmark_time) if not dummy else str(10)
        process = [
                        "ssh", f'cc@{client_address}', "cd", "paxos", "&&", "sudo",
                        "java", "-jar", performance_jar,
                        "--address", servers[i]['ip'],
                        "--port", servers[i]['port'],
                        "--benchmark-time", time,
                        "--throughput", "-1",
                        "--record-size", "255",
                        "--partition-id", str(i + 1),
                        "--batch-size", str(batch_size),
                        "--timeout", str(timeout), 
                        "--result-file", result_file,
                        "--metric-file", f'~/paxos/result/metrics_partition{i + 1}.csv',
                        "--exponential-load", "true",
                        "--max-retry", str(max_retry)

                    ]
        if dummy:
            process += ["--interval", str(interval)]
        else: 
            process += interval

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