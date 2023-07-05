from pydoc import cli
import subprocess
import time
import os
import shutil
import signal
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

batch_size =250000
number_of_clients = 1
benchmark_time = 600
trigger_time = 30
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
    print(f"cleaning up previous run")
    clients = runClients(dummy_interval, dummy_batch_size, dummy=True)
    for p in clients:
        p.wait()
    time.sleep(10)

    print(f"starting {number_of_clients} benchmark clients for batch size {batch_size}, interval: {interval}")
    clients = runClients(interval, batch_size)

    print(f'waiting for {trigger_time} seconds to trigger into metastable state')
    time.sleep(trigger_time)

    

    for p in clients:
        p.wait()

    
    subprocess.Popen(["scp", "-r", f'cc@{client_address}:~/paxos/result', "."]).wait

def runClients(interval, batch_size, dummy=False):
    clients = []
    for i in range(number_of_clients):
        result_file = f'~/paxos/result/result_partition{i + 1}.csv' if not dummy else f'/tmp/result_dummy{i + 1}.csv'
        time = str(benchmark_time) if not dummy else str(10)
        client = runClient(interval, batch_size, i, result_file, time)
        clients.append(client)
    return clients

def runClient(interval, batch_size, i, result_file, time):
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
            
    return client


def killAllOpenProcesses():
    print("killing opened processes")
    subprocess.Popen(["ssh", f'cc@{client_address}', 'sudo', 'pkill', '--signal', 'SIGKILL',  'java']).wait()
    for server in servers:
        subprocess.Popen(["ssh", f'cc@{server["address"]}', 'sudo', 'pkill', '--signal', 'SIGKILL',  'java']).wait()

def killAClient():
    subprocess.Popen(["ssh", f'cc@{client_address}' ,"sudo  kill $(ps -ef | grep java | grep 8003 | awk '{print $2}')"])

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