# Paxos Postgres Ansible Playbooks

This repository provides two playbooks: 
* `init.yml`: installs requirements on the inventory servers (Java, Postgres, Docker, ...)
* `deploy.yml`: deploys `server` and `benchmark` programs on the remote servers 
    * Runs postgres on each of the servers
    * Builds and Deploys programs to the remote servers

## How to run a playbook using ansible
```bash
ansible-playbook -v -i <inventory_file> <playbook_file>
```
