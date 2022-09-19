package com;

import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

// RMI Interface
public interface DatastoreInterface extends Remote{
	public Response clear(String partitionId) throws RemoteException;
	public Response put(String key, String value, String partitionId) throws RemoteException;
	public Response batch(Map<String, String> values, String partitionId) throws RemoteException;
	public Response get(String key) throws RemoteException;
	public Response delete(String key, String partitionId) throws RemoteException;
	public HashMap<String, String> getStorage(String partitionId) throws RemoteException;
	public String getServerID() throws RemoteException;
	public Promise prepare(long proposalNumber, String partitionId) throws RemoteException;
	public Accepted accept(long proposalNumber, String partitionId) throws RemoteException;
	public void invokeLearner(Accepted accepted, Transaction transaction) throws RemoteException;
	public void registerNewServer(String currentServerID, DatastoreInterface server) throws RemoteException, AlreadyBoundException;
}

