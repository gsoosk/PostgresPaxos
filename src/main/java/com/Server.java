package com;

import java.net.*;
import java.rmi.AccessException;
import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.io.*; 
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Server extends UnicastRemoteObject implements DatastoreInterface
{ 

	private static final long serialVersionUID = 1L;

	private String serverID;

	// Stores all the data sent from the client
	private Storage storage;

	// com.Server log is stored in logs folder
	public Logger logger;

	private Registry registry;

	private int port;

	private long previousProposalNumber;

	private Transaction previousAcceptedValue;

	private long lastLearnedProposalNumber;

	// This value is configurable for random acceptor failures
	private long randomAcceptorFailureNumber = 81l;

	private int maxPaxosRetrys = 3;

	protected Server(String serverID, Registry registry, int port, String postgresPort) throws RemoteException {
		super();
		this.serverID = serverID;
		this.registry = registry;
		this.logger = getLogger("logs/"+serverID+"_server.log");
		this.port = port;
		//TODO: change the port!
		this.storage = new Storage(postgresPort, logger);
	}

	public void registerNewServer(String currentServerID, DatastoreInterface server) throws RemoteException{
		this.registry.rebind(currentServerID, server);
		this.logger.info("Registered new server: "+currentServerID);
	}

	public String getServerID() throws RemoteException{
		return serverID;
	}

	public void setServerID(String serverID) {
		this.serverID = serverID;
	}

	public HashMap<String, String> getStorage() throws RemoteException {
		return storage.getAll();
	}

	public void setStorage(HashMap<String, String> storage) {
		this.storage.putAll(storage);
	}


	public synchronized Response get(String key) throws RemoteException {
		logger.info("Request Query [type=" + "get" + ", key=" + key + "]");

		Response response = new Response();
		response.setType("get");

		if(!storage.containsKey(key)){
			response.setReturnValue(null);
			response.setMessage("key "+key+" does not exist in the storage");
		}
		else {
			String val = storage.get(key);
			response.setReturnValue(val);
			response.setMessage("successfully retrieved entry from storage");
		}	

		logger.info(response.toString());
		return response;
	}


	public Response put(String key, String value) throws RemoteException {
		logger.info("Request Query [type=" + "put" + ", key=" + key + ", value=" + value + "]");
		Transaction transaction = new Transaction();
		transaction.setType("put");
		transaction.setKey(key);
		transaction.setValue(value);

		logger.info("Invoking Proposer");

		Response response = new Response();
		response.setType("put");
		response.setReturnValue(null);

		try {
			invokeProposer(transaction);
			response.setMessage("Successfully inserted the entry in the datastore");
		}
		catch(TimeoutException e) {
			response.setMessage("Request timed out");
		}

		logger.info(response.toString());	
		return response;
	}

	public Response delete(String key) throws RemoteException {
		logger.info("Request Query [type=" + "delete" + ", key=" + key + "]");
		Transaction transaction = new Transaction();
		transaction.setType("delete");
		transaction.setKey(key);
		transaction.setValue(null);

		logger.info("Invoking Proposer");
		Response response = new Response();
		response.setType("delete");
		response.setReturnValue(null);

		try{
			invokeProposer(transaction);
			response.setMessage("Successfully deleted the entry from the datastore");
		}
		catch(TimeoutException e) {
			response.setMessage("Request timed out");
		}

		logger.info(response.toString());
		return response;

	}

	public void invokeProposer(Transaction transaction) throws AccessException, RemoteException, TimeoutException {

		boolean isRoundFailed = true;
		int tryNumber = 1;

		while(isRoundFailed){
			if(tryNumber > this.maxPaxosRetrys) {
				throw new TimeoutException();
			}
			tryNumber++;

			logger.info("New Paxos round started");

			long proposalNumber = System.currentTimeMillis();
			logger.info("New proposal number is "+proposalNumber);

			List<Promise> promises = new ArrayList<Promise>();
			for(String serverID: this.registry.list()) {
				try {
					logger.info("Sending prepare to server: "+serverID);
					DatastoreInterface server = (DatastoreInterface) this.registry.lookup(serverID);
					Promise promise = server.prepare(proposalNumber);
					logger.info("Received promise");
					promise.setServerID(serverID);
					promises.add(promise);
				}
				catch(RemoteException re) {
					logger.info("Received denial");
					continue;
				} catch (NotBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

			if( promises.size() <= this.registry.list().length / 2) {
				try {
					logger.info("Majority of acceptors didn't promise, restarting paxos run in 2 seconds");
					TimeUnit.SECONDS.sleep(2);
				} catch (InterruptedException e) {
					logger.log(Level.SEVERE, "Interrupted Exception", e);
				}
				continue;
			}

			logger.info("Majority of acceptors promised");

			long max = 0l;
			Transaction value = transaction;

			for(Promise promise : promises) {
				if((promise.getPreviousProposalNumber() != 0) && (promise.getPreviousProposalNumber() > max)) {
					max = promise.getPreviousProposalNumber();
					value = promise.getPreviousAcceptedValue();
				}
			}

			logger.info("Value for accept: "+value.toString());
			List<Accepted> accepteds = new ArrayList<Accepted>();

			for(Promise promise : promises) {
				try {
					logger.info("Sending accept to server: "+promise.getServerID());
					String serverID = promise.getServerID();
					DatastoreInterface server = (DatastoreInterface) this.registry.lookup(serverID);
					Accepted acceptedMessage = server.accept(proposalNumber, value);
					acceptedMessage.setServerID(promise.getServerID());
					accepteds.add(acceptedMessage);
					logger.info("Received accept");
				}
				catch(RemoteException re) {
					logger.info("Received reject");
					continue;
				} catch (NotBoundException e) {
					logger.log(Level.SEVERE, "Not Bound Exception", e);
				}
			}

			if( accepteds.size() <= this.registry.list().length / 2) {
				try {
					logger.info("Majority of acceptors didn't accept, restarting paxos run in 2 seconds");
					TimeUnit.SECONDS.sleep(2);
				} catch (InterruptedException e) {
					logger.log(Level.SEVERE, "Interrupted Exception", e);
				}
				continue;
			}

			logger.info("Majority of acceptors accepted");

			logger.info("Invoking Learners");
			for(Accepted accepted: accepteds) {
				try {
					logger.info("Invoking learner: "+accepted.getServerID());
					DatastoreInterface server = (DatastoreInterface) this.registry.lookup(accepted.getServerID());
					server.invokeLearner(accepted);
					logger.info("Learner was able to successfully learn");
				}
				catch(RemoteException re) {
					logger.info("Learner failed");
					continue;
				} catch (NotBoundException e) {
					logger.log(Level.SEVERE, "Not Bound Exception", e);
				}
			}

			logger.info("Learning job finished");

			isRoundFailed = false;
		}
		logger.info("Paxos round ended");
	}



	public Promise prepare(long proposalNumber) throws RemoteException {
		// Acceptor is configured to fail at random times - If proposal number % randomAcceptorFailureNumber == 0
		if(proposalNumber % this.randomAcceptorFailureNumber == 0l) {
			logger.info("Acceptor failed at random time as per configuration");
			throw new RemoteException();
		}

		if(proposalNumber<=this.previousProposalNumber) {
			logger.info("Prepare request Declined as previous proposal number("+this.previousProposalNumber+") is greater than new proposal number("+proposalNumber+")");
			throw new RemoteException();
		}

		Promise promise = new Promise();
		promise.setProposalNumber(proposalNumber);
		promise.setPreviousProposalNumber(this.previousProposalNumber);
		promise.setPreviousAcceptedValue(previousAcceptedValue);

		logger.info("Promising for proposal number: "+proposalNumber);
		return promise;
	}



	public Accepted accept(long proposalNumber, Transaction value) throws RemoteException {
		// Acceptor is configured to fail at random times - If proposal number % randomAcceptorFailureNumber == 0
		if(proposalNumber % this.randomAcceptorFailureNumber == 0l) {
			logger.info("Acceptor failed at random time as per configuration");
			throw new RemoteException();
		}

		if(proposalNumber<this.previousProposalNumber) {
			logger.info("Accept request Declined as new proposal number("+proposalNumber+") is less than previous proposal numberr("+this.previousProposalNumber+")");
			throw new RemoteException();
		}

		logger.info("Accept request confirmed for transaction: "+value.toString());

		Accepted accepted = new Accepted();
		accepted.setProposalNumber(proposalNumber);
		accepted.setValue(value);

		return accepted;
	}

	public synchronized void invokeLearner(Accepted accepted) throws RemoteException{
		logger.info("Learner invoked");

		if(this.lastLearnedProposalNumber == accepted.getProposalNumber()) {
			logger.info("Aborting learning, value is already learned");
			throw new RemoteException();
		}

		if(accepted.getServerID() == this.serverID) {
			logger.info("Erasing previous proposal number and accepted value");
			this.previousProposalNumber = 0;
			this.previousAcceptedValue = null;
		}

		Transaction trasaction = accepted.getValue();

		if(trasaction.getType().equals("put")) {
			this.storage.put(trasaction.getKey(), trasaction.getValue());
		}
		else if(trasaction.getType().equals("delete")){
			this.storage.remove(trasaction.getKey());
		}
		this.lastLearnedProposalNumber = accepted.getProposalNumber();
		logger.info("Learned a new value: "+trasaction.toString());
	}


	//takes in the log-file path and builds a logger object
	public static Logger getLogger(String logFile) {
		Logger logger = Logger.getLogger("server_log");  
		FileHandler fh;  

		try {  
			File log = new File(logFile);
			// if file does not exist we create a new file
			if(!log.exists()) {
				log.createNewFile();
			}
			fh = new FileHandler(logFile,true);  
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();  
			fh.setFormatter(formatter);  

		} catch (SecurityException e) {  
			e.printStackTrace();  
		} catch (IOException e) {  
			e.printStackTrace();  
		} 

		return logger;
	}


	public static String createServerID(int port) {
		String id = null;
		try {
			InetAddress IP = InetAddress.getLocalHost();
			id = IP.getHostAddress()+"_"+String.valueOf(port);

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}

	public static void main(String args[]) 
	{ 
		try {
			int port = Integer.parseInt(args[0]);
			Registry registry = LocateRegistry.createRegistry(Integer.parseInt(args[0]));
			String currentServerID = createServerID(port);
			String postgresPort = args[1];
			Server server = new Server(currentServerID, registry, port, postgresPort);

			registry.rebind("com.Server", server);

			server.logger.info("com.Server started");

			InputStream input = new FileInputStream("resources/config.properties");
			Properties prop = new Properties();
			// load a properties file
			prop.load(input);
			// get discovery nodes to connect to cluster
			String[] discoveryNodes = prop.getProperty("discovery.nodes").split(",");

			boolean discoverySuccessful = false;
			server.logger.info("com.Server trying to connect to a cluster");

			for(String discoveryNode : discoveryNodes)
			{
				try {
					String[] data = discoveryNode.split(":");
					String discoveryNodeIPAddress = data[0];
					int discoveryNodePort = Integer.parseInt(data[1]);

					Registry discoveredRegistry = LocateRegistry.getRegistry(discoveryNodeIPAddress, discoveryNodePort);

					for(String serverID : discoveredRegistry.list()) {
						try {
							DatastoreInterface discoveredRegistryServer = (DatastoreInterface)discoveredRegistry.lookup(serverID);
							if(!currentServerID.equals(discoveredRegistryServer.getServerID())) {
								discoverySuccessful = true;
								server.setStorage(discoveredRegistryServer.getStorage());
								discoveredRegistryServer.registerNewServer(currentServerID, server);
								server.logger.info("Registered current server with server: "+discoveredRegistryServer.getServerID());
								registry.bind(discoveredRegistryServer.getServerID(), discoveredRegistryServer);
								server.logger.info("Registered server: "+discoveredRegistryServer.getServerID()+" with current server" );
							}
						}
						catch(ConnectException e) {
							continue;
						}
					}
					if(discoverySuccessful==true) break;
				}
				catch(Exception e){
					continue;
				}
			}

			if(!discoverySuccessful) {
				server.logger.info("Could not connect to any cluster, acting as a standalone cluster");
			}
			else {
				server.logger.info("Connected to a cluster");
			}

		}
		catch(ArrayIndexOutOfBoundsException e) {
			System.out.println("Please provide port as command line argument");
			System.exit(0);
		}
		catch(NumberFormatException e) {
			System.out.println("Please provide port as command line argument");
			System.exit(0);
		}
		catch(java.rmi.ConnectException e) {
			System.err.println("Could not connect to Master com.Server: " + e);
			System.exit(0);
		}
		catch (Exception e) {
			e.printStackTrace();
			System.err.println("com.Server exception: " + e);
			System.exit(0);
		}
	}
} 