package com;

import java.net.*;
import java.rmi.AccessException;
import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
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

	private Map<String, Long> previousProposalNumber = new HashMap<>();

	private Map<String, Transaction> previousAcceptedValue = new HashMap<>();

	private long lastLearnedProposalNumber;

	// This value is configurable for random acceptor failures
	private long randomAcceptorFailureNumber = 81l;

	private int maxPaxosRetrys = 3;

	// for multi paxos
	private Map<String, Boolean> electedAsLeaderBefore = new HashMap<>();
	private Map<String, Long> multiPaxosProposalNumber = new HashMap<>();
	private Map<String, List<Promise>> previousPromises = new HashMap<>();

	protected Server(String serverID, Registry registry, int port, String postgresPort) throws RemoteException {
		super();
		this.serverID = serverID;
		this.registry = registry;
		this.logger = getLogger("logs/"+serverID+"_server.log", true, false);
		this.port = port;
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

	public HashMap<String, String> getStorage(String partitionId) throws RemoteException {
		this.storage.setPartitionId(partitionId);
		return storage.getAll();
	}

	public void setStorage(HashMap<String, String> storage, String partitionId) {
	    // TODO : fix this if multiple partition initialization needed
		this.storage.setPartitionId(partitionId);
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


	@Override
	public Response clear(String partitionId) throws RemoteException {
		logger.info("Request Query [type=" + "clear"+ "]");
		Transaction transaction = new Transaction();
		transaction.setType("clear");
		transaction.setPartitionID(partitionId);


		logger.info("Invoking Proposer");
		Response response = new Response();
		response.setType("clear");
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

	public Response put(String key, String value, String partitionId) throws RemoteException {
		logger.info("Request Query [type=" + "put" + ", key=" + key + ", value=" + value + "]");
		Transaction transaction = new Transaction();
		transaction.setType("put");
		transaction.setKey(key);
		transaction.setValue(value);
		transaction.setPartitionID(partitionId);

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

	public Response batch(Map<String, String> values, String partitionId) throws RemoteException {
		logger.info("Request Query [type=" + "batch" + "]");
		Transaction transaction = new Transaction();
		transaction.setType("batch");
		transaction.setValues(values);
		transaction.setPartitionID(partitionId);

		logger.info("Invoking Proposer");

		Response response = new Response();
		response.setType("batch");
		response.setReturnValue(null);

		try {
			invokeProposer(transaction);
			response.setMessage("Successfully inserted batch entries in the datastore");
		}
		catch(TimeoutException e) {
			response.setMessage("Request timed out");
		}

		logger.info(response.toString());
		return response;
	}

	public Response delete(String key, String partitionId) throws RemoteException {
		logger.info("Request Query [type=" + "delete" + ", key=" + key + "]");
		Transaction transaction = new Transaction();
		transaction.setType("delete");
		transaction.setKey(key);
		transaction.setValue(null);
		transaction.setPartitionID(partitionId);


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

		String partitionId = transaction.getPartitionID();

		while(isRoundFailed){
			if(tryNumber > this.maxPaxosRetrys) {
				throw new TimeoutException();
			}
			tryNumber++;

			logger.info("New Paxos round started");
			if (!electedAsLeaderBefore.containsKey(partitionId))
				electedAsLeaderBefore.put(partitionId, false);
			long proposalNumber = electedAsLeaderBefore.get(partitionId) ? multiPaxosProposalNumber.get(partitionId) : System.currentTimeMillis();
			List<Promise> promises = electedAsLeaderBefore.get(partitionId) ? previousPromises.get(partitionId) : new ArrayList<Promise>();
			if (!electedAsLeaderBefore.get(partitionId)) {

				logger.info("New proposal number is " + proposalNumber);

				for (String serverID : this.registry.list()) {
					try {
						logger.info("Sending prepare to server: " + serverID);
						DatastoreInterface server = (DatastoreInterface) this.registry.lookup(serverID);
						Promise promise = server.prepare(proposalNumber, partitionId);
						logger.info("Received promise");
						promise.setServerID(serverID);
						promises.add(promise);
					} catch (RemoteException re) {
						logger.info("Received denial");
						continue;
					} catch (NotBoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

				if (promises.size() <= this.registry.list().length / 2) {
					try {
						logger.info("Majority of acceptors didn't promise, restarting paxos run in 2 seconds");
						TimeUnit.SECONDS.sleep(2);
					} catch (InterruptedException e) {
						logger.log(Level.SEVERE, "Interrupted Exception", e);
					}
					continue;
				}

				logger.info("Majority of acceptors promised");
			}

			long max = 0l;
			Transaction value = transaction;


//			for(Promise promise : promises) {
//				if((promise.getPreviousProposalNumber() != 0) && (promise.getPreviousProposalNumber() > max)) {
//					max = promise.getPreviousProposalNumber();
//					value = promise.getPreviousAcceptedValue();
//				}
//			}

			logger.info("Value for accept: "+value.toString());
			List<Accepted> accepteds = new ArrayList<Accepted>();

			for(Promise promise : promises) {
				try {
					logger.info("Sending accept to server: "+promise.getServerID());
					String serverID = promise.getServerID();
					DatastoreInterface server = (DatastoreInterface) this.registry.lookup(serverID);
					Accepted acceptedMessage = server.accept(proposalNumber, partitionId);
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
//				try {
					logger.info("Majority of acceptors didn't accept, restarting paxos");
					electedAsLeaderBefore.put(partitionId, false);
//					TimeUnit.SECONDS.sleep(2);
//				} catch (InterruptedException e) {
//					logger.log(Level.SEVERE, "Interrupted Exception", e);
//				}
				continue;
			}

			logger.info("Majority of acceptors accepted");

			logger.info("Invoking Learners");
			for(Accepted accepted: accepteds) {
				try {
					logger.info("Invoking learner: "+accepted.getServerID());
					DatastoreInterface server = (DatastoreInterface) this.registry.lookup(accepted.getServerID());
					server.invokeLearner(accepted, value);
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
			this.electedAsLeaderBefore.put(partitionId, true);
			this.multiPaxosProposalNumber.put(partitionId, proposalNumber);
			this.previousPromises.put(partitionId, promises);

			isRoundFailed = false;
		}
		logger.info("Paxos round ended");
	}



	public Promise prepare(long proposalNumber, String partitionId) throws RemoteException {
		// Acceptor is configured to fail at random times - If proposal number % randomAcceptorFailureNumber == 0
		if(proposalNumber % this.randomAcceptorFailureNumber == 0l) {
			logger.info("Acceptor failed at random time as per configuration");
			throw new RemoteException();
		}

		if(previousProposalNumber.containsKey(partitionId) && proposalNumber < this.previousProposalNumber.get(partitionId)) {
			logger.info("Prepare request Declined as previous proposal number("+this.previousProposalNumber+") is greater than new proposal number("+proposalNumber+")");
			throw new RemoteException();
		}

		Promise promise = new Promise();
		promise.setProposalNumber(proposalNumber);
		promise.setPreviousProposalNumber(previousProposalNumber.getOrDefault(partitionId, 0L));
		promise.setPreviousAcceptedValue(previousAcceptedValue.getOrDefault(partitionId, null));

		logger.info("Promising for proposal number: "+proposalNumber);
		return promise;
	}



	public Accepted accept(long proposalNumber, String partitionId) throws RemoteException {
		// Acceptor is configured to fail at random times - If proposal number % randomAcceptorFailureNumber == 0
		if(proposalNumber % this.randomAcceptorFailureNumber == 0l) {
			logger.info("Acceptor failed at random time as per configuration");
			throw new RemoteException();
		}

		if(previousProposalNumber.containsKey(partitionId) && proposalNumber < this.previousProposalNumber.get(partitionId)) {
			logger.info("Accept request Declined as new proposal number("+proposalNumber+") is less than previous proposal numberr("+this.previousProposalNumber+")");
			throw new RemoteException();
		}

		logger.info("Accept request confirmed for transaction ");

		this.previousProposalNumber.put(partitionId, proposalNumber);
		Accepted accepted = new Accepted();
		accepted.setProposalNumber(proposalNumber);

		return accepted;
	}

	public synchronized void invokeLearner(Accepted accepted, Transaction transaction) throws RemoteException{
		logger.info("Learner invoked");
		this.storage.setPartitionId(transaction.getPartitionID());

//		if(this.lastLearnedProposalNumber == accepted.getProposalNumber()) {
//			logger.info("Aborting learning, value is already learned");
//			throw new RemoteException();
//		}

		if(accepted.getServerID() == this.serverID) {
			logger.info("Erasing previous proposal number and accepted value");
//			this.previousProposalNumber = 0;
//			this.previousAcceptedValue = null;
		}

		if(transaction.getType().equals("put")) {
			logger.info("Learner putting data in db");
			this.storage.put(transaction.getKey(), transaction.getValue());
		}
		else if(transaction.getType().equals("delete")){
			logger.info("Learner deleting data in db");
			this.storage.remove(transaction.getKey());
		}
		else if (transaction.getType().equals("batch")) {
			logger.info("Learner putting data in db");
			this.storage.putAll(transaction.getValues());
		}
		else if (transaction.getType().equals("clear")) {
			this.storage.clear();
		}
		this.lastLearnedProposalNumber = accepted.getProposalNumber();
		logger.info("Learned a new value: "+transaction.toString());
	}


	//takes in the log-file path and builds a logger object
	public static Logger getLogger(String logFile, Boolean verbose, Boolean fileVerbose) {
		Logger logger = Logger.getLogger("server_log");
		logger.setLevel(Level.INFO);
		FileHandler fh;

		try {
			// This stops logs from getting displayed on console
			if (!verbose)
				logger.setUseParentHandlers(false);
			// if file does not exist we create a new file
			if (fileVerbose) {
				File log = new File(logFile);
				if (!log.exists()) {
					log.createNewFile();
				}
				fh = new FileHandler(logFile, true);
				logger.addHandler(fh);
				SimpleFormatter formatter = new SimpleFormatter();
				fh.setFormatter(formatter);
			}

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
			System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT : %1$tL] [%4$-7s] %5$s %n");
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
								// TODO: if storage initialization needed
//								server.setStorage(discoveredRegistryServer.getStorage(), );
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