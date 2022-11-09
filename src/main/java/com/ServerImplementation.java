package com;

import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.net.*;
import java.rmi.AccessException;
import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.io.*; 
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ServerImplementation
{

	private static final long serialVersionUID = 1L;

	private String serverID;

	// Stores all the data sent from the client
	private Storage storage;

	// com.Server log is stored in logs folder
	public Logger logger;

	private int port;

	private Map<String, PaxosServerGrpc.PaxosServerBlockingStub> registery = new HashMap<>();

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

	protected ServerImplementation(String serverID, int port, String postgresPort, Logger logger) {
		super();
		this.serverID = serverID;
		this.logger = logger;
		this.port = port;
		this.storage = new Storage(postgresPort, logger);
	}

	public void registerNewServer(String serverID) {

		String[] data = serverID.split(":");
		String ip = data[0];
		int port = Integer.parseInt(data[1]);

		ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
		PaxosServerGrpc.PaxosServerBlockingStub stub = PaxosServerGrpc.newBlockingStub(channel);

		registery.put(serverID, stub);

		this.logger.info("Registered new server: "+serverID);
		this.logger.info("Registery is: " + registery);
	}

	public String getServerID() {
		return serverID;
	}

	public void setServerID(String serverID) {
		this.serverID = serverID;
	}
//
//	public HashMap<String, String> getStorage(String partitionId) throws RemoteException {
//		this.storage.setPartitionId(partitionId);
//		return storage.getAll();
//	}
//
//	public void setStorage(HashMap<String, String> storage, String partitionId) {
//	    // TODO : fix this if multiple partition initialization needed
//		this.storage.setPartitionId(partitionId);
//		this.storage.putAll(storage);
//	}
//
//
	public Response get(String key, String partition) {
		logger.info("Request Query [type=" + "get" + ", key=" + key + "]");

		Response response = new Response();
		response.setType("get");
		storage.setPartitionId(partition);
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


	public Response clear(String partitionId) {
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

	public Response put(String key, String value, String partitionId) throws StatusRuntimeException {
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

	public Response batch(Map<String, String> values, String partitionId) {
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

	public Response delete(String key, String partitionId)  {
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

	public void invokeProposer(Transaction transaction) throws StatusRuntimeException, TimeoutException{

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

				for (Map.Entry<String, PaxosServerGrpc.PaxosServerBlockingStub> server : this.registery.entrySet()) {
					String serverID = server.getKey();
					try {
						logger.info("Sending prepare to server: " + serverID);

						PaxosServerGrpc.PaxosServerBlockingStub stub = server.getValue();
						PromiseMessage promise = stub.prepare(Proposal.newBuilder().setProposalNumber(proposalNumber).setPartitionId(partitionId).build());
						logger.info("Received promise");

						Promise p = new Promise();
						p.setServerID(serverID);
						p.setProposalNumber(promise.getProposalNumber());
						p.setPreviousProposalNumber(promise.getPreviousProposalNumber());
						promises.add(p);
					} catch (StatusRuntimeException re) {
						logger.info("Received denial");
						continue;
					}
				}

				if (promises.size() <= this.registery.size() / 2) {
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
					PaxosServerGrpc.PaxosServerBlockingStub stub = registery.get(serverID);
					AcceptedMessage acceptedMessage = stub.accept(Proposal.newBuilder().setPartitionId(partitionId).setProposalNumber(proposalNumber).build());

					Accepted accepted = new Accepted();
					accepted.setProposalNumber(acceptedMessage.getProposalNumber());
					accepted.setServerID(promise.getServerID());
					accepteds.add(accepted);
					logger.info("Received accept");
				}
				catch(StatusRuntimeException re) {
					logger.info("Received reject");
					continue;
				}
			}

			if( accepteds.size() <= this.registery.size() / 2) {
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
				logger.info("Invoking learner: "+accepted.getServerID());
				PaxosServerGrpc.PaxosServerBlockingStub stub = registery.get(accepted.getServerID());
				stub.learn(getTransactionMessage(transaction, accepted));
				logger.info("Learner was able to successfully learn");

			}

			logger.info("Learning job finished");
			this.electedAsLeaderBefore.put(partitionId, true);
			this.multiPaxosProposalNumber.put(partitionId, proposalNumber);
			this.previousPromises.put(partitionId, promises);

			isRoundFailed = false;
		}
		logger.info("Paxos round ended");
	}

	private TransactionMessage getTransactionMessage(Transaction transaction, Accepted accepted) {
		TransactionMessage.Builder builder = TransactionMessage.newBuilder();
		builder.setServerId(accepted.getServerID());
		builder.setPartitionId(transaction.getPartitionID());
		builder.setType(transaction.getType());
		if (transaction.getKey() != null)
			builder.setKey(transaction.getKey());
		if (transaction.getValue() != null)
			builder.setValue(transaction.getValue());
		if (transaction.getValues() != null)
			builder.putAllValues(transaction.getValues());
		builder.setProposalNumber(accepted.getProposalNumber());
		return builder.build();
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

	public synchronized void invokeLearner(TransactionMessage transaction) {
		logger.info("Learner invoked");
		this.storage.setPartitionId(transaction.getPartitionId());

//		if(this.lastLearnedProposalNumber == accepted.getProposalNumber()) {
//			logger.info("Aborting learning, value is already learned");
//			throw new RemoteException();
//		}

		if(transaction.getServerId().equals(this.serverID)) {
			logger.info("Erasing previous proposal number and accepted value");
//			this.previousProposalNumber = 0;
//			this.previousAcceptedValue = null;
		}

		if(transaction.getType().equals("put")) {
			logger.info("Learner putting data in db");
			this.storage.put(transaction.getKey(), transaction.getValue());
			logger.info("Learned a new value: "+transaction.toString());
		}
		else if(transaction.getType().equals("delete")){
			logger.info("Learner deleting data in db");
			this.storage.remove(transaction.getKey());
		}
		else if (transaction.getType().equals("batch")) {
			logger.info("Learner putting data in db");
			this.storage.putAll(transaction.getValuesMap());
		}
		else if (transaction.getType().equals("clear")) {
			this.storage.clear();
		}
		this.lastLearnedProposalNumber = transaction.getProposalNumber();
	}

} 