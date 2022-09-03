package com;

import java.io.Serializable;

public class Promise implements Serializable {
	
	private String serverID;
	
	private long proposalNumber;
	
	private long previousProposalNumber;
	 
	private Transaction previousAcceptedValue;


	public String getServerID() {
		return serverID;
	}

	public void setServerID(String serverID) {
		this.serverID = serverID;
	}

	public long getPreviousProposalNumber() {
		return previousProposalNumber;
	}

	public void setPreviousProposalNumber(long previousProposalNumber) {
		this.previousProposalNumber = previousProposalNumber;
	}

	public long getProposalNumber() {
		return proposalNumber;
	}

	public void setProposalNumber(long proposalNumber) {
		this.proposalNumber = proposalNumber;
	}

	public Transaction getPreviousAcceptedValue() {
		return previousAcceptedValue;
	}

	public void setPreviousAcceptedValue(Transaction previousAcceptedValue) {
		this.previousAcceptedValue = previousAcceptedValue;
	}

}
