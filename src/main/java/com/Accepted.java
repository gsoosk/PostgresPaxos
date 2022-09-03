package com;

import java.io.Serializable;

public class Accepted implements Serializable {
	
	private String serverID;
	
	private long proposalNumber;
	
	private Transaction value;

	public long getProposalNumber() {
		return proposalNumber;
	}

	public void setProposalNumber(long proposalNumber) {
		this.proposalNumber = proposalNumber;
	}

	public Transaction getValue() {
		return value;
	}

	public void setValue(Transaction value) {
		this.value = value;
	}

	public String getServerID() {
		return serverID;
	}

	public void setServerID(String serverID) {
		this.serverID = serverID;
	}
}
