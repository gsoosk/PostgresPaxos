package com;

import java.io.Serializable;

//The Request class contains information about the request sent from client to server
public class Transaction implements Serializable{

	private static final long serialVersionUID = -5548531573238110706L;

	// Request type sent by the client
	private String type;
	
	// Data in the request
	private String key;
	private String value;

	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	@Override
	public String toString() {
		return "com.Transaction [type=" + type + ", key=" + key + ", value=" + value
				+ "]";
	}
	
	
	
}
