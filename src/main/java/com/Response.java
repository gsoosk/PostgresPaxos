package com;

import java.io.Serializable;

//The com.Response class contains information about the response sent from server to client
public class Response implements Serializable {

	private static final long serialVersionUID = 8949718079867932728L;
	
	// Type of the request to which server is responding
	private String type;
	
	// return value of the request
	private String returnValue;
	
	// Message describing what happened on the server side
	private String message;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getReturnValue() {
		return returnValue;
	}

	public void setReturnValue(String returnValue) {
		this.returnValue = returnValue;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "com.Response [type=" + type + ", returnValue=" + returnValue
				+ ", message=" + message + "]";
	}
}
