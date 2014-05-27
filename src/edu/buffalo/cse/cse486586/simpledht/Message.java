package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;

public class Message implements Serializable {

	private String key;
	private String value;
	private static String type;	
	
	

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
}


