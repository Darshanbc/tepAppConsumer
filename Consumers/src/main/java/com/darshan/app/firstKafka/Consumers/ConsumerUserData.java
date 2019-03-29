package com.darshan.app.firstKafka.Consumers;

public class ConsumerUserData {
	static String topic="userData";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Starting userData App");
    	new ConsumerProcess(topic).processRecord();
	}

}
