package com.darshan.app.firstKafka.Consumers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Consumer {
	static String topic="DataPool";
	 Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
    public static void main( String[] args ){
    	System.out.println("Starting App");
    	new ConsumerProcess(topic).processRecord();
    }
}

class ConsumerProcess{
	private static String topic;
	
	private static String getTopic() {
		return topic;
	}

	private static void setTopic(String topic) {
		ConsumerProcess.topic = topic;
	}

	public ConsumerProcess(String topic) {
		setTopic(topic);;
	};
	
	public void processRecord() {
		
		Properties props = new Properties();
	    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
	    props.put("acks", "all");
	    props.put("retries", 0);
	    props.put("batch.size", 16384);
	    props.put("linger.ms", 1);
	    props.put("buffer.memory", 33554432);
	    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "app1");
	    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	    consumer.subscribe(Arrays.asList(topic));
	  
//	    int counter = 0;

	    try {
//	        consumer.subscribe(topics);
//	    	Logger logger=new Consumer().logger;
	        while (true) {
//	        	  System.out.println("subscribing to topic "+getTopic());
	            @SuppressWarnings({ "deprecation", "unchecked" })
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	            if (records.count()>0) {
	            	System.out.println("No. of Records"+records.count());
	            	Runnable r=	new DBOps(records,getTopic());
	 	           new Thread(r).start(); 	
	            }
	        }
	    } catch (Exception e) {
	        System.out.println(e.toString());
	    } finally {
	        consumer.close();
	    }
	}
	
	
}


//for (ConsumerRecord<String, String> record : records) {
//System.out.print("Partition= "+record.partition()+", key ="+record.key()+", value = "+record.value()+"\n");
//new Dbops().run(record.value());
