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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
/**
 * Hello world!
 *l
 */
public class Consumer {
	 Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
    public static void main( String[] args ){

    	System.out.println("Starting App");
    	new ConsumerProcess().processRecord();
    }
}
class Dbops{
	String  USER_ID="admin";
	String PASSWORD="Password123";

	 public void run(String jsonObj) {
		  // Creating a Mongo client 
	      MongoClient mongo = new MongoClient( "localhost" , 27017 ); 
	   
	      // Creating Credentials 
//	      MongoCredential credential; 
//	      credential = MongoCredential.createCredential(USER_ID, "myDb", 
//	         PASSWORD.toCharArray()); 
	      System.out.println("Connected to the database successfully");  
	      
	      // Accessing the database 
	      MongoDatabase database = mongo.getDatabase("myDb"); 
//	      System.out.println("Credentials ::"+ credential);    
	      MongoCollection<Document> collection= database.getCollection("testCollection");
	      Document doc = Document.parse(jsonObj);
//	      DBObject dbObject = (DBObject)JSON.parse(jsonObj);
//	      Document document = new Document("title", "MongoDB") 
//	    	      .append("id", 1)
//	    	      .append("description", "database") 
//	    	      .append("likes", 100) 
//	    	      .append("url", "http://www.tutorialspoint.com/mongodb/") 
//	    	      .append("by", "tutorials point");  
//	    collection.up  (doc); 
//	      collection.find
	    mongo.close();
	      System.out.println("Document Inserted successfully");
	 }  
}

class ConsumerProcess{
	public ConsumerProcess() {};
	public void processRecord() {
		Properties props = new Properties();
	    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
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
	    consumer.subscribe(Arrays.asList("odd"));
	  
	    int counter = 0;

	    try {
//	        consumer.subscribe(topics);
	    	Logger logger=new Consumer().logger;
	        while (true) {
//	        	  System.out.println("subscribing ")
	            @SuppressWarnings({ "deprecation", "unchecked" })
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	            if (records.count()>0) {
	            	System.out.println("No. of Records"+records.count());
	            	Runnable r=	new DBOps(records);
	 	           new Thread(r).start(); 	
	            }
	           
//	            for (ConsumerRecord<String, String> record : records) {
//	            	System.out.print("Partition= "+record.partition()+", key ="+record.key()+", value = "+record.value()+"\n");
//	            new Dbops().run(record.value());
//	        }
	        }
	    } catch (Exception e) {
	        System.out.println(e.toString());
	    } finally {
	        consumer.close();
	    }
	}
	
	public JSONObject jsonParser(String jsonString) {
		JSONParser parser = new JSONParser();
		JSONObject json = null;
		try {
			json = (JSONObject) parser.parse(jsonString);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return json;
	}
}


