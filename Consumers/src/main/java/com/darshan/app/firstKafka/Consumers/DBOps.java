package com.darshan.app.firstKafka.Consumers;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


@SuppressWarnings("deprecation")
public class DBOps implements Runnable{
	private static final String  USER_ID="admin";
	private static final String PASSWORD="Password123";
	private static final String HOST="localhost";
	private static final int PORT=27017;
	private static final String  DATABASE="DataLake";
	private static final String COLLECTION_TESTCOLLECTION="testCollection";
	private static final String COLLECTION_DATAPOOL="trip_data";
	private static final String COLLECTION_USERDATA="user_data";
	private static final String TRIP_ID="tripId";
	private static final String DRIVE_STATUS="DriveStatus";
	private static final String END_TRIP="END_TRIP";
	private static final String TOPIC_TESTCOLLECTION="testCollection";
	private static final String TOPIC_DATAPOOL="DataPool";
	private static final String TOPIC_USERDATA="userData";
	static List<Document> list =new ArrayList<Document>(); 
	private ConsumerRecords<String, String> records;
			MongoClient mongo;
			MongoDatabase database;
			MongoCollection<Document> collection;
			RedisClient redisClient;
			CryptoOps cryptoOps;
			String topic;
			String redisValue;

			
	//------------------------------------Constructor------------------------------------
	public DBOps(ConsumerRecords<String, String> records,String topic) {
		
		System.out.println("toipc: "+topic);
		setRecords(records);
		JSONUtils jsonutils= new JSONUtils();
		setTopic(topic);
		mongo = new MongoClient(jsonutils.getMONGO_HOST(),jsonutils.getMONGO_PORT()); 
		database = mongo.getDatabase(DATABASE);
		if (topic==TOPIC_USERDATA) {
			collection= database.getCollection(COLLECTION_USERDATA);
		}else if(topic==TOPIC_DATAPOOL) {
			collection=database.getCollection(COLLECTION_DATAPOOL);
		}else {
			collection=database.getCollection(COLLECTION_TESTCOLLECTION);
		}
		this.redisClient= new RedisClient();
		this.cryptoOps= new CryptoOps();
		
	}
	//------------------------------getters and setters----------------------------------------	
	private String getTopic() {
		return topic;
	}


	private void setTopic(String topic) {
		this.topic = topic;
	}

	private ConsumerRecords<String, String> getRecords() {
		return records;
	}
	
	private void setRecords(ConsumerRecords<String, String> records) {
		this.records = records;
	}
	
	private String getRedisValue() {
		return redisValue;
	}

	private void setRedisValue(String redisValue) {
		this.redisValue = redisValue;
	}

	//---------------------------------run() for thread ----------------------------------
	public void run() {
		
		List<Document> list =new ArrayList<Document>();
		String tripId = null;
		
		for (ConsumerRecord<String, String> record : this.getRecords()) {
			System.out.println(record.value());
			String value=record.value();

			value=value.replace("\\", "");

			Document document= Document.parse(value);                              
			System.out.println("Document: "+document);

			if(this.getTopic()==TOPIC_DATAPOOL) {
				String hash=cryptoOps.getMd5(record.value()); //hash created
				System.out.println ("hash of Data"+hash);// printing hash

				
				try {
					tripId=document.getInteger(TRIP_ID).toString();
					this.setRedisValue(tripId);
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();	
				}
				System.out.println("hash:"+hash);
				System.out.println("tripId:"+tripId);
				if (this.redisClient.insert(hash,tripId)==1) {
					System.out.println("adding element to ");
					list.add(document);
				}
				
//				if(document.getString(DRIVE_STATUS).toLowerCase().equals(END_TRIP.toLowerCase())){
//					redisClient.deleteAllRecord(tripId);
//				}
			}else if(this.getTopic()==TOPIC_USERDATA) {

						list.add(document);
			}else {
				System.out.println(record.value());
			}
				
		}
				
		if(list.size()>0) {
			this.collection.insertMany(list); 
			System.out.println("inserted to db");
		}
		return;
	}
		
}
	


