package com.darshan.app.firstKafka.Consumers;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


@SuppressWarnings("deprecation")
public class DBOps implements Runnable{
	private static final String  USER_ID="admin";
	private static final String PASSWORD="Password123";
	private static final String HOST="localhost";
	private static final int PORT=27017;
	private static final String  DATABASE="myDb";
	private static final String COLLECTION_TESTCOLLECTION="testCollection";
	private static final String COLLECTION_DATAPOOL="dataPool";
	private static final String COLLECTION_USERDATA="userData";
	private static final String TRIP_ID="tripId";
	private static final String DRIVE_STATUS="DriveStatus";
	private static final String END_TRIP="END_TRIP";
	private static final String TOPIC_TESTCOLLECTION="testCollection";
	private static final String TOPIC_DATAPOOL="dataPool";
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
		setRecords(records);
		setTopic(topic);
		mongo = new MongoClient(HOST,PORT); 
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
		
		for (ConsumerRecord<String, String> record : getRecords()) {
			String value=record.value();
			value=value.replace("\\", "");
			value=value.substring(1, value.length()-1);
			Document document= Document.parse(value);
			System.out.println("Document"+document);
			
			if(getTopic()==TOPIC_DATAPOOL) {
				String hash=cryptoOps.getMd5(record.value()); //hash created
				System.out.println ("hash of Data"+hash);// printing hash
				this.redisClient.setValue(hash);

				try {
					tripId=document.getString(TRIP_ID);
					setRedisValue(tripId);
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();	
				}
				
					if(!this.redisClient.isValueExists(getRedisValue())) {
						System.out.println("value doesn't exist");
						this.redisClient.insertValue(tripId);
						list.add(document);
					}
					if(document.getString(DRIVE_STATUS).toLowerCase().equals(END_TRIP.toLowerCase())){
						redisClient.deleteAllRecord(tripId);
					}
			}else if(getTopic()==TOPIC_USERDATA) {
//						System.out.println("value already exist");
						document.append("tripID",0);
						list.add(document);
			}else {
				System.out.println(record.value());
			}
				
		}
				
		if(list.size()>0) {
			this.collection.insertMany(list); 
			System.out.println("inserted to db");
		}
//			this.collection.insertOne(doc);
		return;
	}
		
}
	
//	public BasicDBObject getDBObject(String data) {
//		if (data==null){
//			return null;
//		}
//		return (BasicDBObject)JSON.parse(data); 
//	}
//	
//	public static Document getDocument(DBObject doc)
//	{
//	   if(doc == null) return null;
//	   return new Document(doc.toMap());
//	}
//	public JSONObject jsonParser(String jsonString) {
//		JSONParser parser = new JSONParser();
//		JSONObject json = null;
//		try {
//			json = (JSONObject) parser.parse(jsonString);
//		
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return json;
//		
//	}

