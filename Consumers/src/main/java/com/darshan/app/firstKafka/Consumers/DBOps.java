package com.darshan.app.firstKafka.Consumers;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

@SuppressWarnings("deprecation")
public class DBOps implements Runnable{
	private static final String  USER_ID="admin";
	private static final String PASSWORD="Password123";
	private static final String HOST="localhost";
	private static final int PORT=27017;
	private static final String  DATABASE="myDb";
	private static final String COLLECTION="testCollection";
	private static final String TRIP_ID="tripId";
	private static final String DRIVE_STATUS="DriveStatus";
	private static final String END_TRIP="END_TRIP";
	
	static List<Document> list =new ArrayList<Document>(); 
	private ConsumerRecords<String, String> records;
			MongoClient mongo;
			MongoDatabase database;
			MongoCollection<Document> collection;
			RedisClient redisClient;
			CryptoOps cryptoOps;
			
	private ConsumerRecords<String, String> getRecords() {
		return records;
	}
	
	private void setRecords(ConsumerRecords<String, String> records) {
		this.records = records;
	}
	
	public DBOps(ConsumerRecords<String, String> records) {
		setRecords(records);
		mongo = new MongoClient(HOST,PORT); 
		database = mongo.getDatabase(DATABASE);
		collection= database.getCollection(COLLECTION);
		this.redisClient= new RedisClient();
		this.cryptoOps= new CryptoOps();
		
	}
	
	public void run() {
		
		List<Document> list =new ArrayList<Document>();
		for (ConsumerRecord<String, String> record : this.records) {
			System.out.println("record value: "+record.value());
//			
//			String json=gson.toJson(record.value());
			BasicDBObject dbo = getDBObject(record.value());
			Document document= new Document(dbo);//getDocument(json);
			String tripId=document.getString(TRIP_ID);
			System.out.println("Trip Id: "+tripId);
			String hash=cryptoOps.getMd5(record.value());
			System.out.println("hash of Data"+hash);
			this.redisClient.setValue(hash);
			
			if(!this.redisClient.isValueExists(tripId)) {
				this.redisClient.insertValue(tripId);
				list.add(document);
			}
			if(document.getString(DRIVE_STATUS).toLowerCase().equals(END_TRIP.toLowerCase())){
				redisClient.deleteAllRecord(tripId);
			}
		}
			this.collection.insertMany(list);   

			return;
}
	public BasicDBObject getDBObject(String data) {
		if (data==null){
			return null;
		}
		return (BasicDBObject)JSON.parse(data); 
	}
	
	public static Document getDocument(DBObject doc)
	{
	   if(doc == null) return null;
	   return new Document(doc.toMap());
	}
}
