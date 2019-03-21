package com.darshan.app.firstKafka.Consumers;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

public class DBOps implements Runnable{
	private static final String  USER_ID="admin";
	private static final String PASSWORD="Password123";
	private static final String HOST="localhost";
	private static final int PORT=27017;
	private static final String  DATABASE="myDb";
	private static final String COLLECTION="testCollection";
	static List<Document> list =new ArrayList<Document>(); 
	public void run() {
		   MongoClient mongo = new MongoClient(HOST,PORT); 
		   MongoDatabase database = mongo.getDatabase(DATABASE);
		   MongoCollection<Document> collection= database.getCollection(COLLECTION);
		   Document doc = Document.parse(record)
				   
				   List<WriteModel<Document>> updates = new ArrayList<WriteModel<Document>>();
//			 Arrays.<WriteModel<Document>>asList(
		       
//		    );
	 
	

}

}
