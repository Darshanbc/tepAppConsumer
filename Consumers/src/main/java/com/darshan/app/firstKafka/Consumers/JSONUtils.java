package com.darshan.app.firstKafka.Consumers;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class JSONUtils {
		
	String JEDIS_HOST;
	protected String getJEDIS_HOST() {
		return JEDIS_HOST;
	}

	protected void setJEDIS_HOST(String jEDIS_HOST) {
		JEDIS_HOST = jEDIS_HOST;
	}

	protected String getMONGO_HOST() {
		return MONGO_HOST;
	}

	protected void setMONGO_HOST(String mONGO_HOST) {
		MONGO_HOST = mONGO_HOST;
	}

	protected String getKAFKA_BROKER() {
		return KAFKA_BROKER;
	}

	protected void setKAFKA_BROKER(String kAFKA_BROKER) {
		KAFKA_BROKER = kAFKA_BROKER;
	}

	protected int getMONGO_PORT() {
		return MONGO_PORT;
	}

	private void setMONGO_PORT(int mONGO_PORT) {
		MONGO_PORT = mONGO_PORT;
	}

	String MONGO_HOST;
	String KAFKA_BROKER;
	int MONGO_PORT;
		
	  public JSONUtils(){
		  Properties prop= new Properties();
			try {
				InputStream input = new FileInputStream("/home/config.properties");
				prop.load(input);
				setJEDIS_HOST( prop.getProperty("JEDIS_HOST"));
				setMONGO_HOST(prop.getProperty("MONGO_HOST"));
				setKAFKA_BROKER(prop.getProperty("KAFKA_BROKER"));
				setMONGO_PORT(Integer.parseInt(prop.getProperty("MONGO_PORT")));
//				System.out.println("Jedis: "+JEDIS_HOST+"\n"+"MONGO: "+MONGO_HOST+"\n"+"KAFKA_BROKER: "+KAFKA_BROKER);
			}catch(Exception e) {
				System.out.println(e.toString());
			}
	  }
	  
	  
	  
}
		  

