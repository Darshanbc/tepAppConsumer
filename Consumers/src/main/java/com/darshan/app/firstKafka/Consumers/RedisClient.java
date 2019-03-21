package com.darshan.app.firstKafka.Consumers;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import redis.clients.jedis.Jedis;

public class RedisClient {
	
	private static String JEDIS_HOST="localhost";
	Jedis jedis=null;
	private String value;
	
	

	private String getValue() {
		return value;
	}
	private void setValue(String value) {
		this.value = value;
	}
	
	
	
	public RedisClient() {
		Properties prop= new Properties();
		try {
			InputStream input = new FileInputStream("./config.properties");
			prop.load(input);
			JEDIS_HOST=prop.getProperty("JEDIS_HOST");
			this.jedis =this.createClient();
			this.setValue("");
		}catch(Exception e) {
			System.out.println(e.toString());
		}
        
		
	}
	private Jedis createClient() {
		return  new Jedis(JEDIS_HOST);
		
	}
	
	public boolean isValueExists(String key) {
		return this.jedis.sismember(key,this.getValue());
	}
	public void insertValue(String key) {
		this.jedis.sadd(key, this.getValue());
	}

}
