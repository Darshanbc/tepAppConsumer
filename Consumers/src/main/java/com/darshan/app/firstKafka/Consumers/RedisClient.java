package com.darshan.app.firstKafka.Consumers;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import redis.clients.jedis.Jedis;

public class RedisClient {
	
	private static String JEDIS_HOST="localhost";
//	private Jedis jedis;

	
	
	
	public RedisClient() {
		Properties prop= new Properties();
//		Jedis jedis=null;
		try {
			InputStream input = new FileInputStream("./config.properties");
			prop.load(input);
			JEDIS_HOST=prop.getProperty("JEDIS_HOST");
//			jedis =this.createClient();
		}catch(Exception e) {
			System.out.println(e.toString());
		}		
	}
	private Jedis createClient() {
		return  new Jedis(JEDIS_HOST);
		
	}
	
	
	public Long insert(String key, String value) {
		System.out.println("key: "+key);
		System.out.println("value: "+value);
		Long result;
		Jedis jedis= new Jedis("localhost",6379);
		if (jedis==null) {
			 System.out.println("jedis is null"); 
		 }
		try {
			result =jedis.sadd(key,value);
		}catch(Exception err) {
			System.out.println("Exception occured");
			result = (long) 0;
		}
		return result;
	}
	
	
	public void deleteAllRecord(String key) {
		Jedis jedis= new Jedis("localhost",6379);
		jedis.del(key);
	}
	
	
}
