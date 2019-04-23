package com.darshan.app.firstKafka.Consumers;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import redis.clients.jedis.Jedis;

public class RedisClient {
	
	public RedisClient() {

	}
	
	@SuppressWarnings("unused")
	public Long insert(String key, String value) {
		System.out.println("key: "+key);
		System.out.println("value: "+value);
		Long result;
		JSONUtils jsonutils = new JSONUtils();
		@SuppressWarnings("resource")
		Jedis jedis= new Jedis(jsonutils.getJEDIS_HOST(),6379);
		System.out.println("jedis host: "+jsonutils.getJEDIS_HOST());
		if (jedis==null) {
			 System.out.println("jedis is null"); 
		 }
		try {
			result =jedis.sadd(key,value);
		}catch(Exception err) {
			System.out.println("Error string: "+err.toString());
			result = (long) 0;
		}
		return result;
	}
	
	
	public void deleteAllRecord(String key) {
		Jedis jedis= new Jedis(new JSONUtils().JEDIS_HOST,6379);
		jedis.del(key);
	}
	
	
}
