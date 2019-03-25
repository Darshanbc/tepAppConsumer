package com.darshan.app.firstKafka.Consumers;

import java.io.IOException;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;

public class JSONUtils {
	private static final Gson gson = new Gson();

	  JSONUtils(){}

	  public static HashMap<String,Object>  isJSONValid(String jsonInString) {
//	      try {
//	          gson.fromJson(jsonInString, Object.class);
//	          return true;
//	      } catch(com.google.gson.JsonSyntaxException ex) { 
//	          return false;
//	      }
//		  try {
//		       final ObjectMapper mapper = new ObjectMapper();
//		       ObjectNode jsonnode =(ObjectNode) mapper.readTree(jsonInString);
//		       
//		       return true;
//		    } catch (IOException e) {
//		       return false;
//		    }
		  
		  try {
			HashMap<String,Object> result = new ObjectMapper().readValue(jsonInString, HashMap.class);
			return result;
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	  }
}
		  

