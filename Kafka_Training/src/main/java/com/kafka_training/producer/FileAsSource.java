package com.kafka_training.producer;

import java.util.Properties;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class FileAsSource {

	public static void main(String[] args) {
		
		Properties properties=new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String,String> myProducer= new KafkaProducer<String,String>(properties);
		FileInputStream fis; 
		BufferedReader br = null;
		try {
			
			fis = new FileInputStream("src/main/resources/sample-text");
			br = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			while((line= br.readLine()) != null) {
				//System.out.println(line);
				myProducer.send(new  ProducerRecord<String, String>("test",line));
			 }
			
	
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
				myProducer.close();
		}

		
		
	}

}
