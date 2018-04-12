package com.kafka_trianing.consumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MysqlAsConsumer {

	
	public static void main(String[] args) {
		

		Properties properties=new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("group.id","test1");
		
		KafkaConsumer< String, String> consumer=new KafkaConsumer<String, String>(properties);
		
		
		ArrayList<String> topics=new ArrayList<String>();
		topics.add("test");
		
		consumer.subscribe(topics); // You can subscribe to any number of topics.
		
		try {
			
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/datasource","root","root");
			Statement stmt = con.createStatement();			
			while(true){
				
				ConsumerRecords<String, String> records = consumer.poll(1000);
				
				for(ConsumerRecord<String, String> record : records){
					
					System.out.println(record.value());
					String Message = record.value().toString();			
					Integer empno = new Integer(Message.split("\\|")[0]);
					String ename= new String(Message.split("\\|")[1]);
					String job= new String(Message.split("\\|")[2]);
					String query = "INSERT INTO emp1 VALUES('" + empno + "', '" + ename + "','" + job + "')";
					stmt.executeUpdate(query);
				}
			}
			
		} catch (Exception e) {
			System.out.println("Inside exception loop : ");
			e.printStackTrace();
		}finally{
			consumer.close();
		}
		
	}

}
