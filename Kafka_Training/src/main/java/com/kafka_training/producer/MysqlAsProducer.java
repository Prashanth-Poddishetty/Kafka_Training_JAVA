package com.kafka_training.producer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;



public class MysqlAsProducer {

	public static void main(String[] args) {
		
		Properties properties=new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String,String> myProducer= new KafkaProducer<String,String>(properties);

		try {
			
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/datasource","root","root");
			Statement stmt = con.createStatement();
			ResultSet rs= stmt.executeQuery("select * from emp;");
			ResultSetMetaData rsmd = rs.getMetaData();
			int col_num = rsmd.getColumnCount();
			while (rs.next())
			{	
				
				String msg_value = "";		
				
				for(int i=1; i<=col_num; i++) {
					
					if(i<=(col_num-1))
					{
						msg_value = msg_value + rs.getString(i) + "|";
					}
					else {
						msg_value = msg_value + rs.getString(i);
					}
					//System.out.print(msg_value);
					
				}
				myProducer.send(new  ProducerRecord<String, String>("test",  msg_value));
				System.out.println(msg_value);
				msg_value = "";				
			}	
			rs.close();
			stmt.close();
			con.close();
	} catch (Exception e) {
		e.printStackTrace();
	}finally{
		myProducer.close();
	}
		
		
		
	}

}
