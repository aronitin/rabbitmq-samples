package org.nitin.net.queue.basic;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Producer {
	private static final String QUEUE_NAME = "hello";
	
	public static void main(String[] args) throws IOException {
		basicProduce();
	}
	
	private static void basicProduce() throws IOException {
		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.setHost("localhost");
		
		Connection connection = null;
		Channel channel = null;
		try {
			connection = connFactory.newConnection();
			channel = connection.createChannel();
			
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			
			String message = "Hello World";
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
			System.out.println("Message sent");
		} finally {
			if(channel != null)
				channel.close();
			if(connection != null)
				connection.close();
			
		}
	}
}
