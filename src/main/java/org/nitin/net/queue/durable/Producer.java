package org.nitin.net.queue.durable;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Producer {
	private static final String TASK_QUEUE = "task-queue";
	
	public static void main(String[] args) throws IOException {
		basicProduceWithDurableQueue();
	}
	
	private static void basicProduceWithDurableQueue() throws IOException {
		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.setHost("localhost");
		
		Connection connection = null;
		Channel channel = null;
		try {
			connection = connFactory.newConnection();
			channel = connection.createChannel();
			
			//Creates a durable queue			
			channel.queueDeclare(TASK_QUEUE, true, false, false, null);
			
			String message = "Hello World";
			channel.basicPublish("", TASK_QUEUE, null, message.getBytes());
			System.out.println("Message sent");
		} finally {
			if(channel != null)
				channel.close();
			if(connection != null)
				connection.close();
			
		}
	}
}
