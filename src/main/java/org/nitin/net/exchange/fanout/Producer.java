package org.nitin.net.exchange.fanout;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Producer {
	public static void main(String[] args) throws IOException {
		emitLog();
	}
	
	/**
	 * To implement publish/susbscribe paradigm fanout exchange should be created.
	 * Queues are generated with random names and destroyed when consumer are disconnected.
	 * @throws IOException
	 */
	private static void emitLog() throws IOException {
		String EXCHANGE_NAME = "logs";
		
		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.setHost("localhost");
		
		Connection connection = null;
		Channel channel = null;
		try {
			connection = connFactory.newConnection();
			channel = connection.createChannel();
			
			//declare logs exchange with fanout type.			
			channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		
			String message = "Log Message";
			channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
			System.out.println("Message sent");
		} finally {
			if(channel != null)
				channel.close();
			if(connection != null)
				connection.close();
			
		}
	}
}
