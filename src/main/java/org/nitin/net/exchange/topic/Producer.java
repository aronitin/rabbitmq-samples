package org.nitin.net.exchange.topic;

import java.io.IOException;
import java.util.Random;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Producer with topic exchange.
 * 
 * @author nitinarora
 *
 */
public class Producer {
	public static void main(String[] args) throws IOException {
		String EXCHANGE_NAME = "topic-logs";

		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.setHost("localhost");

		Connection connection = null;
		Channel channel = null;
		try {
			connection = connFactory.newConnection();
			channel = connection.createChannel();

			// declare logs exchange with direct type.
			channel.exchangeDeclare(EXCHANGE_NAME, "topic");

			String message = "Log Message";

			String routingKey = getRoutingKey();
			channel.basicPublish(EXCHANGE_NAME, routingKey, null,
					message.getBytes());

			System.out.println(" [x] Sent '" + routingKey + "':'" + message
					+ "'");

		} finally {
			if (channel != null)
				channel.close();
			if (connection != null)
				connection.close();
		}
	}

	private static String getRoutingKey() {
		Random rand = new Random();
        int value = rand.nextInt(3);
        
        String severity = "kern.critical";
        if(value == 0) severity = "io.critical";
        else if (value == 1) severity = "sys.critical";
        else if (value == 2) severity = "kern.warning";
		
        return severity;
	}
}
