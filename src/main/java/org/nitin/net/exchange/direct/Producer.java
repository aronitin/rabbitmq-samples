package org.nitin.net.exchange.direct;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Producer with direct exchange. Exchange will receive messages with routing key and will be delivered to Queue where
 * x.routing_key=q.binding_key
 * @author nitinarora
 *
 */
public class Producer {
	public static void main(String[] args) throws IOException {
		String EXCHANGE_NAME = "direct-logs";

		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.setHost("localhost");

		Connection connection = null;
		Channel channel = null;
		try {
			connection = connFactory.newConnection();
			channel = connection.createChannel();

			// declare logs exchange with direct type.
			channel.exchangeDeclare(EXCHANGE_NAME, "direct");

			String message = "Log Message";
			
			String severity = "info";
			for (int i = 0; i < 3; i++) {
				if(i==0)
					severity = "info";
				else if (i == 1) 
					severity = "error";
				else if(i == 2) 
					severity = "warning";
				
				channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes());
				System.out.println(String.format("Message with severity %s sent.", severity));
			}
		} finally {
			if (channel != null)
				channel.close();
			if (connection != null)
				connection.close();
		}
	}
}
