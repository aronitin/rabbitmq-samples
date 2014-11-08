/**
 * 
 */
package org.nitin.net.exchange.direct;

import java.io.IOException;
import java.util.Random;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Consumer for direct exchanges with routing keys randomly generated.
 * @author nitinarora
 *
 */
public class Consumer {

	public static void main(String[] args) throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.setHost("localhost");

		Connection connection = null;
		Channel channel = null;
		try {
			connection = connFactory.newConnection();
			channel = connection.createChannel();
			
			String EXCHANGE_NAME = "direct-logs";
			
			channel.exchangeDeclare(EXCHANGE_NAME, "direct");
			String queueName = channel.queueDeclare().getQueue();
			
			Random rand = new Random();
	        int value = rand.nextInt(3);
	        
	        String severity = "info";
	        if(value == 0) severity = "info";
	        else if (value == 1) severity = "error";
	        else if (value == 2) severity = "warning";
	        
	        System.out.println(String.format("Severity generated is %s", severity));
			
			channel.queueBind(queueName, EXCHANGE_NAME, severity);	

			System.out
					.println("[*] Waiting for messages. Press Ctrl+C to exit");

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, true, consumer);

			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody());
				String routingKey = delivery.getEnvelope().getRoutingKey();
				System.out.println(" [x] Received '" + routingKey + "':'" + message + "'");
			}
		} finally {
			if (channel != null)
				channel.close();
			if (connection != null)
				connection.close();
		}


	}

}
