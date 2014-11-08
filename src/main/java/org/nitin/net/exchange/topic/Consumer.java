/**
 * 
 */
package org.nitin.net.exchange.topic;

import java.io.IOException;
import java.util.Random;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Consumer for topic exchange with routing keys randomly generated. Multiple
 * bindings can be created for a queue by calling queueBind repititively.
 * 
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
			
			String EXCHANGE_NAME = "topic-logs";
			
			channel.exchangeDeclare(EXCHANGE_NAME, "topic");
			String queueName = channel.queueDeclare().getQueue();
	        
			String bindingKey = getBindingKey();
	        System.out.println(String.format("Routing key generated is %s", bindingKey));
			
			channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);	

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

	private static String getBindingKey() {
		Random rand = new Random();
        int value = rand.nextInt(3);
        
        String bindingKey = "#";
        if(value == 0) bindingKey = "#";
        else if (value == 1) bindingKey = "kern.*";
        else if (value == 2) bindingKey = "*.critical";
        
        return bindingKey;
	}
}
