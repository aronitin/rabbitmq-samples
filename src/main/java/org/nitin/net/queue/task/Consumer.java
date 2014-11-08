/**
 * 
 */
package org.nitin.net.queue.task;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class Consumer {
	private static final String QUEUE_NAME = "hello";
	private static final String TASK_QUEUE = "task-queue";

	public static void main(String[] args) throws IOException,
			ShutdownSignalException, ConsumerCancelledException,
			InterruptedException {
		consumeWithFairDispatch();
	}

	private static void consumeWithDurableQueue() throws IOException,
			ShutdownSignalException, ConsumerCancelledException,
			InterruptedException {
		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.setHost("localhost");

		Connection connection = null;
		Channel channel = null;
		try {
			connection = connFactory.newConnection();
			channel = connection.createChannel();

			// creates a durable queue
			channel.queueDeclare(QUEUE_NAME, true, false, false, null);

			System.out
					.println("[*] Waiting for messages. Press Ctrl+C to exit");

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(TASK_QUEUE, false, consumer);

			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody());
				Thread.sleep(5000);
				System.out.println(" [x] Received '" + message + "'");
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			}
		} finally {
			if (channel != null)
				channel.close();
			if (connection != null)
				connection.close();
		}
	}

	/**
	 * By default RabbitMQ dispatches messages in a round robin fashion and
	 * criteria is dispath n-th message to n-th consumer. In a situation where
	 * there are 2 workers, when all odd messages are heavy and even messages
	 * are light, 1 worker will be busy and the other will be relatively free.
	 * 
	 * To resolve this issue enforce Fair Dispatch policy using Qos parameters.
	 */
	private static void consumeWithFairDispatch() throws IOException,
			ShutdownSignalException, ConsumerCancelledException,
			InterruptedException {
		ConnectionFactory connFactory = new ConnectionFactory();
		connFactory.setHost("localhost");

		Connection connection = null;
		Channel channel = null;
		try {
			connection = connFactory.newConnection();
			channel = connection.createChannel();

			// creates a durable queue
			channel.queueDeclare(TASK_QUEUE, true, false, false, null);

			System.out
					.println("[*] Waiting for messages. Press Ctrl+C to exit");
			
			channel.basicQos(1);

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(TASK_QUEUE, false, consumer);

			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				String message = new String(delivery.getBody());
				Thread.sleep(5000);
				System.out.println(" [x] Received '" + message + "'");
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			}
		} finally {
			if (channel != null)
				channel.close();
			if (connection != null)
				connection.close();
		}
	}
}
