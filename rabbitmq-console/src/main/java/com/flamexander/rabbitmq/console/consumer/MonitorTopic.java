package com.flamexander.rabbitmq.console.consumer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import javax.annotation.processing.AbstractProcessor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MonitorTopic {

	private static final String EXCHANGE_NAME;
	private static volatile BufferedReader reader;
	private Thread thread;
	private Channel channel;
	private String  queueName;
	private String currentTopic="";

	static {
		EXCHANGE_NAME = "blog_exchange";
		reader = new BufferedReader(new InputStreamReader(System.in));
	}

	public MonitorTopic(Connection connection) throws IOException {
		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), "UTF-8");
			System.out.println(" [x] Received '" + message + "'");
			System.out.println(Thread.currentThread().getName());
		};
		channel = connection.createChannel();
		channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

		queueName = channel.queueDeclare().getQueue();
		System.out.println("My queue name: " + queueName);

		channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

		System.out.println("Input command 'set_topic' (topic name): ");
	}

	public void start() {
		this.thread = new Thread(() -> {
			while(this.threadNotInterrapted().getAsBoolean()) {

				if (this.readerIsReady().test(reader)) {

					this.readLine().accept(reader);
				}
			}

		});
		this.thread.start();
	}

	public BooleanSupplier threadNotInterrapted() {
		return () -> {
			return !Thread.currentThread().isInterrupted();
		};
	}

	public Predicate<BufferedReader> readerIsReady() {
		return (t) -> {
			try {
				return t.ready();
			} catch (IOException var2) {
				return false;
			}
		};
	}

	public Consumer<BufferedReader> readLine() {
		return (t) -> {
			try {

				String newTopic = t.readLine();

				if (newTopic.startsWith("set_topic ")) {
					newTopic = Arrays.stream(newTopic.split(" ")).skip(1).collect(Collectors.joining(" "));
					System.out.println("Topic name for unBind: " + currentTopic);
					channel.queueUnbind(queueName, EXCHANGE_NAME, currentTopic);
					currentTopic = newTopic;
					System.out.println("Topic name: " + currentTopic);
					channel.queueBind(queueName, EXCHANGE_NAME, currentTopic);
					System.out.println("Input command 'set_topic' (topic name): ");
				}
				else if (newTopic.startsWith("stop")){
					stop();
				}
				else {
					System.out.println("Input command 'set_topic' (topic name): ");
				}
			} catch (IOException var5) {
				stop();
			}
		};
	}

	public void stop() {
		System.out.println("Stop consuming");
		try {
			channel.close();
			channel.getConnection().close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		finally {
			this.thread.interrupt();
		}

	}
}
