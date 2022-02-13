package com.flamexander.rabbitmq.console.consumer;

import com.rabbitmq.client.*;

import javax.annotation.processing.AbstractProcessor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

public class BlogConsumerApp {

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("user");
        factory.setPassword("bitnami");
        Connection connection = factory.newConnection();

        MonitorTopic monitorTopic = new MonitorTopic(connection);
        monitorTopic.start();
    }
}
