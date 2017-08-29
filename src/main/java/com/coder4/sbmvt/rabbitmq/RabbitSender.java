/**
 * @(#)BaseSender.java, Aug 28, 2017.
 * <p>
 * Copyright 2017 fenbi.com. All rights reserved.
 * FENBI.COM PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.coder4.sbmvt.rabbitmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author coder4
 */
public abstract class RabbitSender<T> {

    private static final int SENDER_RETRY_QUEUE_MAX_SIZE = 1024 * 1024 * 1024;

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    private ObjectMapper jsonObjectMapper;

    private RabbitClient rabbitClient;

    protected LinkedBlockingDeque<T> retryQueue;

    private Thread retryThread;

    private Channel retryChannel;

    private Channel senderChannel;

    private void init() {

        jsonObjectMapper = new ObjectMapper();

        retryQueue = new LinkedBlockingDeque<>(SENDER_RETRY_QUEUE_MAX_SIZE);

        try {
            senderChannel = rabbitClient.createChannel();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        start();
    }

    public void stop() {
        // wait for empty
        while (!retryQueue.isEmpty()) {
            try {
                LOG.info("retryQueue is not empty, sleep.");
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Close all resources
        try {
            senderChannel.close();
            retryChannel.close();
            retryThread.interrupt();
            retryThread.join();
        } catch (Exception e) {
            LOG.warn("join retryThread failed", e);
        }
        rabbitClient.stop();
    }

    private void start() {
        retryThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {

                    try {
                        if (retryChannel == null || !retryChannel.isOpen()) {
                            retryChannel = rabbitClient.createChannel();
                        }
                    } catch (Exception e) {
                        LOG.error("Create retryThread failed.", e);
                        Thread.sleep(500);
                        continue;
                    }

                    T msg = null;
                    try {
                        msg = retryQueue.take();
                        resend(msg);
                        LOG.info("RabbitSender resend success");
                    } catch (Exception t) {
                        if (t instanceof InterruptedException) {
                            throw (InterruptedException) t;
                        }

                        LOG.error("RabbitSender resend exception", t);
                        if (msg != null) {
                            retryQueue.putLast(msg);
                        }
                        if (retryChannel != null) {
                            RabbitClient.closeChannel(retryChannel);
                            retryChannel = null;
                        }
                    }
                }
            } catch (InterruptedException e) {
                // will exit
                LOG.info("RabbitSenderThread will exit", e);
            }
        });

        retryThread.start();
    }

    public void send(T msg) {
        try {
            doSend(senderChannel, msg);
        } catch (Exception e) {
            LOG.error("RabbitSender send exception, will resend", e);
            retryQueue.offer(msg);
        }
    }

    private void resend(T msg) throws IOException {
        doSend(retryChannel, msg);
    }

    private void doSend(Channel channel, T msg) throws IOException {
        byte[] payload = serialize(msg);
        channel.basicPublish(
                getExchangeName(),
                getRoutingKey(msg),
                false,
                false,
                MessageProperties.MINIMAL_PERSISTENT_BASIC,
                payload);

        LOG.info("RabbitSender success send a msg.");
    }

    protected byte[] serialize(T msg) {
        try {
            return jsonObjectMapper.writeValueAsBytes(msg);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract String getExchangeName();

    protected String getRoutingKey(T msg) {
        // Default
        return "#";
    }

    @Autowired
    public void setRabbitClient(RabbitClient rabbitClient) {
        this.rabbitClient = rabbitClient;
        // after got rabbit client, init it
        init();
    }
}