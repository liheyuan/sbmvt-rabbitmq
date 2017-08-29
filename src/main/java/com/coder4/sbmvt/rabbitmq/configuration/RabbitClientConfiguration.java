/**
 * @(#)RabbitClientConfiguration.java, Aug 29, 2017.
 * <p>
 * Copyright 2017 fenbi.com. All rights reserved.
 * FENBI.COM PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.coder4.sbmvt.rabbitmq.configuration;

import com.coder4.sbmvt.rabbitmq.RabbitClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author coder4
 */
@Configuration
@ConditionalOnProperty("rabbitmq.host")
public class RabbitClientConfiguration {

    @Value("${rabbitmq.host}")
    private String host;

    @Value("${rabbitmq.port}")
    private int port;

    @Value("${rabbitmq.username}")
    private String username;

    @Value("${rabbitmq.password}")
    private String password;

    @Value("${rabbitmq.vhost}")
    private String vhost;

    @Bean
    public RabbitClient rabbitClient() {
        RabbitClient rabbitClient = new RabbitClient();
        rabbitClient.setHost(host);
        rabbitClient.setPort(port);
        rabbitClient.setUsername(username);
        rabbitClient.setPassword(password);
        rabbitClient.setVhost(vhost);
        rabbitClient.init();
        return rabbitClient;
    }

}