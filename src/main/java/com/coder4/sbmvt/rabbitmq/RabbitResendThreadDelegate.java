/**
 * @(#)RabbitResendThreadDelegate.java, Dec 05, 2017.
 * <p>
 * Copyright 2017 fenbi.com. All rights reserved.
 * FENBI.COM PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.coder4.sbmvt.rabbitmq;

import com.rabbitmq.client.Channel;

/**
 * @author coder4
 */
interface RabbitResendThreadDelegate<T> {

    Channel createChannel() throws Exception;

    void doSend(Channel retryChannel, T msg) throws Exception;

}