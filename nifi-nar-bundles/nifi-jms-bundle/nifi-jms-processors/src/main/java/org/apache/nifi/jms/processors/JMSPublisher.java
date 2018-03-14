/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.jms.processors;

import java.util.Map;
import java.util.Map.Entry;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.nifi.logging.ComponentLog;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.core.SessionCallback;
import org.springframework.jms.support.JmsHeaders;

/**
 * Generic publisher of messages to JMS compliant messaging system.
 */
final class JMSPublisher extends JMSWorker {

    JMSPublisher(CachingConnectionFactory connectionFactory, JmsTemplate jmsTemplate, ComponentLog processLog) {
        super(connectionFactory, jmsTemplate, processLog);
        processLog.debug("Created Message Publisher for {}", new Object[] {jmsTemplate});
    }

    void publish(String destinationName, byte[] messageBytes) {
        this.publish(destinationName, messageBytes, null);
    }

    void publish(final String destinationName, final byte[] messageBytes, final Map<String, String> flowFileAttributes) {
        this.jmsTemplate.send(destinationName, new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                BytesMessage message = session.createBytesMessage();
                message.writeBytes(messageBytes);
                setMessageHeaderAndProperties(message, flowFileAttributes);
                return message;
            }
        });
    }

    void publish(String destinationName, String messageText) {
        this.publish(destinationName, messageText, null);
    }

    void publish(String destinationName, String messageText, final Map<String, String> flowFileAttributes) {
        this.jmsTemplate.send(destinationName, new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                TextMessage message = session.createTextMessage(messageText);
                setMessageHeaderAndProperties(message, flowFileAttributes);
                return message;
            }
        });
    }

    void setMessageHeaderAndProperties(final Message message, final Map<String, String> flowFileAttributes) throws JMSException {
        if (flowFileAttributes != null && !flowFileAttributes.isEmpty()) {
            for (Entry<String, String> entry : flowFileAttributes.entrySet()) {
                try {
                    if (!entry.getKey().startsWith(JmsHeaders.PREFIX) && !entry.getKey().contains("-") && !entry.getKey().contains(".")) {// '-' and '.' are illegal char in JMS prop names
                        message.setStringProperty(entry.getKey(), entry.getValue());
                    } else if (entry.getKey().equals(JmsHeaders.DELIVERY_MODE)) {
                        message.setJMSDeliveryMode(Integer.parseInt(entry.getValue()));
                    } else if (entry.getKey().equals(JmsHeaders.EXPIRATION)) {
                        message.setJMSExpiration(Integer.parseInt(entry.getValue()));
                    } else if (entry.getKey().equals(JmsHeaders.PRIORITY)) {
                        message.setJMSPriority(Integer.parseInt(entry.getValue()));
                    } else if (entry.getKey().equals(JmsHeaders.REDELIVERED)) {
                        message.setJMSRedelivered(Boolean.parseBoolean(entry.getValue()));
                    } else if (entry.getKey().equals(JmsHeaders.TIMESTAMP)) {
                        message.setJMSTimestamp(Long.parseLong(entry.getValue()));
                    } else if (entry.getKey().equals(JmsHeaders.CORRELATION_ID)) {
                        message.setJMSCorrelationID(entry.getValue());
                    } else if (entry.getKey().equals(JmsHeaders.TYPE)) {
                        message.setJMSType(entry.getValue());
                    } else if (entry.getKey().equals(JmsHeaders.REPLY_TO)) {
                        Destination destination = buildDestination(entry.getValue());
                        if (destination != null) {
                            message.setJMSReplyTo(destination);
                        } else {
                            logUnbuildableDestination(entry.getKey(), JmsHeaders.REPLY_TO);
                        }
                    } else if (entry.getKey().equals(JmsHeaders.DESTINATION)) {
                        Destination destination = buildDestination(entry.getValue());
                        if (destination != null) {
                            message.setJMSDestination(destination);
                        } else {
                            logUnbuildableDestination(entry.getKey(), JmsHeaders.DESTINATION);
                        }
                    }
                } catch (NumberFormatException ne) {
                    this.processLog.warn("Incompatible value for attribute " + entry.getKey()
                            + " [" + entry.getValue() + "] is not a number. Ignoring this attribute.");
                }
            }
        }
    }

    private void logUnbuildableDestination(String destinationName, String headerName) {
        this.processLog.warn("Failed to determine destination type from destination name '{}'. The '{}' header will not be set.", new Object[] {destinationName, headerName});
    }


    private Destination buildDestination(final String destinationName) {
        Destination destination;
        if (destinationName.toLowerCase().contains("topic")) {
            destination = this.jmsTemplate.execute(new SessionCallback<Topic>() {
                @Override
                public Topic doInJms(Session session) throws JMSException {
                    return session.createTopic(destinationName);
                }
            });
        } else if (destinationName.toLowerCase().contains("queue")) {
            destination = this.jmsTemplate.execute(new SessionCallback<Queue>() {
                @Override
                public Queue doInJms(Session session) throws JMSException {
                    return session.createQueue(destinationName);
                }
            });
        } else {
            destination = null;
        }

        return destination;
    }
}
