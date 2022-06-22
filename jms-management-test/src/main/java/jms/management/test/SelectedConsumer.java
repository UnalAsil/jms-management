package jms.management.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class SelectedConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(SelectedConsumer.class);

    public static void main(String[] args) throws JMSException, InterruptedException {

        ConnectionFactory factory = FactoryOfConnectionFactory.getConnectionFactory("tcp://localhost:61616");

        try (Connection conn = factory.createConnection()) {
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("example.MyQueue");
            MessageConsumer consumer = session.createConsumer(destination, "JMSMessageID=exampleMessageId");
            consumer.setMessageListener(message -> {
                if (message instanceof TextMessage textMessage) {
                    try {
                        LOG.info("Received message : {}", textMessage.getText());
                    } catch (JMSException e) {
                        LOG.error("Error while getting text from message: {}", textMessage);
                    }
                }
            });
            conn.start();
            Thread.sleep(1000000);
        }
    }

}
