package jms.management.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) throws JMSException, InterruptedException {

        ConnectionFactory connectionFactory = FactoryOfConnectionFactory.getConnectionFactory("tcp://localhost:61616");
        for (int i = 0; i < 100; i++) {
            Thread.sleep(1000);
            try (Connection connection = connectionFactory.createConnection()) {
                connection.start();

                connection.setExceptionListener(exception -> LOG.error("JMS Exception occurred.  Shutting " +
                        "down client.", exception));

                Session session = connection.createSession(false,
                        Session.AUTO_ACKNOWLEDGE);

                Destination destination = session.createQueue("example.MyQueue");

                MessageConsumer consumer = session.createConsumer(destination);

                Message message = consumer.receive(1000);

                if (message instanceof TextMessage textMessage) {
                    LOG.info("Received message : {}", textMessage.getText());
                }
                consumer.close();
                session.close();
            }
        }

    }

}
