package jms.management.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;


public class Producer {

    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) throws JMSException, InterruptedException {

        ConnectionFactory conFactory = FactoryOfConnectionFactory.getConnectionFactory("tcp://localhost:61616");

        for (int i = 0; i < 100; i++) {
            Thread.sleep(1000);
            try (Connection connection = conFactory.createConnection()) {
                connection.start();

                connection.setExceptionListener(exception -> System.out.println("Error while pushing message" + exception));

                Session session = connection.createSession(false,
                        Session.AUTO_ACKNOWLEDGE);

                Destination destination = session.createQueue("example.MyQueue");

                MessageProducer producer = session.createProducer(destination);

                TextMessage message = session.createTextMessage("Hello World!");
                message.setJMSMessageID("exampleMessageId");

                producer.send(message);
                LOG.info("Sent message: {}", message.getText());
                session.close();
            }
        }
    }


}
