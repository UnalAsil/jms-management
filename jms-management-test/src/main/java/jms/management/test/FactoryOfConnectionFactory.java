package jms.management.test;


import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.ConnectionFactory;

public class FactoryOfConnectionFactory {

    private FactoryOfConnectionFactory() {
    }

    /**
     * Returns a {@link ConnectionFactory} with given url.
     *
     * @param url URL of broker.
     * @return {@link ConnectionFactory} with given url
     */
    public static ConnectionFactory getConnectionFactory(String url) {
        //TODO->Dependency injection or jndi could be used for further broker implementations.
        return new ActiveMQConnectionFactory(url);
    }
}
