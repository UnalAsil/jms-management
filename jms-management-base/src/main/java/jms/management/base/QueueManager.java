package jms.management.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.*;

public class QueueManager {

    private static final Logger LOG = LoggerFactory.getLogger(QueueManager.class);

    private final ConnectionFactory connectionFactory;

    public QueueManager(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * Removes the message in the given queue.
     *
     * @param queueName       Name of queue.
     * @param messageSelector Message selector.
     * @param <T>             The type of message to be deleted.
     * @return removed message.
     * @throws JMSException         If any failure occurred in JMS provider.
     * @throws InterruptedException if interrupted while waiting.
     */
    public <T extends Serializable> Optional<T> remove(String queueName, String messageSelector, long timeout,
                                                       TimeUnit unit) throws JMSException, InterruptedException {
        JmsReplyFuture<T> future = JmsReplyFuture.newInstance(connectionFactory, queueName, messageSelector);
        try {
            return Optional.ofNullable(future.get(timeout, unit));
        } catch (ExecutionException | TimeoutException e) {
            LOG.error("Error occurred during remove operation", e);
            return Optional.empty();
        }
    }

    /**
     * Removes the message in the given queue. Registers a listener to be run on the given executor. When message
     * removed listener will be run.
     *
     * @param queueName       Name of queue.
     * @param messageSelector Message selector.
     * @param listener        listener
     * @param executor        Executor
     * @param <T>             The type of message to be deleted.
     * @return {@link Future<T>}
     * @throws JMSException If any failure occurred in JMS provider.
     */
    public <T extends Serializable> Future<T> remove(String queueName, String messageSelector,
                                                     Runnable listener,
                                                     Executor executor) throws JMSException {
        return JmsReplyFuture.newInstance(connectionFactory, queueName, messageSelector, listener
                , executor);
    }

}
