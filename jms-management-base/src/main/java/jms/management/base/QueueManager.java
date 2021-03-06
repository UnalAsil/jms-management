package jms.management.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
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

    /**
     * Updates the message in the given queue.
     *
     * @param queueName       Name of queue.
     * @param message         Jms message.
     * @param messageSelector Message selector Used to find the message to update.
     * @param <T>             The type of message to be updated.
     * @param timeout         timeout
     * @param unit            Time unit
     * @return Updated message.
     * @throws JMSException         If any failure occurred in JMS provider.
     * @throws InterruptedException if interrupted while waiting.
     */
    public <T extends Serializable> Optional<T> update(String queueName, T message, String messageSelector,
                                                       long timeout, TimeUnit unit) throws JMSException,
            InterruptedException {
        Optional<T> removedMessage = remove(queueName, messageSelector, timeout, unit);

        if (removedMessage.isPresent()) {
            try (Connection connection = connectionFactory.createConnection()) {
                connection.start();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(queueName);
                ObjectMessage objectMessage = session.createObjectMessage(message);
                MessageProducer producer = session.createProducer(destination);
                producer.send(objectMessage);
                producer.close();
                session.close();
                return removedMessage;
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * Suspends the message in the given resource queue. Pushes the message to the queue given as the target.
     *
     * @param sourceQueueName Queue containing the message to be suspended.
     * @param targetQueueName The queue where the message to be suspended will be stored.
     * @param message         Message to be suspended
     * @param messageSelector Message selector
     * @param timeout         timeout
     * @param unit            time unit
     * @param <T>             Type of the message
     * @return Suspended message.
     * @throws JMSException         If any failure occurred in JMS provider.
     * @throws InterruptedException If  interrupted while waiting.
     */
    public <T extends Serializable> Optional<T> suspend(String sourceQueueName, String targetQueueName, T message,
                                                        String messageSelector, long timeout, TimeUnit unit) throws JMSException, InterruptedException {

        Optional<T> removedMessage = remove(sourceQueueName, messageSelector, timeout, unit);

        if (removedMessage.isPresent()) {
            try (Connection connection = connectionFactory.createConnection()) {
                connection.start();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(targetQueueName);
                ObjectMessage objectMessage = session.createObjectMessage(message);
                MessageProducer producer = session.createProducer(destination);
                producer.send(objectMessage);
                producer.close();
                session.close();
                return removedMessage;
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * Resumes the message in the given suspended queue. Pushes the message to the queue given as the target.
     *
     * @param targetQueueName    The queue where the message to be resumed will be pushed.
     * @param suspendedQueueName Queue containing the message to be resumed.
     * @param message            Message to be resumed
     * @param messageSelector    Message selector
     * @param timeout            timeout
     * @param unit               time unit
     * @param <T>                Type of the message
     * @return Resumed message.
     * @throws JMSException         If any failure occurred in JMS provider.
     * @throws InterruptedException If  interrupted while waiting.
     */
    public <T extends Serializable> Optional<T> resume(String targetQueueName, String suspendedQueueName,
                                                       T message, String messageSelector, long timeout,
                                                       TimeUnit unit) throws JMSException, InterruptedException {
        return suspend(suspendedQueueName, targetQueueName, message, messageSelector, timeout, unit);
    }

}
