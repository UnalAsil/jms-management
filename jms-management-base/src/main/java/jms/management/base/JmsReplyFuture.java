package jms.management.base;

import javax.jms.*;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @param <T>
 */
//TODO-> Implementation of multiple message consumption
public class JmsReplyFuture<T extends Serializable> implements ListenableFuture<T>, MessageListener {

    private enum State {WAITING, DONE, CANCELLED}

    private final Connection connection;
    private final Session session;
    private final MessageConsumer replyConsumer;
    private final BlockingQueue<T> reply = new ArrayBlockingQueue<>(1);
    private final Map<Runnable, Executor> listeners = new HashMap<>();
    private volatile State state = State.WAITING;

    private JmsReplyFuture(Connection connection, Session session, MessageConsumer messageConsumer) {
        this.connection = connection;
        this.session = session;
        this.replyConsumer = messageConsumer;
    }

    public static <T extends Serializable> JmsReplyFuture<T> newInstance(ConnectionFactory connectionFactory,
                                                                         String queueName,
                                                                         String messageSelector) throws JMSException {
        Connection connection = connectionFactory.createConnection();
        Session session0 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session0.createQueue(queueName);
        MessageConsumer consumer = session0.createConsumer(destination, messageSelector);
        return new JmsReplyFuture<>(connection, session0, consumer);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        state = State.CANCELLED;
        try {
            cleanUp();
            return true;
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanUp() throws JMSException {
        replyConsumer.close();
        session.close();
        connection.close();
    }

    @Override
    public boolean isCancelled() {
        return state == State.CANCELLED;
    }

    @Override
    public boolean isDone() {
        return state == State.DONE;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return this.reply.take();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        final T replyOrNull = reply.poll(timeout, unit);
        if (replyOrNull == null) {
            throw new TimeoutException();
        }
        return replyOrNull;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        listeners.put(listener, executor);
    }

    public void removeListener(Runnable listener) {
        listeners.remove(listener);
    }

    @Override
    public void onMessage(Message message) {
        try {
            final ObjectMessage objectMessage = (ObjectMessage) message;
            final Serializable object = objectMessage.getObject();
            reply.put((T) object);
            state = State.DONE;
            cleanUp();
            wakeTheListeners();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void wakeTheListeners() {
        for (var entry : listeners.entrySet()) {
            entry.getValue().execute(entry.getKey());
        }
    }
}
