package jms.management.base;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

public interface ListenableFuture<T> extends Future<T> {

    void addListener(Runnable listener, Executor executor);

    void removeListener(Runnable listener);
}
