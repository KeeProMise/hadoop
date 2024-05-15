package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public final class RouterAsyncRpcUtil {
  public static final ThreadLocal<CompletableFuture<Object>> CUR_COMPLETABLE_FUTURE
      = new ThreadLocal<>();
  private static final Boolean BOOLEAN_RESULT = false;
  private static final Long LONG_RESULT = -1l;
  private static final Object NULL_RESULT = null;

  public static  <T> CompletableFuture<T> asyncInvoke(
      Invoker<T> invoker) throws IOException {
    invoker.invoke();
    return (CompletableFuture<T>) CUR_COMPLETABLE_FUTURE.get();
  }

  public static CompletableFuture<Object> getCompletableFuture() {
    return CUR_COMPLETABLE_FUTURE.get();
  }

  public static void setCurCompletableFuture(
      CompletableFuture<Object> completableFuture) {
    CUR_COMPLETABLE_FUTURE.set(completableFuture);
  }

  // todo : only test
  public static Object getResult() throws IOException {
    try {
      CompletableFuture<Object> completableFuture = CUR_COMPLETABLE_FUTURE.get();
      Object o =  completableFuture.get();
      return o;
    } catch (InterruptedException e) {
    } catch (ExecutionException e) {
      IOException ioe = (IOException) e.getCause();
      throw ioe;
    }
    return null;
  }

  public static <T> T asyncReturn(Class<T> clazz) {
    if (clazz == null) {
      return null;
    }
    if (clazz.equals(Boolean.class)) {
      return (T) BOOLEAN_RESULT;
    } else if (clazz.equals(Long.class)) {
      return (T) LONG_RESULT;
    }
    return (T) NULL_RESULT;
  }

  public interface Invoker<T> {
    T invoke() throws IOException;
  }
}
