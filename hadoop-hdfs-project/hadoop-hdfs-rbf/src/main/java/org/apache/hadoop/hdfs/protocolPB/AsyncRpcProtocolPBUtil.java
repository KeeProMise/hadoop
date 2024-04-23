package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.internal.ShadedProtobufHelper;
import org.apache.hadoop.util.concurrent.AsyncGet;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

public class AsyncRpcProtocolPBUtil {
  public static final ThreadLocal<CompletableFuture<Object>> completableFutureThreadLocal
      = new ThreadLocal<>();

  public static  <T> AsyncGet<T, Exception> asyncIpc(
      ShadedProtobufHelper.IpcCall<T> call) throws IOException {
    CompletableFuture<Object> completableFuture = new CompletableFuture<>();
    Client.COMPLETABLE_FUTURE_THREAD_LOCAL.set(completableFuture);
    ipc(call);
    return (AsyncGet<T, Exception>) ProtobufRpcEngine2.getAsyncReturnMessage();
  }

  public static <T> void asyncResponse(Response<T> response) {
    CompletableFuture<T> completableFuture =
        (CompletableFuture<T>) Client.COMPLETABLE_FUTURE_THREAD_LOCAL.get();

    CompletableFuture<Object> resCompletableFuture = completableFuture.thenApplyAsync(t -> {
      try {
        return response.response();
      }catch (Exception e) {
        throw new CompletionException(e);
      }
    }, RouterRpcServer.getExecutor());
    setThreadLocal(resCompletableFuture);
  }

  public static void setThreadLocal(CompletableFuture<Object> completableFuture) {
    completableFutureThreadLocal.set(completableFuture);
  }

  public static CompletableFuture<Object> getCompletableFuture() {
    return completableFutureThreadLocal.get();
  }

  @FunctionalInterface
   interface Response<T> {
    T response() throws Exception;
  }
}
