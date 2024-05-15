package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtobufRpcEngineCallback2;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.internal.ShadedProtobufHelper;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;


import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

public final class AsyncRpcProtocolPBUtil {
  public static final Logger LOG = LoggerFactory.getLogger(AsyncRpcProtocolPBUtil.class);
  public static final ThreadLocal<CompletableFuture<Object>> completableFutureThreadLocal
      = new ThreadLocal<>();

  private AsyncRpcProtocolPBUtil() {}

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
    // transfer originCall & callerContext to worker threads of executor.
    final Server.Call originCall = Server.getCurCall().get();
    final CallerContext originContext = CallerContext.getCurrent();

    CompletableFuture<Object> resCompletableFuture = completableFuture.thenApplyAsync(t -> {
      try {
        Server.getCurCall().set(originCall);
        CallerContext.setCurrent(originContext);
        return response.response();
      }catch (Exception e) {
        throw new CompletionException(e);
      }
    }, RouterRpcServer.getExecutor());
    setThreadLocal(resCompletableFuture);
  }

  public static <T> void asyncRouterServer(ServerReq<T> req, ServerRes<T> res) {
    final ProtobufRpcEngineCallback2 callback =
        ProtobufRpcEngine2.Server.registerForDeferredResponse2();
    CompletableFuture<Object> completableFuture =
        CompletableFuture.completedFuture(null);
    // transfer originCall & callerContext to worker threads of executor.
    final Server.Call originCall = Server.getCurCall().get();
    final CallerContext originContext = CallerContext.getCurrent();
    completableFuture.thenComposeAsync(o -> {
      Server.getCurCall().set(originCall);
      CallerContext.setCurrent(originContext);
      try {
        req.req();
        return (CompletableFuture<T>)RouterAsyncRpcUtil.getCompletableFuture();
//        return CompletableFuture.completedFuture(req.req());
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    }).handle((result, e) -> {
      LOG.info("zjtest async response by [{}], callback: {}, CallerContext: {}, result: [{}]",
          Thread.currentThread().getName(), callback, originContext, result);
      if (e == null) {
        Message value = null;
        try {
          value = res.res(result);
        } catch (RuntimeException re) {
          callback.error(re);
          return null;
        }
        callback.setResponse(value);
      } else {
        callback.error(e.getCause());
      }
      return null;
    });
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

  @FunctionalInterface
  interface ServerReq<T> {
    T req() throws Exception;
  }

  @FunctionalInterface
  interface ServerRes<T> {
    Message res(T result) throws RuntimeException;
  }
}
