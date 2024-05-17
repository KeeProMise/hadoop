package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class AsyncQuota extends Quota{
  public AsyncQuota(Router router, RouterRpcServer server) {
    super(router, server);
  }

  /**
   * Get aggregated quota usage for the federation path.
   * @param path Federation path.
   * @return Aggregated quota.
   * @throws IOException If the quota system is disabled.
   */
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    getEachQuotaUsage(path);
    CompletableFuture<Object> completableFuture =
        RouterAsyncRpcUtil.getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<RemoteLocation, QuotaUsage> results = (Map<RemoteLocation, QuotaUsage>) o;
      try {
        return AsyncQuota.super.aggregateQuota(path, results);
      } catch (IOException e) {
        throw new CompletionException(e);
      }
    });
    RouterAsyncRpcUtil.setCurCompletableFuture(completableFuture);
    return RouterAsyncRpcUtil.asyncReturn(QuotaUsage.class);
  }
}
