package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class AsyncQuota extends Quota{
  public AsyncQuota(Router router, RouterRpcServer server) {
    super(router, server);
  }

  @Override
  QuotaUsage aggregateQuota(
      String path, Map<RemoteLocation, QuotaUsage> results) throws IOException {
    QuotaUsage quotaUsage = super.aggregateQuota(path, results);
    RouterAsyncRpcUtil.setCurCompletableFuture(CompletableFuture.completedFuture(quotaUsage));
    return null;
  }
}
