package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RouterAsyncCacheAdmin extends RouterCacheAdmin{
  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;
  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private final ActiveNamenodeResolver namenodeResolver;
  public RouterAsyncCacheAdmin(RouterRpcServer server) {
    super(server);
    this.rpcServer = server;
    this.rpcClient = this.rpcServer.getRPCClient();
    this.namenodeResolver = this.rpcClient.getNamenodeResolver();
  }

  public long addCacheDirective(CacheDirectiveInfo path,
                                EnumSet<CacheFlag> flags) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE, true);
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(path.getPath().toString(), true, false);
    RemoteMethod method = new RemoteMethod("addCacheDirective",
        new Class<?>[] {CacheDirectiveInfo.class, EnumSet.class},
        new RemoteParam(getRemoteMap(path, locations)), flags);
    rpcClient.invokeConcurrent(locations, method, false, false, long.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<RemoteLocation, Long> response =
          (Map<RemoteLocation, Long>) o;
      return response.values().iterator().next();
    });
    setCurCompletableFuture(completableFuture);
    return (long) getResult();
  }

  public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
      long prevId,
      CacheDirectiveInfo filter) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, true);
    CompletableFuture<Object> completableFuture = null;
    if (filter.getPath() != null) {
      final List<RemoteLocation> locations = rpcServer
          .getLocationsForPath(filter.getPath().toString(), true, false);
      RemoteMethod method = new RemoteMethod("listCacheDirectives",
          new Class<?>[] {long.class, CacheDirectiveInfo.class}, prevId,
          new RemoteParam(getRemoteMap(filter, locations)));
      rpcClient.invokeConcurrent(
          locations, method, false, false, BatchedRemoteIterator.BatchedEntries.class);
      completableFuture = getCompletableFuture();
      completableFuture = completableFuture.thenApply(o -> {
        Map<RemoteLocation, BatchedRemoteIterator.BatchedEntries> response =
            (Map<RemoteLocation, BatchedRemoteIterator.BatchedEntries>) o;
        return response.values().iterator().next();
      });
      setCurCompletableFuture(completableFuture);
      return (BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry>) getResult();
    }
    RemoteMethod method = new RemoteMethod("listCacheDirectives",
        new Class<?>[] {long.class, CacheDirectiveInfo.class}, prevId,
        filter);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient
        .invokeConcurrent(nss, method, true, false, BatchedRemoteIterator.BatchedEntries.class);
    completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, BatchedRemoteIterator.BatchedEntries> results =
          (Map<FederationNamespaceInfo, BatchedRemoteIterator.BatchedEntries>) o;
      return results.values().iterator().next();
    });
    setCurCompletableFuture(completableFuture);
    return (BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry>) getResult();
  }

  public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ, true);
    RemoteMethod method = new RemoteMethod("listCachePools",
        new Class<?>[] {String.class}, prevKey);
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient
        .invokeConcurrent(nss, method, true, false, BatchedRemoteIterator.BatchedEntries.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, BatchedRemoteIterator.BatchedEntries> results =
          (Map<FederationNamespaceInfo, BatchedRemoteIterator.BatchedEntries>) o;
      return results.values().iterator().next();
    });
    setCurCompletableFuture(completableFuture);
    return (BatchedRemoteIterator.BatchedEntries<CachePoolEntry>) getResult();
  }

  private static CompletableFuture<Object> getCompletableFuture() {
    return RouterAsyncRpcClient.CUR_COMPLETABLE_FUTURE.get();
  }

  private static void setCurCompletableFuture(
      CompletableFuture<Object> completableFuture) {
    RouterAsyncRpcClient.CUR_COMPLETABLE_FUTURE.set(completableFuture);
  }

  // todo : only test
  public Object getResult() throws IOException {
    try {
      CompletableFuture<Object> completableFuture = RouterAsyncRpcClient.CUR_COMPLETABLE_FUTURE.get();
      System.out.println("zjcom3: " + completableFuture);
      Object o =  completableFuture.get();
      return o;
    } catch (InterruptedException e) {
    } catch (ExecutionException e) {
      IOException ioe = (IOException) e.getCause();
      throw ioe;
    }
    return null;
  }
}
