package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.asyncReturn;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.getCompletableFuture;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.setCurCompletableFuture;

public class RouterAsyncSnapshot extends RouterSnapshot{
  public RouterAsyncSnapshot(RouterRpcServer server) {
    super(server);
  }

  public String createSnapshot(String snapshotRoot, String snapshotName)
      throws IOException {
    RouterRpcClient rpcClient = getRpcClient();
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod method = new RemoteMethod("createSnapshot",
        new Class<?>[] {String.class, String.class}, new RemoteParam(),
        snapshotName);

    CompletableFuture<Object> completableFuture = null;
    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      rpcClient.invokeConcurrent(
          locations, method, String.class);
      completableFuture = getCompletableFuture();
      completableFuture = completableFuture.thenApply(o -> {
        Map<RemoteLocation, String> results =
            (Map<RemoteLocation, String>) o;
        Map.Entry<RemoteLocation, String> firstelement =
            results.entrySet().iterator().next();
        RemoteLocation loc = firstelement.getKey();
        String result = firstelement.getValue();
        result = result.replaceFirst(loc.getDest(), loc.getSrc());
        return result;
      });
    } else {
      rpcClient.invokeSequential(
          method, locations, String.class, null);
      completableFuture = getCompletableFuture();
      completableFuture = completableFuture.thenApply(o -> {
        RemoteResult<RemoteLocation, String> response =
            (RemoteResult<RemoteLocation, String>) o;
        RemoteLocation loc = response.getLocation();
        String invokedResult = response.getResult();
        return invokedResult.replaceFirst(loc.getDest(), loc.getSrc());
      });
    }
    setCurCompletableFuture(completableFuture);
    return asyncReturn(String.class);
  }

  public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
      throws IOException {
    RouterRpcClient rpcClient = getRpcClient();
    RouterRpcServer rpcServer = getRpcServer();
    ActiveNamenodeResolver namenodeResolver = getNamenodeResolver();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getSnapshottableDirListing");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(
            nss, method, true, false, SnapshottableDirectoryStatus[].class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, SnapshottableDirectoryStatus[]> ret1 =
          (Map<FederationNamespaceInfo, SnapshottableDirectoryStatus[]>) o;
      return RouterRpcServer.merge(ret1, SnapshottableDirectoryStatus.class);
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(SnapshottableDirectoryStatus[].class);
  }

  public SnapshotStatus[] getSnapshotListing(String snapshotRoot)
      throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod remoteMethod = new RemoteMethod("getSnapshotListing",
        new Class<?>[]{String.class},
        new RemoteParam());

    CompletableFuture<Object> completableFuture = null;
    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      Map<RemoteLocation, SnapshotStatus[]> ret = rpcClient.invokeConcurrent(
          locations, remoteMethod, true, false, SnapshotStatus[].class);
      completableFuture = getCompletableFuture();
      completableFuture = completableFuture.thenApply(o -> {
        Map<RemoteLocation, SnapshotStatus[]> ret1 =
            (Map<RemoteLocation, SnapshotStatus[]>) o;
        SnapshotStatus[] response = ret1.values().iterator().next();
        String src = ret1.keySet().iterator().next().getSrc();
        String dst = ret1.keySet().iterator().next().getDest();
        for (SnapshotStatus s : response) {
          String mountPath = DFSUtil.bytes2String(s.getParentFullPath()).
              replaceFirst(src, dst);
          s.setParentFullPath(DFSUtil.string2Bytes(mountPath));
        }
        return response;
      });
    } else {
      rpcClient.invokeSequential(remoteMethod, locations,
          SnapshotStatus[].class, null);
      completableFuture = getCompletableFuture();
      completableFuture = completableFuture.thenApply(o -> {
        RemoteResult<RemoteLocation, SnapshotStatus[]> invokedResponse =
            (RemoteResult<RemoteLocation, SnapshotStatus[]>) o;
        RemoteLocation loc = invokedResponse.getLocation();
        SnapshotStatus[] response = invokedResponse.getResult();
        for (SnapshotStatus s : response) {
          String mountPath = DFSUtil.bytes2String(s.getParentFullPath()).
              replaceFirst(loc.getDest(), loc.getSrc());
          s.setParentFullPath(DFSUtil.string2Bytes(mountPath));
        }
        return response;
      });
    }
    setCurCompletableFuture(completableFuture);
    return asyncReturn(SnapshotStatus[].class);
  }


  public SnapshotDiffReport getSnapshotDiffReport(
      String snapshotRoot,
      String earlierSnapshotName, String laterSnapshotName) throws IOException {
    RouterRpcClient rpcClient = getRpcClient();
    RouterRpcServer rpcServer = getRpcServer();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod remoteMethod = new RemoteMethod("getSnapshotDiffReport",
        new Class<?>[] {String.class, String.class, String.class},
        new RemoteParam(), earlierSnapshotName, laterSnapshotName);

    CompletableFuture<Object> completableFuture = null;
    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      rpcClient.invokeConcurrent(
          locations, remoteMethod, true, false, SnapshotDiffReport.class);
      completableFuture = getCompletableFuture();
      completableFuture = completableFuture.thenApply(o -> {
        Map<RemoteLocation, SnapshotDiffReport> ret =
            (Map<RemoteLocation, SnapshotDiffReport>) o;
        return ret.values().iterator().next();
      });
    } else {
      rpcClient.invokeSequential(
          locations, remoteMethod, SnapshotDiffReport.class, null);
      completableFuture = getCompletableFuture();
    }
    setCurCompletableFuture(completableFuture);
    return asyncReturn(SnapshotDiffReport.class);
  }

  public SnapshotDiffReportListing getSnapshotDiffReportListing(
      String snapshotRoot, String earlierSnapshotName, String laterSnapshotName,
      byte[] startPath, int index) throws IOException {
    RouterRpcServer rpcServer = getRpcServer();
    RouterRpcClient rpcClient = getRpcClient();
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    Class<?>[] params = new Class<?>[] {
        String.class, String.class, String.class,
        byte[].class, int.class};
    RemoteMethod remoteMethod = new RemoteMethod(
        "getSnapshotDiffReportListing", params,
        new RemoteParam(), earlierSnapshotName, laterSnapshotName,
        startPath, index);

    CompletableFuture<Object> completableFuture = null;
    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      rpcClient.invokeConcurrent(locations, remoteMethod, false, false,
              SnapshotDiffReportListing.class);
      completableFuture = getCompletableFuture();
      completableFuture = completableFuture.thenApply(o -> {
        Map<RemoteLocation, SnapshotDiffReportListing> ret =
            (Map<RemoteLocation, SnapshotDiffReportListing>) o;
        Collection<SnapshotDiffReportListing> listings = ret.values();
        SnapshotDiffReportListing listing0 = listings.iterator().next();
        return listing0;
      });
    } else {
      rpcClient.invokeSequential(
          locations, remoteMethod, SnapshotDiffReportListing.class, null);
      completableFuture = getCompletableFuture();
    }
    setCurCompletableFuture(completableFuture);
    return asyncReturn(SnapshotDiffReportListing.class);
  }
}
