package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;

public class RouterAsyncStoragePolicy extends RouterStoragePolicy{
  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;

  public RouterAsyncStoragePolicy(RouterRpcServer server) {
    super(server);
    this.rpcServer = server;
    this.rpcClient = this.rpcServer.getRPCClient();
  }

  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getStoragePolicies");
    return rpcServer.invokeAtAvailableNsAsync(method, BlockStoragePolicy[].class);
  }
}
