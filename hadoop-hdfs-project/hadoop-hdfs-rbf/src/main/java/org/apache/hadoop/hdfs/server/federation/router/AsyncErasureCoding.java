package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.asyncRequestThenApply;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.asyncReturn;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.getCompletableFuture;
import static org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil.setCurCompletableFuture;

import static org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer.merge;

public class AsyncErasureCoding extends ErasureCoding{
  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;
  /** Interface to identify the active NN for a nameservice or blockpool ID. */
  private final ActiveNamenodeResolver namenodeResolver;

  public AsyncErasureCoding(RouterRpcServer server) {
    super(server);
    this.rpcServer = server;
    this.rpcClient =  this.rpcServer.getRPCClient();
    this.namenodeResolver = this.rpcClient.getNamenodeResolver();
  }

  public ErasureCodingPolicyInfo[] getErasureCodingPolicies()
      throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getErasureCodingPolicies");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(
            nss, method, true, false, ErasureCodingPolicyInfo[].class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, ErasureCodingPolicyInfo[]> ret =
          (Map<FederationNamespaceInfo, ErasureCodingPolicyInfo[]>) o;
      return merge(ret, ErasureCodingPolicyInfo.class);
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(ErasureCodingPolicyInfo[].class);
  }

  public Map getErasureCodingCodecs() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getErasureCodingCodecs");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(
            nss, method, true, false, Map.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, Map> retCodecs =
          (Map<FederationNamespaceInfo, Map>) o;
      Map<String, String> ret = new HashMap<>();
      Object obj = retCodecs;
      @SuppressWarnings("unchecked")
      Map<FederationNamespaceInfo, Map<String, String>> results =
          (Map<FederationNamespaceInfo, Map<String, String>>)obj;
      Collection<Map<String, String>> allCodecs = results.values();
      for (Map<String, String> codecs : allCodecs) {
        ret.putAll(codecs);
      }

      return ret;
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(Map.class);
  }

  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    RemoteMethod method = new RemoteMethod("addErasureCodingPolicies",
        new Class<?>[] {ErasureCodingPolicy[].class}, new Object[] {policies});
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(
            nss, method, true, false, AddErasureCodingPolicyResponse[].class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, AddErasureCodingPolicyResponse[]> ret =
          (Map<FederationNamespaceInfo, AddErasureCodingPolicyResponse[]>) o;
      return merge(ret, AddErasureCodingPolicyResponse.class);
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(AddErasureCodingPolicyResponse[].class);
  }

  public ECTopologyVerifierResult getECTopologyResultForPolicies(
      String[] policyNames) throws IOException {
    RemoteMethod method = new RemoteMethod("getECTopologyResultForPolicies",
        new Class<?>[] {String[].class}, new Object[] {policyNames});
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    if (nss.isEmpty()) {
      throw new IOException("No namespace availaible.");
    }
    rpcClient.invokeConcurrent(nss, method, true, false,
            ECTopologyVerifierResult.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, ECTopologyVerifierResult> ret =
          (Map<FederationNamespaceInfo, ECTopologyVerifierResult>) o;
      for (Map.Entry<FederationNamespaceInfo, ECTopologyVerifierResult> entry : ret
          .entrySet()) {
        if (!entry.getValue().isSupported()) {
          return entry.getValue();
        }
      }
      // If no negative result, return the result from the first namespace.
      return ret.get(nss.iterator().next());
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(ECTopologyVerifierResult.class);
  }

  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getECBlockGroupStats");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    Map<FederationNamespaceInfo, ECBlockGroupStats> allStats =
        rpcClient.invokeConcurrent(
            nss, method, true, false, ECBlockGroupStats.class);
    CompletableFuture<Object> completableFuture = getCompletableFuture();
    completableFuture = completableFuture.thenApply(o -> {
      Map<FederationNamespaceInfo, ECBlockGroupStats> allStats1 =
          (Map<FederationNamespaceInfo, ECBlockGroupStats>) o;
      return ECBlockGroupStats.merge(allStats1.values());
    });
    setCurCompletableFuture(completableFuture);
    return asyncReturn(ECBlockGroupStats.class);
  }
}
