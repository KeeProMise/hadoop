package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;

public class RouterAsyncRpcClient extends RouterRpcClient{

  /**
   * Create a router RPC client to manage remote procedure calls to NNs.
   *
   * @param conf                 Hdfs Configuration.
   * @param router               A router using this RPC client.
   * @param resolver             A NN resolver to determine the currently active NN in HA.
   * @param monitor              Optional performance monitor.
   * @param routerStateIdContext the router state context object to hold the state ids for all
   *                             namespaces.
   */
  public RouterAsyncRpcClient(
      Configuration conf, Router router, ActiveNamenodeResolver resolver,
      RouterRpcMonitor monitor, RouterStateIdContext routerStateIdContext) {
    super(conf, router, resolver, monitor, routerStateIdContext);
  }


}
