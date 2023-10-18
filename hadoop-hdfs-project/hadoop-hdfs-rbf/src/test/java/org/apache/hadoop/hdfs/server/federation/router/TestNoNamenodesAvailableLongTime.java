package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Test;


import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE;
import static org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.transitionClusterNSToStandby;;
import static org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * When failover occurs, the router may record that the ns has no active namenode.
 * Only when the router updates the cache next time can the memory status be updated,
 * causing the router to report NoNamenodesAvailableException for a long time.
 */
public class TestNoNamenodesAvailableLongTime {


  private StateStoreDFSCluster cluster;

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void setupCluster(int numNameservices, int numberOfObserver, boolean useObserver)
      throws Exception {
    if (!useObserver) {
      numberOfObserver = 0;
    }
    int numberOfNamenode = 2 + numberOfObserver;
    cluster = new StateStoreDFSCluster(true, numNameservices, numberOfNamenode,
        DEFAULT_HEARTBEAT_INTERVAL_MS, 10000);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .metrics()
        .admin()
        .rpc()
        .heartbeat()
        .build();

    if (useObserver) {
      routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY, true);
      routerConf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
      routerConf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");
    }

    // Reduce the number of RPC clients threads to overload the Router easy
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE, 4);
    // Overload control
    routerConf.setBoolean(
        RBFConfigKeys.DFS_ROUTER_CLIENT_REJECT_OVERLOAD, false);

    // No need for datanodes as we use renewLease() for testing
    cluster.setNumDatanodesPerNameservice(0);
    cluster.addRouterOverrides(routerConf);


    cluster.startCluster();
    // Making one Namenode active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        List<MiniRouterDFSCluster.NamenodeContext>  nnList = cluster.getNamenodes(ns);
        cluster.switchToActive(ns, nnList.get(0).getNamenodeId());
        cluster.switchToStandby(ns, nnList.get(1).getNamenodeId());
        for (int i = 2; i < numberOfNamenode; i++) {
          cluster.switchToObserver(ns, nnList.get(i).getNamenodeId());
        }
      }
    }

    cluster.startRouters();
    cluster.waitClusterUp();
  }

  /**
   *
   */
  @Test
  public void testCacheShouldNotBeRotated() throws Exception {
    setupCluster(1, 0, false);
    transitionClusterNSToStandby(cluster);
    allRoutersHeartbeatAndLoadCache();
    // Record the time after the router first updated the cache
    long firstLoadTime = Time.now();
    List<MiniRouterDFSCluster.NamenodeContext> namenodes = cluster.getNamenodes();

    // Make sure all namenodes are in standby state
    for (MiniRouterDFSCluster.NamenodeContext namenodeContext : namenodes) {
      assertEquals(STANDBY.ordinal(), namenodeContext.getNamenode().getNameNodeState());
    }

    Configuration conf = cluster.getRouterClientConf();
    // Set dfs.client.failover.random.order false, to pick 1st router at first
    conf.setBoolean("dfs.client.failover.random.order", false);

    // Retries is 2 (see FailoverOnNetworkExceptionRetry#shouldRetry, will fail
    // when reties > max.attempts), so total access is 3.
    conf.setInt("dfs.client.retry.max.attempts", 1);
    DFSClient routerClient = new DFSClient(new URI("hdfs://fed"), conf);

    // Get the second namenode in the router cache and make it active
    setSecondNonObserverNamenodeInTheRouterCacheActive(0, false);
    allRoutersHeartbeatNotLoadCache(false);

    RouterContext routerContext = cluster.getRouters().get(0);
    // Get router0 metrics
    FederationRPCMetrics rpcMetrics0 = routerContext.getRouter().getRpcServer().getRPCMetrics();
    // Original failures
    long originalRouter0NoNamenodesFailures = rpcMetrics0.getProxyOpNoNamenodes();

    // At this time, the router has recorded 2 standby namenodes in memory.
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", false));

    /*
     * The first accessed namenode is indeed standby,
     * then an NoNamenodesAvailableException will be reported for the first access,
     * and the next access will be successful.
     */
    routerClient.create("/test.txt", true);
    assertEquals(originalRouter0NoNamenodesFailures + 1, rpcMetrics0.getProxyOpNoNamenodes());
    originalRouter0NoNamenodesFailures = rpcMetrics0.getProxyOpNoNamenodes();

    // At this time, the router has recorded 2 standby namenodes in memory.
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", false));

    /*
     * we have put the actually active namenode at the front of the cache by rotating the cache.
     * Therefore, the access does not cause NoNamenodesAvailableException.
     */
    routerClient.setPermission("/test.txt", FsPermission.createImmutable((short)0640));
    assertEquals(originalRouter0NoNamenodesFailures, rpcMetrics0.getProxyOpNoNamenodes());

    // At this time, the router has recorded 2 standby namenodes in memory
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", false));

    /*
     * If the router sends an illegal operation to active nn,
     * NoNamenodesAvailableException will still be reported at this time,
     * and the cache should not be rotated due to illegal operations.
     *
     */
    List<AclEntry> aclSpec = Lists.newArrayList(aclEntry(DEFAULT, USER, "foo", ALL));
    try {
      routerClient.setAcl("/test.txt", aclSpec);
    }catch (RemoteException e) {
      assertTrue(e.getMessage().contains(
          "org.apache.hadoop.hdfs.server.federation.router.NoNamenodesAvailableException: " +
          "No namenodes available under nameservice ns0"));
      assertTrue(e.getMessage().contains(
          "org.apache.hadoop.hdfs.protocol.AclException: Invalid ACL: " +
          "only directories may have a default ACL. Path: /test.txt"));
    }
    assertEquals(originalRouter0NoNamenodesFailures + 3, rpcMetrics0.getProxyOpNoNamenodes());
    originalRouter0NoNamenodesFailures = rpcMetrics0.getProxyOpNoNamenodes();

    // So legal operations can be accessed normally without reporting NoNamenodesAvailableException.
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", false));
    routerClient.getFileInfo("/");
    assertEquals(originalRouter0NoNamenodesFailures, rpcMetrics0.getProxyOpNoNamenodes());

    /*
     * Access the active namenode without waiting for the router to update the cache,
     * even if there are 2 standby states recorded in the router memory.
     */
    long endTime = Time.now();
    assertTrue(routerCacheNoActiveNamenode(routerContext, "ns0", false));
    assertTrue(endTime - firstLoadTime < cluster.getCacheFlushInterval());
  }

  @Test
  public void testUseObserver() throws Exception {
    setupCluster(1, 2, true);
    RouterContext router = cluster.getRandomRouter();
    allRoutersHeartbeatAndLoadCache();
    transitionActiveToStandby();
    setSecondNonObserverNamenodeInTheRouterCacheActive(2, true);
    allRoutersHeartbeatNotLoadCache(true);

    assertTrue(routerCacheNoActiveNamenode(router, "ns0", true));
    Path path = new Path("/testFile");
    FileSystem fileSystem = router.getFileSystem();
    fileSystem.getFileStatus(path);
  }

  /**
   * Determine whether the router has an active namenode.
   */
  private boolean routerCacheNoActiveNamenode(
      RouterContext context, String nsId, boolean useObserver) throws IOException {
    List<? extends FederationNamenodeContext> namenodes
        = context.getRouter().getNamenodeResolver().getNamenodesForNameserviceId(nsId, useObserver);
    for (FederationNamenodeContext namenode : namenodes) {
      if (namenode.getState() == FederationNamenodeServiceState.ACTIVE){
        return false;
      }
    }
    return true;
  }

  private void allRoutersHeartbeatAndLoadCache() {
    for (MiniRouterDFSCluster.RouterContext routerContext : cluster.getRouters()) {
      // Manually trigger the heartbeat
      Collection<NamenodeHeartbeatService> heartbeatServices = routerContext
          .getRouter().getNamenodeHeartbeatServices();
      for (NamenodeHeartbeatService service : heartbeatServices) {
        service.periodicInvoke();
      }
      // Update service cache
      routerContext.getRouter().getStateStore().refreshCaches(true);
    }
  }

  private void setSecondNonObserverNamenodeInTheRouterCacheActive(
      int numberOfObserver, boolean useObserver) throws IOException {
    for (RouterContext routerContext : cluster.getRouters()) {
      List<? extends FederationNamenodeContext> ns0 = routerContext.getRouter()
          .getNamenodeResolver()
          .getNamenodesForNameserviceId("ns0", useObserver);

      String nsId = ns0.get(numberOfObserver+1).getNamenodeId();
      cluster.switchToActive("ns0", nsId);
      assertEquals(ACTIVE.ordinal(),
          cluster.getNamenode("ns0", nsId).getNamenode().getNameNodeState());
    }
  }

  private void allRoutersHeartbeatNotLoadCache(boolean useObserver) throws IOException {
    for (RouterContext routerContext : cluster.getRouters()) {
      List<? extends FederationNamenodeContext> ns0 = routerContext.getRouter()
          .getNamenodeResolver()
          .getNamenodesForNameserviceId("ns0", useObserver);
      // Manually trigger the heartbeat, but the router does not manually load the cache
      Collection<NamenodeHeartbeatService> heartbeatServices = routerContext
          .getRouter().getNamenodeHeartbeatServices();
      for (NamenodeHeartbeatService service : heartbeatServices) {
        service.periodicInvoke();
      }
    }
  }

  private void transitionActiveToStandby() {
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        List<MiniRouterDFSCluster.NamenodeContext>  nnList = cluster.getNamenodes(ns);
        for (MiniRouterDFSCluster.NamenodeContext namenodeContext : nnList) {
          if (namenodeContext.getNamenode().isActiveState()) {
            cluster.switchToStandby(ns, namenodeContext.getNamenodeId());
          }
        }
      }
    }
  }
}
