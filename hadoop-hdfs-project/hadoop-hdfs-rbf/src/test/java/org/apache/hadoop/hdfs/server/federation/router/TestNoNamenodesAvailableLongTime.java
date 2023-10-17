package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;



import java.net.URI;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE;
import static org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.STANDBY;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.transitionClusterNSToStandby;;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestNoNamenodesAvailableLongTime {

  private StateStoreDFSCluster cluster;

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  private void setupCluster(int numNameservices, int numberOfObserver)
      throws Exception {
    int numberOfNamenode = 2 + numberOfObserver;
    cluster = new StateStoreDFSCluster(true, numNameservices, numberOfNamenode);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .metrics()
        .admin()
        .rpc()
        .heartbeat()
        .build();

    routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY, true);
    routerConf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    routerConf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");

    // Reduce the number of RPC clients threads to overload the Router easy
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE, 4);
    // Overload control
    routerConf.setBoolean(
        RBFConfigKeys.DFS_ROUTER_CLIENT_REJECT_OVERLOAD, false);

    // No need for datanodes as we use renewLease() for testing
    cluster.setNumDatanodesPerNameservice(0);
    // No need for datanodes as we use renewLease() for testing
    cluster.setNumDatanodesPerNameservice(0);
    cluster.addRouterOverrides(routerConf);


    cluster.startCluster();
//    // Making one Namenode active per nameservice
//    if (cluster.isHighAvailability()) {
//      for (String ns : cluster.getNameservices()) {
//        cluster.switchToActive(ns, NAMENODES[0]);
//        cluster.switchToStandby(ns, NAMENODES[1]);
//        for (int i = 2; i < numberOfNamenode; i++) {
//          cluster.switchToObserver(ns, NAMENODES[i]);
//        }
//      }
//    }

    cluster.startRouters();
    cluster.waitClusterUp();
  }

  /**
   * When failover occurs, the router may record that the ns has no active namenode.
   * Only when the router updates the cache next time can the memory status be updated,
   * causing the router to report NoNamenodesAvailableException for a long time.
   */
  @Test
  public void testNoNamenodesAvailableLongTimeWhenNsFailover() throws Exception {
    setupCluster(1, 2);
    transitionClusterNSToStandby(cluster);
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

    DFSClient routerClient = new DFSClient(new URI("hdfs://fed"), conf);

    for (MiniRouterDFSCluster.RouterContext routerContext : cluster.getRouters()) {
      // Get the second namenode in the router cache and make it active
      List<? extends FederationNamenodeContext> ns0 = routerContext.getRouter()
          .getNamenodeResolver()
          .getNamenodesForNameserviceId("ns0", false);

      String nsId = ns0.get(1).getNamenodeId();
      cluster.switchToActive("ns0", nsId);
      // Manually trigger the heartbeat, but the router does not manually load the cache
      Collection<NamenodeHeartbeatService> heartbeatServices = routerContext
          .getRouter().getNamenodeHeartbeatServices();
      for (NamenodeHeartbeatService service : heartbeatServices) {
        service.periodicInvoke();
      }
      assertEquals(ACTIVE.ordinal(),
          cluster.getNamenode("ns0", nsId).getNamenode().getNameNodeState());
    }

    // Get router0 metrics
    FederationRPCMetrics rpcMetrics0 = cluster.getRouters().get(0)
        .getRouter().getRpcServer().getRPCMetrics();
    // Original failures
    long originalRouter0Failures = rpcMetrics0.getProxyOpNoNamenodes();

    /*
     * At this time, the router has recorded 2 standby namenodes in memory,
     * and the first accessed namenode is indeed standby,
     * then an NoNamenodesAvailableException will be reported for the first access,
     * and the next access will be successful.
     */
    routerClient.getFileInfo("/");
    long successReadTime = Time.now();
    assertEquals(originalRouter0Failures + 1, rpcMetrics0.getProxyOpNoNamenodes());

    /*
     * access the active namenode without waiting for the router to update the cache,
     * even if there are 2 standby states recorded in the router memory.
     */
    assertTrue(successReadTime - firstLoadTime < cluster.getCacheFlushInterval());
  }

  @Test
  public void testOnn() throws Exception {
    setupCluster(1, 2);
    MiniRouterDFSCluster.RouterContext router = cluster.getRandomRouter();

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
    Path path = new Path("/testFile");
    FileSystem fileSystem = router.getFileSystem();
    // Send Create call to active
    fileSystem.create(path).close();

    // Send read request to observer. The router will msync to the active namenode.
    fileSystem.open(path).close();
  }

}
