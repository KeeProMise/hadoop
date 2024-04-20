/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocolPB.RouterAsyncClientProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ObserverRetryOnActiveException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

public class RouterAsyncRpcClient extends RouterRpcClient{
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAsyncRpcClient.class);

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

  @Override
  public Object invokeMethod(
      UserGroupInformation ugi,
      List<? extends FederationNamenodeContext> namenodes,
      boolean useObserver, Class<?> protocol,
      Method method, Object... params) throws IOException {
    if (protocol.getName().equals(ClientProtocol.class.getName())) {
      CompletableFuture<Object> completableFuture =
          invokeMethodAsync(ugi, namenodes, useObserver, protocol, method, params);
      try {
        return completableFuture.get();
      }catch (ExecutionException e) {
        IOException ioe = (IOException) e.getCause();
        throw ioe;
      }catch (InterruptedException e) {
      }
    }
    return super.invokeMethod(ugi, namenodes, useObserver, protocol, method, params);
  }

  private CompletableFuture<Object> invokeMethodAsync(
      final UserGroupInformation ugi,
      final List<? extends FederationNamenodeContext> namenodes,
      boolean useObserver,
      final Class<?> protocol, final Method method, final Object... params)
      throws IOException {

    if (namenodes == null || namenodes.isEmpty()) {
      throw new IOException("No namenodes to invoke " + method.getName() +
          " with params " + Arrays.deepToString(params) + " from "
          + router.getRouterId());
    }

    addClientInfoToCallerContext(ugi);
    if (rpcMonitor != null) {
      rpcMonitor.proxyOp();
    }
    // transfer originCall & callerContext to worker threads of executor.
    final Server.Call originCall = Server.getCurCall().get();
    final CallerContext originContext = CallerContext.getCurrent();

    final long startProxyTime = Time.monotonicNow();
    Map<FederationNamenodeContext, IOException> ioes = new LinkedHashMap<>();

    CompletableFuture<Object[]> completableFuture = CompletableFuture.supplyAsync(
        () -> new Object[]{useObserver, false, false, null},
        RouterAsyncClientProtocolTranslatorPB.getExecutor());

    for (FederationNamenodeContext namenode : namenodes) {
      completableFuture = completableFuture.thenCompose(args -> {
        Boolean shouldUseObserver = (Boolean) args[0];
        Boolean failover = (Boolean) args[1];
        Boolean complete = (Boolean) args[2];
        if (complete) {
          return CompletableFuture.completedFuture(
              new Object[]{shouldUseObserver, failover, complete, args[3]});
        }
        return invokeAsyncTask(originCall, originContext, startProxyTime, ioes, ugi,
            namenode, shouldUseObserver, failover, protocol, method, params);
      });
    }

    return completableFuture.thenApply(args -> {
      Boolean complete = (Boolean) args[2];
      if (complete) {
        return args[3];
      }
      // All namenodes were unavailable or in standby
      String msg = "No namenode available to invoke " + method.getName() + " " +
          Arrays.deepToString(params) + " in " + namenodes + " from " +
          router.getRouterId();
      LOG.error(msg);
      int exConnect = 0;
      for (Map.Entry<FederationNamenodeContext, IOException> entry :
          ioes.entrySet()) {
        FederationNamenodeContext namenode = entry.getKey();
        String nnKey = namenode.getNamenodeKey();
        String addr = namenode.getRpcAddress();
        IOException ioe = entry.getValue();
        if (ioe instanceof StandbyException) {
          LOG.error("{} at {} is in Standby: {}",
              nnKey, addr, ioe.getMessage());
        } else if (isUnavailableException(ioe)) {
          exConnect++;
          LOG.error("{} at {} cannot be reached: {}",
              nnKey, addr, ioe.getMessage());
        } else {
          LOG.error("{} at {} error: \"{}\"", nnKey, addr, ioe.getMessage());
        }
      }
      if (exConnect == ioes.size()) {
        throw new CompletionException(new ConnectException(msg));
      } else {
        throw new CompletionException(new StandbyException(msg));
      }
    });
  }


  @SuppressWarnings("checkstyle:ParameterNumber")
  private CompletableFuture<Object[]> invokeAsyncTask(
      final Server.Call originCall,
      final CallerContext callerContext,
      final long startProxyTime,
      final Map<FederationNamenodeContext, IOException> ioes,
      final UserGroupInformation ugi,
      FederationNamenodeContext namenode,
      boolean useObserver,
      boolean failover,
      final Class<?> protocol, final Method method, final Object... params) {

    if (!useObserver && (namenode.getState() == FederationNamenodeServiceState.OBSERVER)) {
      return CompletableFuture.completedFuture(new Object[]{useObserver, failover, false, null});
    }
    String nsId = namenode.getNameserviceId();
    String rpcAddress = namenode.getRpcAddress();
    try {
      transferThreadLocalContext(originCall, callerContext);
      ConnectionContext connection = getConnection(ugi, nsId, rpcAddress, protocol);
      NameNodeProxiesClient.ProxyAndInfo<?> client = connection.getClient();
      return invokeAsync(originCall, callerContext, nsId, namenode, useObserver,
          0, method, client.getProxy(), params)
          .handle((result, e) -> {
            connection.release();
            boolean complete = false;
            if (result != null || e == null) {
              complete = true;
              if (failover &&
                  FederationNamenodeServiceState.OBSERVER != namenode.getState()) {
                // Success on alternate server, update
                InetSocketAddress address = client.getAddress();
                try {
                  namenodeResolver.updateActiveNamenode(nsId, address);
                } catch (IOException ex) {
                  throw new CompletionException(ex);
                }
              }
              if (this.rpcMonitor != null) {
                this.rpcMonitor.proxyOpComplete(
                    true, nsId, namenode.getState(), Time.monotonicNow() - startProxyTime);
              }
              if (this.router.getRouterClientMetrics() != null) {
                this.router.getRouterClientMetrics().incInvokedMethod(method);
              }
              return new Object[] {useObserver, failover, complete, result};
            }
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
              IOException ioe = (IOException) cause;
              ioes.put(namenode, ioe);
              if (ioe instanceof ObserverRetryOnActiveException) {
                LOG.info("Encountered ObserverRetryOnActiveException from {}."
                    + " Retry active namenode directly.", namenode);
                return new Object[]{false, failover, complete, null};
              } else if (ioe instanceof StandbyException) {
                // Fail over indicated by retry policy and/or NN
                if (this.rpcMonitor != null) {
                  this.rpcMonitor.proxyOpFailureStandby(nsId);
                }
                return new Object[]{useObserver, true, complete, null};
              } else if (isUnavailableException(ioe)) {
                if (this.rpcMonitor != null) {
                  this.rpcMonitor.proxyOpFailureCommunicate(nsId);
                }
                boolean tmpFailover = failover;
                if (FederationNamenodeServiceState.OBSERVER == namenode.getState()) {
                  try {
                    namenodeResolver.updateUnavailableNamenode(nsId,
                        NetUtils.createSocketAddr(namenode.getRpcAddress()));
                  } catch (IOException ex) {
                    throw new CompletionException(ex);
                  }
                } else {
                  tmpFailover = true;
                }
                return new Object[]{useObserver, tmpFailover, complete, null};
              } else if (ioe instanceof RemoteException) {
                if (this.rpcMonitor != null) {
                  this.rpcMonitor.proxyOpComplete(
                      true, nsId, namenode.getState(), Time.monotonicNow() - startProxyTime);
                }
                RemoteException re = (RemoteException) ioe;
                ioe = re.unwrapRemoteException();
                ioe = getCleanException(ioe);
                // RemoteException returned by NN
                throw new CompletionException(ioe);
              } else if (ioe instanceof NoNamenodesAvailableException) {
                IOException cau = (IOException) ioe.getCause();
                if (this.rpcMonitor != null) {
                  this.rpcMonitor.proxyOpNoNamenodes(nsId);
                }
                LOG.error("Cannot get available namenode for {} {} error: {}",
                    nsId, rpcAddress, ioe.getMessage());
                // Rotate cache so that client can retry the next namenode in the cache
                if (shouldRotateCache(cau)) {
                  this.namenodeResolver.rotateCache(nsId, namenode, useObserver);
                }
                // Throw RetriableException so that client can retry
                throw new CompletionException(new RetriableException(ioe));
              } else {
                // Other communication error, this is a failure
                // Communication retries are handled by the retry policy
                if (this.rpcMonitor != null) {
                  this.rpcMonitor.proxyOpFailureCommunicate(nsId);
                  this.rpcMonitor.proxyOpComplete(
                      false, nsId, namenode.getState(), Time.monotonicNow() - startProxyTime);
                }
                throw new CompletionException(ioe);
              }
            }
            throw new CompletionException(cause);
          });
    }catch (IOException ioe) {
      assert ioe instanceof ConnectionNullException;
      if (this.rpcMonitor != null) {
        this.rpcMonitor.proxyOpFailureCommunicate(nsId);
      }
      LOG.error("Get connection for {} {} error: {}", nsId, rpcAddress,
          ioe.getMessage());
      // Throw StandbyException so that client can retry
      StandbyException se = new StandbyException(ioe.getMessage());
      se.initCause(ioe);
      throw new CompletionException(se);
    }
  }


  @SuppressWarnings("checkstyle:ParameterNumber")
  public CompletableFuture<Object> invokeAsync(
      final Server.Call originCall,
      final CallerContext callerContext,
      String nsId, FederationNamenodeContext namenode,
      Boolean listObserverFirst,
      int retryCount, final Method method,
      final Object obj, final Object... params) {
    try {
      transferThreadLocalContext(originCall, callerContext);
      Client.setAsynchronousMode(true);
      method.invoke(obj, params);
      CompletableFuture<Object> completableFuture =
          RouterAsyncClientProtocolTranslatorPB.getCompletableFuture();

      return completableFuture.handle((BiFunction<Object, Throwable, Object>) (result, e) -> {
        if (e == null) {
          return new Object[]{result, true};
        }

        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          IOException ioe = (IOException) cause;

          // Check if we should retry.
          RetryPolicy.RetryAction.RetryDecision decision = null;
          try {
            decision = shouldRetry(ioe, retryCount, nsId, namenode, listObserverFirst);
          } catch (IOException ex) {
            throw new CompletionException(ex);
          }
          if (decision == RetryPolicy.RetryAction.RetryDecision.RETRY) {
            if (RouterAsyncRpcClient.this.rpcMonitor != null) {
              RouterAsyncRpcClient.this.rpcMonitor.proxyOpRetries();
            }
            // retry
            return new Object[]{result, false};
          } else if (decision == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY) {
            // failover, invoker looks for standby exceptions for failover.
            if (ioe instanceof StandbyException) {
              throw new CompletionException(ioe);
            } else if (isUnavailableException(ioe)) {
              throw new CompletionException(ioe);
            } else {
              throw new CompletionException(new StandbyException(ioe.getMessage()));
            }
          } else {
            throw new CompletionException(ioe);
          }
        } else {
          throw new CompletionException(new IOException(e));
        }
      }).thenCompose(o -> {
        Object[] args = (Object[]) o;
        boolean complete = (boolean) args[1];
        if (complete) {
          return CompletableFuture.completedFuture(args[0]);
        }
        return invokeAsync(originCall, callerContext, nsId, namenode,
            listObserverFirst, retryCount + 1, method, obj, params);
      });
    } catch (InvocationTargetException e) {
      throw new CompletionException(e.getCause());
    } catch (Exception e) {
      throw new CompletionException(e);
    }
  }
}
