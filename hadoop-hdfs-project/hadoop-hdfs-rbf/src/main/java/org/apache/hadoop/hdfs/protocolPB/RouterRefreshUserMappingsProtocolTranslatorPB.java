package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;

import org.apache.hadoop.util.concurrent.AsyncGet;

import java.io.IOException;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncIpc;
import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncResponse;


public class RouterRefreshUserMappingsProtocolTranslatorPB
    extends RefreshUserMappingsProtocolClientSideTranslatorPB {
  private final RefreshUserMappingsProtocolPB rpcProxy;
  public RouterRefreshUserMappingsProtocolTranslatorPB(RefreshUserMappingsProtocolPB rpcProxy) {
    super(rpcProxy);
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void refreshUserToGroupsMappings() throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.refreshUserToGroupsMappings();
      return;
    }
    AsyncGet<RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.refreshUserToGroupsMappings(NULL_CONTROLLER,
        VOID_REFRESH_USER_TO_GROUPS_MAPPING_REQUEST));
    asyncResponse(() -> {
      asyncGet.get(-1, null);
      return null;
    });
  }

  @Override
  public void refreshSuperUserGroupsConfiguration() throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.refreshSuperUserGroupsConfiguration();
      return;
    }
    AsyncGet<RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.refreshSuperUserGroupsConfiguration(NULL_CONTROLLER,
        VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_REQUEST));
    asyncResponse(() -> {
      asyncGet.get(-1, null);
      return null;
    });
  }
}
