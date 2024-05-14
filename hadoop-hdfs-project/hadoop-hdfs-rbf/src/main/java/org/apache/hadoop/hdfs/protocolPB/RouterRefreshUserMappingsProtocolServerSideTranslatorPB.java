package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationRequestProto;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshSuperUserGroupsConfigurationResponseProto;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserToGroupsMappingsResponseProto;

import java.io.IOException;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncRouterServer;

public class RouterRefreshUserMappingsProtocolServerSideTranslatorPB
    extends RefreshUserMappingsProtocolServerSideTranslatorPB {

  private final RouterRpcServer server;
  private final boolean isAsyncRpc;

  public RouterRefreshUserMappingsProtocolServerSideTranslatorPB(
      RefreshUserMappingsProtocol impl) {
    super(impl);
    this.server = (RouterRpcServer) impl;
    this.isAsyncRpc = server.isAsync();
  }

  @Override
  public RefreshUserToGroupsMappingsResponseProto
      refreshUserToGroupsMappings(
          RpcController controller,
          RefreshUserToGroupsMappingsRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.refreshUserToGroupsMappings(controller, request);
    }
    asyncRouterServer(() -> {
      server.refreshUserToGroupsMappings();
      return null;
    }, result ->
        VOID_REFRESH_USER_GROUPS_MAPPING_RESPONSE);
    return null;
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponseProto
      refreshSuperUserGroupsConfiguration(
      RpcController controller,
      RefreshSuperUserGroupsConfigurationRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.refreshSuperUserGroupsConfiguration(controller, request);
    }
    asyncRouterServer(() -> {
      server.refreshSuperUserGroupsConfiguration();
      return null;
    }, result ->
        VOID_REFRESH_SUPERUSER_GROUPS_CONFIGURATION_RESPONSE);
    return null;
  }
}
