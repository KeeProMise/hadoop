package org.apache.hadoop.hdfs.protocolPB;


import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto;

import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolServerSideTranslatorPB;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncRouterServer;

public class RouterGetUserMappingsProtocolServerSideTranslatorPB
    extends GetUserMappingsProtocolServerSideTranslatorPB {
  private final RouterRpcServer server;
  private final boolean isAsyncRpc;

  public RouterGetUserMappingsProtocolServerSideTranslatorPB(GetUserMappingsProtocol impl) {
    super(impl);
    this.server = (RouterRpcServer) impl;
    this.isAsyncRpc = server.isAsync();
  }

  @Override
  public GetGroupsForUserResponseProto getGroupsForUser(
      RpcController controller,
      GetGroupsForUserRequestProto request) throws ServiceException {
    if (!isAsyncRpc) {
      return super.getGroupsForUser(controller, request);
    }
    asyncRouterServer(() -> server.getGroupsForUser(request.getUser()), groups -> {
      GetGroupsForUserResponseProto.Builder builder = GetGroupsForUserResponseProto
          .newBuilder();
      for (String g : groups) {
        builder.addGroups(g);
      }
      return builder.build();
    });
    return null;
  }
}
