package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolClientSideTranslatorPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.apache.hadoop.util.concurrent.AsyncGet;

import java.io.IOException;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncIpc;
import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncResponse;

public class RouterGetUserMappingsProtocolTranslatorPB
    extends GetUserMappingsProtocolClientSideTranslatorPB {
  private final GetUserMappingsProtocolPB rpcProxy;

  public RouterGetUserMappingsProtocolTranslatorPB(GetUserMappingsProtocolPB rpcProxy) {
    super(rpcProxy);
    this.rpcProxy = rpcProxy;
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getGroupsForUser(user);
    }
    GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto request = GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto
        .newBuilder().setUser(user).build();
    AsyncGet<GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto, Exception> asyncGet =
        asyncIpc(() -> rpcProxy.getGroupsForUser(NULL_CONTROLLER, request));
    asyncResponse(() -> {
      GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto resp = asyncGet.get(-1, null);
      return resp.getGroupsList().toArray(new String[resp.getGroupsCount()]);
    });
    return null;
  }
}
