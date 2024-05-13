package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.BatchedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsPartialListing;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos;
import org.apache.hadoop.hdfs.protocol.proto.ErasureCodingProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.proto.SecurityProtos;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.thirdparty.protobuf.ProtocolStringList;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncRouterServer;

public class RouterClientNamenodeProtocolServerSideTranslatorPB
    extends ClientNamenodeProtocolServerSideTranslatorPB{
  private final RouterRpcServer server;
  public RouterClientNamenodeProtocolServerSideTranslatorPB(
      ClientProtocol server) throws IOException {
    super(server);
    this.server = (RouterRpcServer) server;
  }


  @Override
  public ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto getBlockLocations(
      RpcController controller, ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto req) {
    asyncRouterServer(() -> server.getBlockLocations(req.getSrc(), req.getOffset(),
        req.getLength()),
        b -> {
          ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto.Builder builder
              = ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto
              .newBuilder();
          if (b != null) {
            builder.setLocations(PBHelperClient.convert(b)).build();
          }
          return builder.build();
        });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto getServerDefaults(
      RpcController controller, ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto req) {
    asyncRouterServer(server::getServerDefaults,
        result -> ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto.newBuilder()
            .setServerDefaults(PBHelperClient.convert(result))
            .build());
    return null;
  }


  @Override
  public ClientNamenodeProtocolProtos.CreateResponseProto create(
      RpcController controller,
      ClientNamenodeProtocolProtos.CreateRequestProto req) {
    asyncRouterServer(() -> {
      FsPermission masked = req.hasUnmasked() ?
          FsCreateModes.create(PBHelperClient.convert(req.getMasked()),
              PBHelperClient.convert(req.getUnmasked())) :
          PBHelperClient.convert(req.getMasked());
      return server.create(req.getSrc(),
          masked, req.getClientName(),
          PBHelperClient.convertCreateFlag(req.getCreateFlag()), req.getCreateParent(),
          (short) req.getReplication(), req.getBlockSize(),
          PBHelperClient.convertCryptoProtocolVersions(
              req.getCryptoProtocolVersionList()),
          req.getEcPolicyName(), req.getStoragePolicy());
    }, result -> {
      if (result != null) {
        return ClientNamenodeProtocolProtos
            .CreateResponseProto.newBuilder().setFs(PBHelperClient.convert(result))
            .build();
      }
      return VOID_CREATE_RESPONSE;
    });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.AppendResponseProto append(
      RpcController controller,
      ClientNamenodeProtocolProtos.AppendRequestProto req) {
    asyncRouterServer(() -> {
      EnumSetWritable<CreateFlag> flags = req.hasFlag() ?
          PBHelperClient.convertCreateFlag(req.getFlag()) :
          new EnumSetWritable<>(EnumSet.of(CreateFlag.APPEND));
      return server.append(req.getSrc(),
          req.getClientName(), flags);
    }, result -> {
      ClientNamenodeProtocolProtos.AppendResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.AppendResponseProto.newBuilder();
      if (result.getLastBlock() != null) {
        builder.setBlock(PBHelperClient.convertLocatedBlock(
            result.getLastBlock()));
      }
      if (result.getFileStatus() != null) {
        builder.setStat(PBHelperClient.convert(result.getFileStatus()));
      }
      return builder.build();
    });
    return null;
  }

  @Override
  public ClientNamenodeProtocolProtos.SetReplicationResponseProto setReplication(
      RpcController controller,
      ClientNamenodeProtocolProtos.SetReplicationRequestProto req) {
    asyncRouterServer(() ->
        server.setReplication(req.getSrc(), (short) req.getReplication()),
        result -> ClientNamenodeProtocolProtos
        .SetReplicationResponseProto.newBuilder().setResult(result).build());
    return null;
  }


  @Override
  public ClientNamenodeProtocolProtos.SetPermissionResponseProto setPermission(
      RpcController controller,
      ClientNamenodeProtocolProtos.SetPermissionRequestProto req) throws ServiceException {
    try {
      server.setPermission(req.getSrc(), PBHelperClient.convert(req.getPermission()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SET_PERM_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.SetOwnerResponseProto setOwner(RpcController controller,
                                                                     ClientNamenodeProtocolProtos.SetOwnerRequestProto req) throws ServiceException {
    try {
      server.setOwner(req.getSrc(),
          req.hasUsername() ? req.getUsername() : null,
          req.hasGroupname() ? req.getGroupname() : null);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SET_OWNER_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.AbandonBlockResponseProto abandonBlock(RpcController controller,
                                                                             ClientNamenodeProtocolProtos.AbandonBlockRequestProto req) throws ServiceException {
    try {
      server.abandonBlock(PBHelperClient.convert(req.getB()), req.getFileId(),
          req.getSrc(), req.getHolder());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_ADD_BLOCK_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.AddBlockResponseProto addBlock(RpcController controller,
                                                                     ClientNamenodeProtocolProtos.AddBlockRequestProto req) throws ServiceException {

    try {
      List<HdfsProtos.DatanodeInfoProto> excl = req.getExcludeNodesList();
      List<String> favor = req.getFavoredNodesList();
      EnumSet<AddBlockFlag> flags =
          PBHelperClient.convertAddBlockFlags(req.getFlagsList());
      LocatedBlock result = server.addBlock(
          req.getSrc(),
          req.getClientName(),
          req.hasPrevious() ? PBHelperClient.convert(req.getPrevious()) : null,
          (excl == null || excl.size() == 0) ? null : PBHelperClient.convert(excl
              .toArray(new HdfsProtos.DatanodeInfoProto[excl.size()])), req.getFileId(),
          (favor == null || favor.size() == 0) ? null : favor
              .toArray(new String[favor.size()]),
          flags);
      return ClientNamenodeProtocolProtos.AddBlockResponseProto.newBuilder()
          .setBlock(PBHelperClient.convertLocatedBlock(result)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetAdditionalDatanodeResponseProto getAdditionalDatanode(
      RpcController controller, ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto req)
      throws ServiceException {
    try {
      List<HdfsProtos.DatanodeInfoProto> existingList = req.getExistingsList();
      List<String> existingStorageIDsList = req.getExistingStorageUuidsList();
      List<HdfsProtos.DatanodeInfoProto> excludesList = req.getExcludesList();
      LocatedBlock result = server.getAdditionalDatanode(req.getSrc(),
          req.getFileId(), PBHelperClient.convert(req.getBlk()),
          PBHelperClient.convert(existingList.toArray(
              new HdfsProtos.DatanodeInfoProto[existingList.size()])),
          existingStorageIDsList.toArray(
              new String[existingStorageIDsList.size()]),
          PBHelperClient.convert(excludesList.toArray(
              new HdfsProtos.DatanodeInfoProto[excludesList.size()])),
          req.getNumAdditionalNodes(), req.getClientName());
      return ClientNamenodeProtocolProtos.GetAdditionalDatanodeResponseProto.newBuilder().setBlock(
              PBHelperClient.convertLocatedBlock(result))
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.CompleteResponseProto complete(RpcController controller,
                                                                     ClientNamenodeProtocolProtos.CompleteRequestProto req) throws ServiceException {
    try {
      boolean result =
          server.complete(req.getSrc(), req.getClientName(),
              req.hasLast() ? PBHelperClient.convert(req.getLast()) : null,
              req.hasFileId() ? req.getFileId() : HdfsConstants.GRANDFATHER_INODE_ID);
      return ClientNamenodeProtocolProtos.CompleteResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.ReportBadBlocksResponseProto reportBadBlocks(RpcController controller,
                                                                                   ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto req) throws ServiceException {
    try {
      List<HdfsProtos.LocatedBlockProto> bl = req.getBlocksList();
      server.reportBadBlocks(PBHelperClient.convertLocatedBlocks(
          bl.toArray(new HdfsProtos.LocatedBlockProto[bl.size()])));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REP_BAD_BLOCK_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.ConcatResponseProto concat(RpcController controller,
                                                                 ClientNamenodeProtocolProtos.ConcatRequestProto req) throws ServiceException {
    try {
      List<String> srcs = req.getSrcsList();
      server.concat(req.getTrg(), srcs.toArray(new String[srcs.size()]));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_CONCAT_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.RenameResponseProto rename(RpcController controller,
                                                                 ClientNamenodeProtocolProtos.RenameRequestProto req) throws ServiceException {
    try {
      boolean result = server.rename(req.getSrc(), req.getDst());
      return ClientNamenodeProtocolProtos.RenameResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.Rename2ResponseProto rename2(RpcController controller,
                                                                   ClientNamenodeProtocolProtos.Rename2RequestProto req) throws ServiceException {
    // resolve rename options
    ArrayList<Options.Rename> optionList = new ArrayList<Options.Rename>();
    if(req.getOverwriteDest()) {
      optionList.add(Options.Rename.OVERWRITE);
    }
    if (req.hasMoveToTrash() && req.getMoveToTrash()) {
      optionList.add(Options.Rename.TO_TRASH);
    }

    if(optionList.isEmpty()) {
      optionList.add(Options.Rename.NONE);
    }

    try {
      server.rename2(req.getSrc(), req.getDst(),
          optionList.toArray(new Options.Rename[optionList.size()]));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_RENAME2_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.TruncateResponseProto truncate(RpcController controller,
                                                                     ClientNamenodeProtocolProtos.TruncateRequestProto req) throws ServiceException {
    try {
      boolean result = server.truncate(req.getSrc(), req.getNewLength(),
          req.getClientName());
      return ClientNamenodeProtocolProtos.TruncateResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.DeleteResponseProto delete(RpcController controller,
                                                                 ClientNamenodeProtocolProtos.DeleteRequestProto req) throws ServiceException {
    try {
      boolean result =  server.delete(req.getSrc(), req.getRecursive());
      return ClientNamenodeProtocolProtos.DeleteResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.MkdirsResponseProto mkdirs(RpcController controller,
                                                                 ClientNamenodeProtocolProtos.MkdirsRequestProto req) throws ServiceException {
    try {
      FsPermission masked = req.hasUnmasked() ?
          FsCreateModes.create(PBHelperClient.convert(req.getMasked()),
              PBHelperClient.convert(req.getUnmasked())) :
          PBHelperClient.convert(req.getMasked());
      boolean result = server.mkdirs(req.getSrc(), masked,
          req.getCreateParent());
      return ClientNamenodeProtocolProtos.MkdirsResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetListingResponseProto getListing(RpcController controller,
                                                                         ClientNamenodeProtocolProtos.GetListingRequestProto req) throws ServiceException {
    try {
      DirectoryListing result = server.getListing(
          req.getSrc(), req.getStartAfter().toByteArray(),
          req.getNeedLocation());
      if (result !=null) {
        return ClientNamenodeProtocolProtos.GetListingResponseProto.newBuilder().setDirList(
            PBHelperClient.convert(result)).build();
      } else {
        return VOID_GETLISTING_RESPONSE;
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetBatchedListingResponseProto getBatchedListing(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetBatchedListingRequestProto request) throws ServiceException {
    try {
      BatchedDirectoryListing result = server.getBatchedListing(
          request.getPathsList().toArray(new String[] {}),
          request.getStartAfter().toByteArray(),
          request.getNeedLocation());
      if (result != null) {
        ClientNamenodeProtocolProtos.GetBatchedListingResponseProto.Builder builder =
            ClientNamenodeProtocolProtos.GetBatchedListingResponseProto.newBuilder();
        for (HdfsPartialListing partialListing : result.getListings()) {
          HdfsProtos.BatchedDirectoryListingProto.Builder listingBuilder =
              HdfsProtos.BatchedDirectoryListingProto.newBuilder();
          if (partialListing.getException() != null) {
            RemoteException ex = partialListing.getException();
            HdfsProtos.RemoteExceptionProto.Builder rexBuilder =
                HdfsProtos.RemoteExceptionProto.newBuilder();
            rexBuilder.setClassName(ex.getClassName());
            if (ex.getMessage() != null) {
              rexBuilder.setMessage(ex.getMessage());
            }
            listingBuilder.setException(rexBuilder.build());
          } else {
            for (HdfsFileStatus f : partialListing.getPartialListing()) {
              listingBuilder.addPartialListing(PBHelperClient.convert(f));
            }
          }
          listingBuilder.setParentIdx(partialListing.getParentIdx());
          builder.addListings(listingBuilder);
        }
        builder.setHasMore(result.hasMore());
        builder.setStartAfter(ByteString.copyFrom(result.getStartAfter()));
        return builder.build();
      } else {
        return VOID_GETBATCHEDLISTING_RESPONSE;
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RenewLeaseResponseProto renewLease(RpcController controller,
                                                                         ClientNamenodeProtocolProtos.RenewLeaseRequestProto req) throws ServiceException {
    try {
      server.renewLease(req.getClientName(), req.getNamespacesList());
      return VOID_RENEWLEASE_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RecoverLeaseResponseProto recoverLease(RpcController controller,
                                                                             ClientNamenodeProtocolProtos.RecoverLeaseRequestProto req) throws ServiceException {
    try {
      boolean result = server.recoverLease(req.getSrc(), req.getClientName());
      return ClientNamenodeProtocolProtos.RecoverLeaseResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RestoreFailedStorageResponseProto restoreFailedStorage(
      RpcController controller, ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto req)
      throws ServiceException {
    try {
      boolean result = server.restoreFailedStorage(req.getArg());
      return ClientNamenodeProtocolProtos.RestoreFailedStorageResponseProto.newBuilder().setResult(result)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetFsStatsResponseProto getFsStats(RpcController controller,
                                                                         ClientNamenodeProtocolProtos.GetFsStatusRequestProto req) throws ServiceException {
    try {
      return PBHelperClient.convert(server.getStats());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetFsReplicatedBlockStatsResponseProto getFsReplicatedBlockStats(
      RpcController controller, ClientNamenodeProtocolProtos.GetFsReplicatedBlockStatsRequestProto request)
      throws ServiceException {
    try {
      return PBHelperClient.convert(server.getReplicatedBlockStats());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetFsECBlockGroupStatsResponseProto getFsECBlockGroupStats(
      RpcController controller, ClientNamenodeProtocolProtos.GetFsECBlockGroupStatsRequestProto request)
      throws ServiceException {
    try {
      return PBHelperClient.convert(server.getECBlockGroupStats());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetDatanodeReportResponseProto getDatanodeReport(
      RpcController controller, ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto req)
      throws ServiceException {
    try {
      List<? extends HdfsProtos.DatanodeInfoProto> result = PBHelperClient.convert(server
          .getDatanodeReport(PBHelperClient.convert(req.getType())));
      return ClientNamenodeProtocolProtos.GetDatanodeReportResponseProto.newBuilder()
          .addAllDi(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto getDatanodeStorageReport(
      RpcController controller, ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto req)
      throws ServiceException {
    try {
      List<ClientNamenodeProtocolProtos.DatanodeStorageReportProto> reports = PBHelperClient.convertDatanodeStorageReports(
          server.getDatanodeStorageReport(PBHelperClient.convert(req.getType())));
      return ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto.newBuilder()
          .addAllDatanodeStorageReports(reports)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetPreferredBlockSizeResponseProto getPreferredBlockSize(
      RpcController controller, ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto req)
      throws ServiceException {
    try {
      long result = server.getPreferredBlockSize(req.getFilename());
      return ClientNamenodeProtocolProtos.GetPreferredBlockSizeResponseProto.newBuilder().setBsize(result)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.SetSafeModeResponseProto setSafeMode(RpcController controller,
                                                                           ClientNamenodeProtocolProtos.SetSafeModeRequestProto req) throws ServiceException {
    try {
      boolean result = server.setSafeMode(PBHelperClient.convert(req.getAction()),
          req.getChecked());
      return ClientNamenodeProtocolProtos.SetSafeModeResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.SaveNamespaceResponseProto saveNamespace(RpcController controller,
                                                                               ClientNamenodeProtocolProtos.SaveNamespaceRequestProto req) throws ServiceException {
    try {
      final long timeWindow = req.hasTimeWindow() ? req.getTimeWindow() : 0;
      final long txGap = req.hasTxGap() ? req.getTxGap() : 0;
      boolean saved = server.saveNamespace(timeWindow, txGap);
      return ClientNamenodeProtocolProtos.SaveNamespaceResponseProto.newBuilder().setSaved(saved).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RollEditsResponseProto rollEdits(RpcController controller,
                                                                       ClientNamenodeProtocolProtos.RollEditsRequestProto request) throws ServiceException {
    try {
      long txid = server.rollEdits();
      return ClientNamenodeProtocolProtos.RollEditsResponseProto.newBuilder()
          .setNewSegmentTxId(txid)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }


  @Override
  public ClientNamenodeProtocolProtos.RefreshNodesResponseProto refreshNodes(RpcController controller,
                                                                             ClientNamenodeProtocolProtos.RefreshNodesRequestProto req) throws ServiceException {
    try {
      server.refreshNodes();
      return VOID_REFRESHNODES_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }

  }

  @Override
  public ClientNamenodeProtocolProtos.FinalizeUpgradeResponseProto finalizeUpgrade(RpcController controller,
                                                                                   ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto req) throws ServiceException {
    try {
      server.finalizeUpgrade();
      return VOID_FINALIZEUPGRADE_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.UpgradeStatusResponseProto upgradeStatus(
      RpcController controller, ClientNamenodeProtocolProtos.UpgradeStatusRequestProto req)
      throws ServiceException {
    try {
      final boolean isUpgradeFinalized = server.upgradeStatus();
      ClientNamenodeProtocolProtos.UpgradeStatusResponseProto.Builder b =
          ClientNamenodeProtocolProtos.UpgradeStatusResponseProto.newBuilder();
      b.setUpgradeFinalized(isUpgradeFinalized);
      return b.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RollingUpgradeResponseProto rollingUpgrade(RpcController controller,
                                                                                 ClientNamenodeProtocolProtos.RollingUpgradeRequestProto req) throws ServiceException {
    try {
      final RollingUpgradeInfo info = server.rollingUpgrade(
          PBHelperClient.convert(req.getAction()));
      final ClientNamenodeProtocolProtos.RollingUpgradeResponseProto.Builder b = ClientNamenodeProtocolProtos.RollingUpgradeResponseProto.newBuilder();
      if (info != null) {
        b.setRollingUpgradeInfo(PBHelperClient.convert(info));
      }
      return b.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto listCorruptFileBlocks(
      RpcController controller, ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto req)
      throws ServiceException {
    try {
      CorruptFileBlocks result = server.listCorruptFileBlocks(
          req.getPath(), req.hasCookie() ? req.getCookie(): null);
      return ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto.newBuilder()
          .setCorrupt(PBHelperClient.convert(result))
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.MetaSaveResponseProto metaSave(RpcController controller,
                                                                     ClientNamenodeProtocolProtos.MetaSaveRequestProto req) throws ServiceException {
    try {
      server.metaSave(req.getFilename());
      return VOID_METASAVE_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }

  }

  @Override
  public ClientNamenodeProtocolProtos.GetFileInfoResponseProto getFileInfo(RpcController controller,
                                                                           ClientNamenodeProtocolProtos.GetFileInfoRequestProto req) throws ServiceException {
    try {
      HdfsFileStatus result = server.getFileInfo(req.getSrc());

      if (result != null) {
        return ClientNamenodeProtocolProtos.GetFileInfoResponseProto.newBuilder().setFs(
            PBHelperClient.convert(result)).build();
      }
      return VOID_GETFILEINFO_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetLocatedFileInfoResponseProto getLocatedFileInfo(
      RpcController controller, ClientNamenodeProtocolProtos.GetLocatedFileInfoRequestProto req)
      throws ServiceException {
    try {
      HdfsFileStatus result = server.getLocatedFileInfo(req.getSrc(),
          req.getNeedBlockToken());
      if (result != null) {
        return ClientNamenodeProtocolProtos.GetLocatedFileInfoResponseProto.newBuilder().setFs(
            PBHelperClient.convert(result)).build();
      }
      return VOID_GETLOCATEDFILEINFO_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto getFileLinkInfo(RpcController controller,
                                                                                   ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto req) throws ServiceException {
    try {
      HdfsFileStatus result = server.getFileLinkInfo(req.getSrc());
      if (result != null) {
        return ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto.newBuilder().setFs(
            PBHelperClient.convert(result)).build();
      } else {
        return VOID_GETFILELINKINFO_RESPONSE;
      }

    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetContentSummaryResponseProto getContentSummary(
      RpcController controller, ClientNamenodeProtocolProtos.GetContentSummaryRequestProto req)
      throws ServiceException {
    try {
      ContentSummary result = server.getContentSummary(req.getPath());
      return ClientNamenodeProtocolProtos.GetContentSummaryResponseProto.newBuilder()
          .setSummary(PBHelperClient.convert(result)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.SetQuotaResponseProto setQuota(RpcController controller,
                                                                     ClientNamenodeProtocolProtos.SetQuotaRequestProto req) throws ServiceException {
    try {
      server.setQuota(req.getPath(), req.getNamespaceQuota(),
          req.getStoragespaceQuota(),
          req.hasStorageType() ?
              PBHelperClient.convertStorageType(req.getStorageType()): null);
      return VOID_SETQUOTA_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.FsyncResponseProto fsync(RpcController controller,
                                                               ClientNamenodeProtocolProtos.FsyncRequestProto req) throws ServiceException {
    try {
      server.fsync(req.getSrc(), req.getFileId(),
          req.getClient(), req.getLastBlockLength());
      return VOID_FSYNC_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.SetTimesResponseProto setTimes(RpcController controller,
                                                                     ClientNamenodeProtocolProtos.SetTimesRequestProto req) throws ServiceException {
    try {
      server.setTimes(req.getSrc(), req.getMtime(), req.getAtime());
      return VOID_SETTIMES_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.CreateSymlinkResponseProto createSymlink(RpcController controller,
                                                                               ClientNamenodeProtocolProtos.CreateSymlinkRequestProto req) throws ServiceException {
    try {
      server.createSymlink(req.getTarget(), req.getLink(),
          PBHelperClient.convert(req.getDirPerm()), req.getCreateParent());
      return VOID_CREATESYMLINK_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetLinkTargetResponseProto getLinkTarget(RpcController controller,
                                                                               ClientNamenodeProtocolProtos.GetLinkTargetRequestProto req) throws ServiceException {
    try {
      String result = server.getLinkTarget(req.getPath());
      ClientNamenodeProtocolProtos.GetLinkTargetResponseProto.Builder builder = ClientNamenodeProtocolProtos.GetLinkTargetResponseProto
          .newBuilder();
      if (result != null) {
        builder.setTargetPath(result);
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.UpdateBlockForPipelineResponseProto updateBlockForPipeline(
      RpcController controller, ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto req)
      throws ServiceException {
    try {
      HdfsProtos.LocatedBlockProto result = PBHelperClient.convertLocatedBlock(
          server.updateBlockForPipeline(PBHelperClient.convert(req.getBlock()),
              req.getClientName()));
      return ClientNamenodeProtocolProtos.UpdateBlockForPipelineResponseProto.newBuilder().setBlock(result)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.UpdatePipelineResponseProto updatePipeline(RpcController controller,
                                                                                 ClientNamenodeProtocolProtos.UpdatePipelineRequestProto req) throws ServiceException {
    try {
      List<HdfsProtos.DatanodeIDProto> newNodes = req.getNewNodesList();
      List<String> newStorageIDs = req.getStorageIDsList();
      server.updatePipeline(req.getClientName(),
          PBHelperClient.convert(req.getOldBlock()),
          PBHelperClient.convert(req.getNewBlock()),
          PBHelperClient.convert(newNodes.toArray(new HdfsProtos.DatanodeIDProto[newNodes.size()])),
          newStorageIDs.toArray(new String[newStorageIDs.size()]));
      return VOID_UPDATEPIPELINE_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SecurityProtos.GetDelegationTokenResponseProto getDelegationToken(
      RpcController controller, SecurityProtos.GetDelegationTokenRequestProto req)
      throws ServiceException {
    try {
      Token<DelegationTokenIdentifier> token = server
          .getDelegationToken(new Text(req.getRenewer()));
      SecurityProtos.GetDelegationTokenResponseProto.Builder rspBuilder =
          SecurityProtos.GetDelegationTokenResponseProto.newBuilder();
      if (token != null) {
        rspBuilder.setToken(PBHelperClient.convert(token));
      }
      return rspBuilder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SecurityProtos.RenewDelegationTokenResponseProto renewDelegationToken(
      RpcController controller, SecurityProtos.RenewDelegationTokenRequestProto req)
      throws ServiceException {
    try {
      long result = server.renewDelegationToken(PBHelperClient
          .convertDelegationToken(req.getToken()));
      return SecurityProtos.RenewDelegationTokenResponseProto.newBuilder()
          .setNewExpiryTime(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SecurityProtos.CancelDelegationTokenResponseProto cancelDelegationToken(
      RpcController controller, SecurityProtos.CancelDelegationTokenRequestProto req)
      throws ServiceException {
    try {
      server.cancelDelegationToken(PBHelperClient.convertDelegationToken(req
          .getToken()));
      return VOID_CANCELDELEGATIONTOKEN_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.SetBalancerBandwidthResponseProto setBalancerBandwidth(
      RpcController controller, ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto req)
      throws ServiceException {
    try {
      server.setBalancerBandwidth(req.getBandwidth());
      return VOID_SETBALANCERBANDWIDTH_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto getDataEncryptionKey(
      RpcController controller, ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto request)
      throws ServiceException {
    try {
      ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto.newBuilder();
      DataEncryptionKey encryptionKey = server.getDataEncryptionKey();
      if (encryptionKey != null) {
        builder.setDataEncryptionKey(PBHelperClient.convert(encryptionKey));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.CreateSnapshotResponseProto createSnapshot(RpcController controller,
                                                                                 ClientNamenodeProtocolProtos.CreateSnapshotRequestProto req) throws ServiceException {
    try {
      final ClientNamenodeProtocolProtos.CreateSnapshotResponseProto.Builder builder
          = ClientNamenodeProtocolProtos.CreateSnapshotResponseProto.newBuilder();
      final String snapshotPath = server.createSnapshot(req.getSnapshotRoot(),
          req.hasSnapshotName()? req.getSnapshotName(): null);
      if (snapshotPath != null) {
        builder.setSnapshotPath(snapshotPath);
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.DeleteSnapshotResponseProto deleteSnapshot(RpcController controller,
                                                                                 ClientNamenodeProtocolProtos.DeleteSnapshotRequestProto req) throws ServiceException {
    try {
      server.deleteSnapshot(req.getSnapshotRoot(), req.getSnapshotName());
      return VOID_DELETE_SNAPSHOT_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.AllowSnapshotResponseProto allowSnapshot(RpcController controller,
                                                                               ClientNamenodeProtocolProtos.AllowSnapshotRequestProto req) throws ServiceException {
    try {
      server.allowSnapshot(req.getSnapshotRoot());
      return VOID_ALLOW_SNAPSHOT_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.DisallowSnapshotResponseProto disallowSnapshot(RpcController controller,
                                                                                     ClientNamenodeProtocolProtos.DisallowSnapshotRequestProto req) throws ServiceException {
    try {
      server.disallowSnapshot(req.getSnapshotRoot());
      return VOID_DISALLOW_SNAPSHOT_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RenameSnapshotResponseProto renameSnapshot(RpcController controller,
                                                                                 ClientNamenodeProtocolProtos.RenameSnapshotRequestProto request) throws ServiceException {
    try {
      server.renameSnapshot(request.getSnapshotRoot(),
          request.getSnapshotOldName(), request.getSnapshotNewName());
      return VOID_RENAME_SNAPSHOT_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto getSnapshottableDirListing(
      RpcController controller, ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto request)
      throws ServiceException {
    try {
      SnapshottableDirectoryStatus[] result = server
          .getSnapshottableDirListing();
      if (result != null) {
        return ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto.newBuilder().
            setSnapshottableDirList(PBHelperClient.convert(result)).build();
      } else {
        return NULL_GET_SNAPSHOTTABLE_DIR_LISTING_RESPONSE;
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetSnapshotListingResponseProto getSnapshotListing(
      RpcController controller, ClientNamenodeProtocolProtos.GetSnapshotListingRequestProto request)
      throws ServiceException {
    try {
      SnapshotStatus[] result = server
          .getSnapshotListing(request.getSnapshotRoot());
      if (result != null) {
        return ClientNamenodeProtocolProtos.GetSnapshotListingResponseProto.newBuilder().
            setSnapshotList(PBHelperClient.convert(result)).build();
      } else {
        return NULL_GET_SNAPSHOT_LISTING_RESPONSE;
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto getSnapshotDiffReport(
      RpcController controller, ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto request)
      throws ServiceException {
    try {
      SnapshotDiffReport report = server.getSnapshotDiffReport(
          request.getSnapshotRoot(), request.getFromSnapshot(),
          request.getToSnapshot());
      return ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto.newBuilder()
          .setDiffReport(PBHelperClient.convert(report)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingResponseProto getSnapshotDiffReportListing(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingRequestProto request)
      throws ServiceException {
    try {
      SnapshotDiffReportListing report = server
          .getSnapshotDiffReportListing(request.getSnapshotRoot(),
              request.getFromSnapshot(), request.getToSnapshot(),
              request.getCursor().getStartPath().toByteArray(),
              request.getCursor().getIndex());
      //request.getStartPath(), request.getIndex());
      return ClientNamenodeProtocolProtos.GetSnapshotDiffReportListingResponseProto.newBuilder()
          .setDiffReport(PBHelperClient.convert(report)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.IsFileClosedResponseProto isFileClosed(
      RpcController controller, ClientNamenodeProtocolProtos.IsFileClosedRequestProto request)
      throws ServiceException {
    try {
      boolean result = server.isFileClosed(request.getSrc());
      return ClientNamenodeProtocolProtos.IsFileClosedResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto addCacheDirective(
      RpcController controller, ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto request)
      throws ServiceException {
    try {
      long id = server.addCacheDirective(
          PBHelperClient.convert(request.getInfo()),
          PBHelperClient.convertCacheFlags(request.getCacheFlags()));
      return ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto.newBuilder().
          setId(id).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.ModifyCacheDirectiveResponseProto modifyCacheDirective(
      RpcController controller, ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto request)
      throws ServiceException {
    try {
      server.modifyCacheDirective(
          PBHelperClient.convert(request.getInfo()),
          PBHelperClient.convertCacheFlags(request.getCacheFlags()));
      return ClientNamenodeProtocolProtos.ModifyCacheDirectiveResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RemoveCacheDirectiveResponseProto
  removeCacheDirective(RpcController controller,
                       ClientNamenodeProtocolProtos.RemoveCacheDirectiveRequestProto request)
      throws ServiceException {
    try {
      server.removeCacheDirective(request.getId());
      return ClientNamenodeProtocolProtos.RemoveCacheDirectiveResponseProto.
          newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto listCacheDirectives(
      RpcController controller, ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto request)
      throws ServiceException {
    try {
      CacheDirectiveInfo filter =
          PBHelperClient.convert(request.getFilter());
      BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> entries =
          server.listCacheDirectives(request.getPrevId(), filter);
      ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto.newBuilder();
      builder.setHasMore(entries.hasMore());
      for (int i=0, n=entries.size(); i<n; i++) {
        builder.addElements(PBHelperClient.convert(entries.get(i)));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.AddCachePoolResponseProto addCachePool(RpcController controller,
                                                                             ClientNamenodeProtocolProtos.AddCachePoolRequestProto request) throws ServiceException {
    try {
      server.addCachePool(PBHelperClient.convert(request.getInfo()));
      return ClientNamenodeProtocolProtos.AddCachePoolResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.ModifyCachePoolResponseProto modifyCachePool(RpcController controller,
                                                                                   ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto request) throws ServiceException {
    try {
      server.modifyCachePool(PBHelperClient.convert(request.getInfo()));
      return ClientNamenodeProtocolProtos.ModifyCachePoolResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RemoveCachePoolResponseProto removeCachePool(RpcController controller,
                                                                                   ClientNamenodeProtocolProtos.RemoveCachePoolRequestProto request) throws ServiceException {
    try {
      server.removeCachePool(request.getPoolName());
      return ClientNamenodeProtocolProtos.RemoveCachePoolResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.ListCachePoolsResponseProto listCachePools(RpcController controller,
                                                                                 ClientNamenodeProtocolProtos.ListCachePoolsRequestProto request) throws ServiceException {
    try {
      BatchedRemoteIterator.BatchedEntries<CachePoolEntry> entries =
          server.listCachePools(request.getPrevPoolName());
      ClientNamenodeProtocolProtos.ListCachePoolsResponseProto.Builder responseBuilder =
          ClientNamenodeProtocolProtos.ListCachePoolsResponseProto.newBuilder();
      responseBuilder.setHasMore(entries.hasMore());
      for (int i=0, n=entries.size(); i<n; i++) {
        responseBuilder.addEntries(PBHelperClient.convert(entries.get(i)));
      }
      return responseBuilder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public AclProtos.ModifyAclEntriesResponseProto modifyAclEntries(
      RpcController controller, AclProtos.ModifyAclEntriesRequestProto req)
      throws ServiceException {
    try {
      server.modifyAclEntries(req.getSrc(), PBHelperClient.convertAclEntry(req.getAclSpecList()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_MODIFYACLENTRIES_RESPONSE;
  }

  @Override
  public AclProtos.RemoveAclEntriesResponseProto removeAclEntries(
      RpcController controller, AclProtos.RemoveAclEntriesRequestProto req)
      throws ServiceException {
    try {
      server.removeAclEntries(req.getSrc(),
          PBHelperClient.convertAclEntry(req.getAclSpecList()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REMOVEACLENTRIES_RESPONSE;
  }

  @Override
  public AclProtos.RemoveDefaultAclResponseProto removeDefaultAcl(
      RpcController controller, AclProtos.RemoveDefaultAclRequestProto req)
      throws ServiceException {
    try {
      server.removeDefaultAcl(req.getSrc());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REMOVEDEFAULTACL_RESPONSE;
  }

  @Override
  public AclProtos.RemoveAclResponseProto removeAcl(RpcController controller,
                                                    AclProtos.RemoveAclRequestProto req) throws ServiceException {
    try {
      server.removeAcl(req.getSrc());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REMOVEACL_RESPONSE;
  }

  @Override
  public AclProtos.SetAclResponseProto setAcl(RpcController controller,
                                              AclProtos.SetAclRequestProto req) throws ServiceException {
    try {
      server.setAcl(req.getSrc(), PBHelperClient.convertAclEntry(req.getAclSpecList()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SETACL_RESPONSE;
  }

  @Override
  public AclProtos.GetAclStatusResponseProto getAclStatus(RpcController controller,
                                                          AclProtos.GetAclStatusRequestProto req) throws ServiceException {
    try {
      return PBHelperClient.convert(server.getAclStatus(req.getSrc()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public EncryptionZonesProtos.CreateEncryptionZoneResponseProto createEncryptionZone(
      RpcController controller, EncryptionZonesProtos.CreateEncryptionZoneRequestProto req)
      throws ServiceException {
    try {
      server.createEncryptionZone(req.getSrc(), req.getKeyName());
      return EncryptionZonesProtos.CreateEncryptionZoneResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public EncryptionZonesProtos.GetEZForPathResponseProto getEZForPath(
      RpcController controller, EncryptionZonesProtos.GetEZForPathRequestProto req)
      throws ServiceException {
    try {
      EncryptionZonesProtos.GetEZForPathResponseProto.Builder builder =
          EncryptionZonesProtos.GetEZForPathResponseProto.newBuilder();
      final EncryptionZone ret = server.getEZForPath(req.getSrc());
      if (ret != null) {
        builder.setZone(PBHelperClient.convert(ret));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public EncryptionZonesProtos.ListEncryptionZonesResponseProto listEncryptionZones(
      RpcController controller, EncryptionZonesProtos.ListEncryptionZonesRequestProto req)
      throws ServiceException {
    try {
      BatchedRemoteIterator.BatchedEntries<EncryptionZone> entries = server
          .listEncryptionZones(req.getId());
      EncryptionZonesProtos.ListEncryptionZonesResponseProto.Builder builder =
          EncryptionZonesProtos.ListEncryptionZonesResponseProto.newBuilder();
      builder.setHasMore(entries.hasMore());
      for (int i=0; i<entries.size(); i++) {
        builder.addZones(PBHelperClient.convert(entries.get(i)));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public EncryptionZonesProtos.ReencryptEncryptionZoneResponseProto reencryptEncryptionZone(
      RpcController controller, EncryptionZonesProtos.ReencryptEncryptionZoneRequestProto req)
      throws ServiceException {
    try {
      server.reencryptEncryptionZone(req.getZone(),
          PBHelperClient.convert(req.getAction()));
      return EncryptionZonesProtos.ReencryptEncryptionZoneResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  public EncryptionZonesProtos.ListReencryptionStatusResponseProto listReencryptionStatus(
      RpcController controller, EncryptionZonesProtos.ListReencryptionStatusRequestProto req)
      throws ServiceException {
    try {
      BatchedRemoteIterator.BatchedEntries<ZoneReencryptionStatus> entries = server
          .listReencryptionStatus(req.getId());
      EncryptionZonesProtos.ListReencryptionStatusResponseProto.Builder builder =
          EncryptionZonesProtos.ListReencryptionStatusResponseProto.newBuilder();
      builder.setHasMore(entries.hasMore());
      for (int i=0; i<entries.size(); i++) {
        builder.addStatuses(PBHelperClient.convert(entries.get(i)));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ErasureCodingProtos.SetErasureCodingPolicyResponseProto setErasureCodingPolicy(
      RpcController controller, ErasureCodingProtos.SetErasureCodingPolicyRequestProto req)
      throws ServiceException {
    try {
      String ecPolicyName = req.hasEcPolicyName() ?
          req.getEcPolicyName() : null;
      server.setErasureCodingPolicy(req.getSrc(), ecPolicyName);
      return ErasureCodingProtos.SetErasureCodingPolicyResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ErasureCodingProtos.UnsetErasureCodingPolicyResponseProto unsetErasureCodingPolicy(
      RpcController controller, ErasureCodingProtos.UnsetErasureCodingPolicyRequestProto req)
      throws ServiceException {
    try {
      server.unsetErasureCodingPolicy(req.getSrc());
      return ErasureCodingProtos.UnsetErasureCodingPolicyResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ErasureCodingProtos.GetECTopologyResultForPoliciesResponseProto getECTopologyResultForPolicies(
      RpcController controller, ErasureCodingProtos.GetECTopologyResultForPoliciesRequestProto req)
      throws ServiceException {
    try {
      ProtocolStringList policies = req.getPoliciesList();
      ECTopologyVerifierResult result = server.getECTopologyResultForPolicies(
          policies.toArray(policies.toArray(new String[policies.size()])));
      ErasureCodingProtos.GetECTopologyResultForPoliciesResponseProto.Builder builder =
          ErasureCodingProtos.GetECTopologyResultForPoliciesResponseProto.newBuilder();
      builder
          .setResponse(PBHelperClient.convertECTopologyVerifierResult(result));
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public XAttrProtos.SetXAttrResponseProto setXAttr(RpcController controller,
                                                    XAttrProtos.SetXAttrRequestProto req) throws ServiceException {
    try {
      server.setXAttr(req.getSrc(), PBHelperClient.convertXAttr(req.getXAttr()),
          PBHelperClient.convert(req.getFlag()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SETXATTR_RESPONSE;
  }

  @Override
  public XAttrProtos.GetXAttrsResponseProto getXAttrs(RpcController controller,
                                                      XAttrProtos.GetXAttrsRequestProto req) throws ServiceException {
    try {
      return PBHelperClient.convertXAttrsResponse(server.getXAttrs(req.getSrc(),
          PBHelperClient.convertXAttrs(req.getXAttrsList())));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public XAttrProtos.ListXAttrsResponseProto listXAttrs(RpcController controller,
                                                        XAttrProtos.ListXAttrsRequestProto req) throws ServiceException {
    try {
      return PBHelperClient.convertListXAttrsResponse(server.listXAttrs(req.getSrc()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public XAttrProtos.RemoveXAttrResponseProto removeXAttr(RpcController controller,
                                                          XAttrProtos.RemoveXAttrRequestProto req) throws ServiceException {
    try {
      server.removeXAttr(req.getSrc(), PBHelperClient.convertXAttr(req.getXAttr()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REMOVEXATTR_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.CheckAccessResponseProto checkAccess(RpcController controller,
                                                                           ClientNamenodeProtocolProtos.CheckAccessRequestProto req) throws ServiceException {
    try {
      server.checkAccess(req.getPath(), PBHelperClient.convert(req.getMode()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_CHECKACCESS_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.SetStoragePolicyResponseProto setStoragePolicy(
      RpcController controller, ClientNamenodeProtocolProtos.SetStoragePolicyRequestProto request)
      throws ServiceException {
    try {
      server.setStoragePolicy(request.getSrc(), request.getPolicyName());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SET_STORAGE_POLICY_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.UnsetStoragePolicyResponseProto unsetStoragePolicy(
      RpcController controller, ClientNamenodeProtocolProtos.UnsetStoragePolicyRequestProto request)
      throws ServiceException {
    try {
      server.unsetStoragePolicy(request.getSrc());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_UNSET_STORAGE_POLICY_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetStoragePolicyResponseProto getStoragePolicy(
      RpcController controller, ClientNamenodeProtocolProtos.GetStoragePolicyRequestProto request)
      throws ServiceException {
    try {
      HdfsProtos.BlockStoragePolicyProto policy = PBHelperClient.convert(server
          .getStoragePolicy(request.getPath()));
      return ClientNamenodeProtocolProtos.GetStoragePolicyResponseProto.newBuilder()
          .setStoragePolicy(policy).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto getStoragePolicies(
      RpcController controller, ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto request)
      throws ServiceException {
    try {
      BlockStoragePolicy[] policies = server.getStoragePolicies();
      ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto.newBuilder();
      if (policies == null) {
        return builder.build();
      }
      for (BlockStoragePolicy policy : policies) {
        builder.addPolicies(PBHelperClient.convert(policy));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  public ClientNamenodeProtocolProtos.GetCurrentEditLogTxidResponseProto getCurrentEditLogTxid(RpcController controller,
                                                                                               ClientNamenodeProtocolProtos.GetCurrentEditLogTxidRequestProto req) throws ServiceException {
    try {
      return ClientNamenodeProtocolProtos.GetCurrentEditLogTxidResponseProto.newBuilder().setTxid(
          server.getCurrentEditLogTxid()).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetEditsFromTxidResponseProto getEditsFromTxid(RpcController controller,
                                                                                     ClientNamenodeProtocolProtos.GetEditsFromTxidRequestProto req) throws ServiceException {
    try {
      return PBHelperClient.convertEditsResponse(server.getEditsFromTxid(
          req.getTxid()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ErasureCodingProtos.GetErasureCodingPoliciesResponseProto getErasureCodingPolicies(RpcController controller,
                                                                                            ErasureCodingProtos.GetErasureCodingPoliciesRequestProto request) throws ServiceException {
    try {
      ErasureCodingPolicyInfo[] ecpInfos = server.getErasureCodingPolicies();
      ErasureCodingProtos.GetErasureCodingPoliciesResponseProto.Builder resBuilder = ErasureCodingProtos.GetErasureCodingPoliciesResponseProto
          .newBuilder();
      for (ErasureCodingPolicyInfo info : ecpInfos) {
        resBuilder.addEcPolicies(
            PBHelperClient.convertErasureCodingPolicy(info));
      }
      return resBuilder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ErasureCodingProtos.GetErasureCodingCodecsResponseProto getErasureCodingCodecs(
      RpcController controller, ErasureCodingProtos.GetErasureCodingCodecsRequestProto request)
      throws ServiceException {
    try {
      Map<String, String> codecs = server.getErasureCodingCodecs();
      ErasureCodingProtos.GetErasureCodingCodecsResponseProto.Builder resBuilder =
          ErasureCodingProtos.GetErasureCodingCodecsResponseProto.newBuilder();
      for (Map.Entry<String, String> codec : codecs.entrySet()) {
        resBuilder.addCodec(
            PBHelperClient.convertErasureCodingCodec(
                codec.getKey(), codec.getValue()));
      }
      return resBuilder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ErasureCodingProtos.AddErasureCodingPoliciesResponseProto addErasureCodingPolicies(
      RpcController controller, ErasureCodingProtos.AddErasureCodingPoliciesRequestProto request)
      throws ServiceException {
    try {
      ErasureCodingPolicy[] policies = request.getEcPoliciesList().stream()
          .map(PBHelperClient::convertErasureCodingPolicy)
          .toArray(ErasureCodingPolicy[]::new);
      AddErasureCodingPolicyResponse[] result = server
          .addErasureCodingPolicies(policies);

      List<HdfsProtos.AddErasureCodingPolicyResponseProto> responseProtos =
          Arrays.stream(result)
              .map(PBHelperClient::convertAddErasureCodingPolicyResponse)
              .collect(Collectors.toList());
      ErasureCodingProtos.AddErasureCodingPoliciesResponseProto response =
          ErasureCodingProtos.AddErasureCodingPoliciesResponseProto.newBuilder()
              .addAllResponses(responseProtos).build();
      return response;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ErasureCodingProtos.RemoveErasureCodingPolicyResponseProto removeErasureCodingPolicy(
      RpcController controller, ErasureCodingProtos.RemoveErasureCodingPolicyRequestProto request)
      throws ServiceException {
    try {
      server.removeErasureCodingPolicy(request.getEcPolicyName());
      return ErasureCodingProtos.RemoveErasureCodingPolicyResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ErasureCodingProtos.EnableErasureCodingPolicyResponseProto enableErasureCodingPolicy(
      RpcController controller, ErasureCodingProtos.EnableErasureCodingPolicyRequestProto request)
      throws ServiceException {
    try {
      server.enableErasureCodingPolicy(request.getEcPolicyName());
      return ErasureCodingProtos.EnableErasureCodingPolicyResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ErasureCodingProtos.DisableErasureCodingPolicyResponseProto disableErasureCodingPolicy(
      RpcController controller, ErasureCodingProtos.DisableErasureCodingPolicyRequestProto request)
      throws ServiceException {
    try {
      server.disableErasureCodingPolicy(request.getEcPolicyName());
      return ErasureCodingProtos.DisableErasureCodingPolicyResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ErasureCodingProtos.GetErasureCodingPolicyResponseProto getErasureCodingPolicy(RpcController controller,
                                                                                        ErasureCodingProtos.GetErasureCodingPolicyRequestProto request) throws ServiceException {
    try {
      ErasureCodingPolicy ecPolicy = server.getErasureCodingPolicy(request.getSrc());
      ErasureCodingProtos.GetErasureCodingPolicyResponseProto.Builder builder = ErasureCodingProtos.GetErasureCodingPolicyResponseProto.newBuilder();
      if (ecPolicy != null) {
        builder.setEcPolicy(PBHelperClient.convertErasureCodingPolicy(ecPolicy));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetQuotaUsageResponseProto getQuotaUsage(
      RpcController controller, ClientNamenodeProtocolProtos.GetQuotaUsageRequestProto req)
      throws ServiceException {
    try {
      QuotaUsage result = server.getQuotaUsage(req.getPath());
      return ClientNamenodeProtocolProtos.GetQuotaUsageResponseProto.newBuilder()
          .setUsage(PBHelperClient.convert(result)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.ListOpenFilesResponseProto listOpenFiles(RpcController controller,
                                                                               ClientNamenodeProtocolProtos.ListOpenFilesRequestProto req) throws ServiceException {
    try {
      EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes =
          PBHelperClient.convertOpenFileTypes(req.getTypesList());
      BatchedRemoteIterator.BatchedEntries<OpenFileEntry> entries = server.listOpenFiles(req.getId(),
          openFilesTypes, req.getPath());
      ClientNamenodeProtocolProtos.ListOpenFilesResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.ListOpenFilesResponseProto.newBuilder();
      builder.setHasMore(entries.hasMore());
      for (int i = 0; i < entries.size(); i++) {
        builder.addEntries(PBHelperClient.convert(entries.get(i)));
      }
      builder.addAllTypes(req.getTypesList());
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.MsyncResponseProto msync(RpcController controller,
                                                               ClientNamenodeProtocolProtos.MsyncRequestProto req) throws ServiceException {
    try {
      server.msync();
      return ClientNamenodeProtocolProtos.MsyncResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.SatisfyStoragePolicyResponseProto satisfyStoragePolicy(
      RpcController controller,
      ClientNamenodeProtocolProtos.SatisfyStoragePolicyRequestProto request) throws ServiceException {
    try {
      server.satisfyStoragePolicy(request.getSrc());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SATISFYSTORAGEPOLICY_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.HAServiceStateResponseProto getHAServiceState(
      RpcController controller,
      ClientNamenodeProtocolProtos.HAServiceStateRequestProto request) throws ServiceException {
    try {
      HAServiceProtocol.HAServiceState state = server.getHAServiceState();
      HAServiceProtocolProtos.HAServiceStateProto retState;
      switch (state) {
        case ACTIVE:
          retState = HAServiceProtocolProtos.HAServiceStateProto.ACTIVE;
          break;
        case STANDBY:
          retState = HAServiceProtocolProtos.HAServiceStateProto.STANDBY;
          break;
        case OBSERVER:
          retState = HAServiceProtocolProtos.HAServiceStateProto.OBSERVER;
          break;
        case INITIALIZING:
        default:
          retState = HAServiceProtocolProtos.HAServiceStateProto.INITIALIZING;
          break;
      }
      ClientNamenodeProtocolProtos.HAServiceStateResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.HAServiceStateResponseProto.newBuilder();
      builder.setState(retState);
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetSlowDatanodeReportResponseProto getSlowDatanodeReport(RpcController controller,
                                                                                               ClientNamenodeProtocolProtos.GetSlowDatanodeReportRequestProto request) throws ServiceException {
    try {
      List<? extends HdfsProtos.DatanodeInfoProto> result =
          PBHelperClient.convert(server.getSlowDatanodeReport());
      return ClientNamenodeProtocolProtos.GetSlowDatanodeReportResponseProto.newBuilder()
          .addAllDatanodeInfoProto(result)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetEnclosingRootResponseProto getEnclosingRoot(
      RpcController controller, ClientNamenodeProtocolProtos.GetEnclosingRootRequestProto req)
      throws ServiceException {
    try {
      Path enclosingRootPath = server.getEnclosingRoot(req.getFilename());
      return ClientNamenodeProtocolProtos.GetEnclosingRootResponseProto.newBuilder()
          .setEnclosingRootPath(enclosingRootPath.toUri().toString())
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
