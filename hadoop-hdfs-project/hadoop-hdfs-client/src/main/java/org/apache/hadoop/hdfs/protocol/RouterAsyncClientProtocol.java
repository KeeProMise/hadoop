package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;


import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface RouterAsyncClientProtocol{
  CompletableFuture<LocatedBlocks> getBlockLocations(String src, long offset, long length);

  CompletableFuture<FsServerDefaults> getServerDefaults();

  @SuppressWarnings("checkstyle:ParameterNumber")
  CompletableFuture<HdfsFileStatus> create(
      String src, FsPermission masked, String clientName,
      EnumSetWritable<CreateFlag> flag,
      boolean createParent, short replication,
      long blockSize, CryptoProtocolVersion[] supportedVersions,
      String ecPolicyName, String storagePolicy);


  CompletableFuture<LastBlockWithStatus> append(
      String src, String clientName, EnumSetWritable<CreateFlag> flag);

  CompletableFuture<Boolean> setReplication(String src, short replication);

  CompletableFuture<BlockStoragePolicy[]> getStoragePolicies();

  CompletableFuture<Object> setStoragePolicy(String src, String policyName);

  CompletableFuture<Object> unsetStoragePolicy(String src);

  CompletableFuture<BlockStoragePolicy> getStoragePolicy(String path);

  CompletableFuture<Object> setPermission(String src, FsPermission permission);

  CompletableFuture<Object> setOwner(String src, String username, String groupname);

  CompletableFuture<Object> abandonBlock(ExtendedBlock b, long fileId, String src, String holder);

  CompletableFuture<LocatedBlock> addBlock(
      String src, String clientName, ExtendedBlock previous,
      DatanodeInfo[] excludeNodes, long fileId,
      String[] favoredNodes, EnumSet<AddBlockFlag> addBlockFlags);

  @SuppressWarnings({"checkstyle:ParameterNumber", "checkstyle:RedundantModifier"})
  CompletableFuture<LocatedBlock> getAdditionalDatanode(
      final String src, final long fileId, final ExtendedBlock blk,
      final DatanodeInfo[] existings, final String[] existingStorageIDs,
      final DatanodeInfo[] excludes, final int numAdditionalNodes, final String clientName);

  CompletableFuture<Boolean> complete(String src, String clientName, ExtendedBlock last, long fileId);

  CompletableFuture<Object> reportBadBlocks(LocatedBlock[] blocks);

  CompletableFuture<Boolean> rename(String src, String dst);

  CompletableFuture<Object> concat(String trg, String[] srcs);

  CompletableFuture<Object> rename2(String src, String dst, Options.Rename... options);

  CompletableFuture<Boolean> truncate(String src, long newLength, String clientName);

  CompletableFuture<Boolean> delete(String src, boolean recursive);

  CompletableFuture<Boolean> mkdirs(String src, FsPermission masked, boolean createParent);

  CompletableFuture<DirectoryListing> getListing(String src, byte[] startAfter, boolean needLocation);

  CompletableFuture<BatchedDirectoryListing> getBatchedListing(String[] srcs, byte[] startAfter, boolean needLocation);

  CompletableFuture<SnapshottableDirectoryStatus[]> getSnapshottableDirListing();

  CompletableFuture<SnapshotStatus[]> getSnapshotListing(String snapshotRoot);

  CompletableFuture<Object> renewLease(String clientName, List<String> namespaces);

  CompletableFuture<Boolean> recoverLease(String src, String clientName);

  CompletableFuture<long[]> getStats();

  CompletableFuture<ReplicatedBlockStats> getReplicatedBlockStats();

  CompletableFuture<ECBlockGroupStats> getECBlockGroupStats();

  CompletableFuture<DatanodeInfo[]> getDatanodeReport(HdfsConstants.DatanodeReportType type);

  CompletableFuture<DatanodeStorageReport[]> getDatanodeStorageReport(HdfsConstants.DatanodeReportType type);

  CompletableFuture<Long> getPreferredBlockSize(String filename);

  CompletableFuture<Boolean> setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked);

  CompletableFuture<Boolean> saveNamespace(long timeWindow, long txGap);

  CompletableFuture<Long> rollEdits();

  CompletableFuture<Boolean> restoreFailedStorage(String arg);

  CompletableFuture<Object> refreshNodes();

  CompletableFuture<Object> finalizeUpgrade();

  CompletableFuture<Boolean> upgradeStatus();

  CompletableFuture<RollingUpgradeInfo> rollingUpgrade(HdfsConstants.RollingUpgradeAction action);

  CompletableFuture<CorruptFileBlocks> listCorruptFileBlocks(String path, String cookie);

  CompletableFuture<Object> metaSave(String filename);

  CompletableFuture<Object> setBalancerBandwidth(long bandwidth);

  CompletableFuture<HdfsFileStatus> getFileInfo(String src);

  CompletableFuture<Boolean> isFileClosed(String src);

  CompletableFuture<HdfsFileStatus> getFileLinkInfo(String src);

  CompletableFuture<HdfsLocatedFileStatus> getLocatedFileInfo(String src, boolean needBlockToken);

  CompletableFuture<ContentSummary> getContentSummary(String path);

  CompletableFuture<Object> setQuota(
      String path, long namespaceQuota, long storagespaceQuota, StorageType type);

  CompletableFuture<Object>fsync(String src, long inodeId, String client, long lastBlockLength);

  CompletableFuture<Object> setTimes(String src, long mtime, long atime);

  CompletableFuture<Object> createSymlink(String target, String link, FsPermission dirPerm, boolean createParent);

  CompletableFuture<String> getLinkTarget(String path);

  CompletableFuture<LocatedBlock> updateBlockForPipeline(ExtendedBlock block, String clientName);

  CompletableFuture<Object> updatePipeline(
      String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs);

  CompletableFuture<Token<DelegationTokenIdentifier>> getDelegationToken(Text renewer);

  CompletableFuture<Long> renewDelegationToken(Token<DelegationTokenIdentifier> token);

  CompletableFuture<Object> cancelDelegationToken(Token<DelegationTokenIdentifier> token);

  CompletableFuture<DataEncryptionKey> getDataEncryptionKey();

  CompletableFuture<String> createSnapshot(String snapshotRoot, String snapshotName);

  CompletableFuture<Object> deleteSnapshot(String snapshotRoot, String snapshotName);

  CompletableFuture<Object> renameSnapshot(String snapshotRoot, String snapshotOldName, String snapshotNewName);

  CompletableFuture<Object> allowSnapshot(String snapshotRoot);

  CompletableFuture<Object> disallowSnapshot(String snapshotRoot);

  CompletableFuture<SnapshotDiffReport> getSnapshotDiffReport(
      String snapshotRoot, String fromSnapshot, String toSnapshot);

  CompletableFuture<SnapshotDiffReportListing> getSnapshotDiffReportListing(
      String snapshotRoot, String fromSnapshot, String toSnapshot, byte[] startPath, int index);

  CompletableFuture<Long> addCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags);

  CompletableFuture<Object> modifyCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags);

  CompletableFuture<Object> removeCacheDirective(long id);

  CompletableFuture<BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry>> listCacheDirectives(
      long prevId, CacheDirectiveInfo filter);

  CompletableFuture<Object> addCachePool(CachePoolInfo info);

  CompletableFuture<Object> modifyCachePool(CachePoolInfo req);

  CompletableFuture<Object> removeCachePool(String pool);

  CompletableFuture<BatchedRemoteIterator.BatchedEntries<CachePoolEntry>> listCachePools(String prevPool);

  CompletableFuture<Object> modifyAclEntries(String src, List<AclEntry> aclSpec);

  CompletableFuture<Object> removeAclEntries(String src, List<AclEntry> aclSpec);

  CompletableFuture<Object> removeDefaultAcl(String src);

  CompletableFuture<Object> removeAcl(String src);

  CompletableFuture<Object> setAcl(String src, List<AclEntry> aclSpec);

  CompletableFuture<AclStatus> getAclStatus(String src);

  CompletableFuture<Object>createEncryptionZone(String src, String keyName);

  CompletableFuture<EncryptionZone> getEZForPath(String src);

  CompletableFuture<BatchedRemoteIterator.BatchedEntries<EncryptionZone>> listEncryptionZones(long prevId);

  CompletableFuture<Object> reencryptEncryptionZone(String zone, HdfsConstants.ReencryptAction action);

  CompletableFuture<BatchedRemoteIterator.BatchedEntries<ZoneReencryptionStatus>> listReencryptionStatus(long prevId);

  CompletableFuture<Object> setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag);

  CompletableFuture<List<XAttr>> getXAttrs(String src, List<XAttr> xAttrs);

  CompletableFuture<List<XAttr>> listXAttrs(String src);

  CompletableFuture<Object> removeXAttr(String src, XAttr xAttr);

  CompletableFuture<Object> checkAccess(String path, FsAction mode);

  CompletableFuture<Long> getCurrentEditLogTxid();

  CompletableFuture<EventBatchList> getEditsFromTxid(long txid);

  CompletableFuture<Object> setErasureCodingPolicy(String src, String ecPolicyName);

  CompletableFuture<AddErasureCodingPolicyResponse[]> addErasureCodingPolicies(ErasureCodingPolicy[] policies);

  CompletableFuture<Object> removeErasureCodingPolicy(String ecPolicyName);

  CompletableFuture<Object> enableErasureCodingPolicy(String ecPolicyName);

  CompletableFuture<Object> disableErasureCodingPolicy(String ecPolicyName);

  CompletableFuture<ErasureCodingPolicyInfo[]> getErasureCodingPolicies();

  CompletableFuture<Map<String, String>> getErasureCodingCodecs();

  CompletableFuture<ErasureCodingPolicy> getErasureCodingPolicy(String src);

  CompletableFuture<Object> unsetErasureCodingPolicy(String src);

  CompletableFuture<ECTopologyVerifierResult> getECTopologyResultForPolicies(String... policyNames);

  CompletableFuture<QuotaUsage> getQuotaUsage(String path);

  CompletableFuture<BatchedRemoteIterator.BatchedEntries<OpenFileEntry>> listOpenFiles(long prevId);

  CompletableFuture<BatchedRemoteIterator.BatchedEntries<OpenFileEntry>> listOpenFiles(
      long prevId, EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes, String path);

  CompletableFuture<HAServiceProtocol.HAServiceState> getHAServiceState();

  CompletableFuture<Object> msync();

  CompletableFuture<Object> satisfyStoragePolicy(String path);

  CompletableFuture<DatanodeInfo[]> getSlowDatanodeReport();

  CompletableFuture<Path> getEnclosingRoot(String src);
}
