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
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
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

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

public class RouterAsyncClientProtocolTranslatorPB implements RouterAsyncClientProtocol{
  final private ClientNamenodeProtocolProtos.ClientNamenodeProtocol.Interface rpcProxy;

  public RouterAsyncClientProtocolTranslatorPB(
      ClientNamenodeProtocolProtos.ClientNamenodeProtocol.Interface rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public CompletableFuture<LocatedBlocks> getBlockLocations(String src, long offset, long length) {
    ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto req = ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto
        .newBuilder()
        .setSrc(src)
        .setOffset(offset)
        .setLength(length)
        .build();
    ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto resp = ipc(() -> rpcProxy.getBlockLocations(null,
        req));
    return resp.hasLocations() ?
        PBHelperClient.convert(resp.getLocations()) : null;
  }

  @Override
  public CompletableFuture<FsServerDefaults> getServerDefaults() {
    return null;
  }

  @Override
  public CompletableFuture<HdfsFileStatus> create(String src, FsPermission masked, String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent, short replication, long blockSize, CryptoProtocolVersion[] supportedVersions, String ecPolicyName, String storagePolicy) {
    return null;
  }

  @Override
  public CompletableFuture<LastBlockWithStatus> append(String src, String clientName, EnumSetWritable<CreateFlag> flag) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> setReplication(String src, short replication) {
    return null;
  }

  @Override
  public CompletableFuture<BlockStoragePolicy[]> getStoragePolicies() {
    return null;
  }

  @Override
  public CompletableFuture<Object> setStoragePolicy(String src, String policyName) {
    return null;
  }

  @Override
  public CompletableFuture<Object> unsetStoragePolicy(String src) {
    return null;
  }

  @Override
  public CompletableFuture<BlockStoragePolicy> getStoragePolicy(String path) {
    return null;
  }

  @Override
  public CompletableFuture<Object> setPermission(String src, FsPermission permission) {
    return null;
  }

  @Override
  public CompletableFuture<Object> setOwner(String src, String username, String groupname) {
    return null;
  }

  @Override
  public CompletableFuture<Object> abandonBlock(ExtendedBlock b, long fileId, String src, String holder) {
    return null;
  }

  @Override
  public CompletableFuture<LocatedBlock> addBlock(String src, String clientName, ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes, EnumSet<AddBlockFlag> addBlockFlags) {
    return null;
  }

  @Override
  public CompletableFuture<LocatedBlock> getAdditionalDatanode(String src, long fileId, ExtendedBlock blk, DatanodeInfo[] existings, String[] existingStorageIDs, DatanodeInfo[] excludes, int numAdditionalNodes, String clientName) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> complete(String src, String clientName, ExtendedBlock last, long fileId) {
    return null;
  }

  @Override
  public CompletableFuture<Object> reportBadBlocks(LocatedBlock[] blocks) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> rename(String src, String dst) {
    return null;
  }

  @Override
  public CompletableFuture<Object> concat(String trg, String[] srcs) {
    return null;
  }

  @Override
  public CompletableFuture<Object> rename2(String src, String dst, Options.Rename... options) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> truncate(String src, long newLength, String clientName) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> delete(String src, boolean recursive) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> mkdirs(String src, FsPermission masked, boolean createParent) {
    return null;
  }

  @Override
  public CompletableFuture<DirectoryListing> getListing(String src, byte[] startAfter, boolean needLocation) {
    return null;
  }

  @Override
  public CompletableFuture<BatchedDirectoryListing> getBatchedListing(String[] srcs, byte[] startAfter, boolean needLocation) {
    return null;
  }

  @Override
  public CompletableFuture<SnapshottableDirectoryStatus[]> getSnapshottableDirListing() {
    return null;
  }

  @Override
  public CompletableFuture<SnapshotStatus[]> getSnapshotListing(String snapshotRoot) {
    return null;
  }

  @Override
  public CompletableFuture<Object> renewLease(String clientName, List<String> namespaces) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> recoverLease(String src, String clientName) {
    return null;
  }

  @Override
  public CompletableFuture<long[]> getStats() {
    return null;
  }

  @Override
  public CompletableFuture<ReplicatedBlockStats> getReplicatedBlockStats() {
    return null;
  }

  @Override
  public CompletableFuture<ECBlockGroupStats> getECBlockGroupStats() {
    return null;
  }

  @Override
  public CompletableFuture<DatanodeInfo[]> getDatanodeReport(HdfsConstants.DatanodeReportType type) {
    return null;
  }

  @Override
  public CompletableFuture<DatanodeStorageReport[]> getDatanodeStorageReport(HdfsConstants.DatanodeReportType type) {
    return null;
  }

  @Override
  public CompletableFuture<Long> getPreferredBlockSize(String filename) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> saveNamespace(long timeWindow, long txGap) {
    return null;
  }

  @Override
  public CompletableFuture<Long> rollEdits() {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> restoreFailedStorage(String arg) {
    return null;
  }

  @Override
  public CompletableFuture<Object> refreshNodes() {
    return null;
  }

  @Override
  public CompletableFuture<Object> finalizeUpgrade() {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> upgradeStatus() {
    return null;
  }

  @Override
  public CompletableFuture<RollingUpgradeInfo> rollingUpgrade(HdfsConstants.RollingUpgradeAction action) {
    return null;
  }

  @Override
  public CompletableFuture<CorruptFileBlocks> listCorruptFileBlocks(String path, String cookie) {
    return null;
  }

  @Override
  public CompletableFuture<Object> metaSave(String filename) {
    return null;
  }

  @Override
  public CompletableFuture<Object> setBalancerBandwidth(long bandwidth) {
    return null;
  }

  @Override
  public CompletableFuture<HdfsFileStatus> getFileInfo(String src) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> isFileClosed(String src) {
    return null;
  }

  @Override
  public CompletableFuture<HdfsFileStatus> getFileLinkInfo(String src) {
    return null;
  }

  @Override
  public CompletableFuture<HdfsLocatedFileStatus> getLocatedFileInfo(String src, boolean needBlockToken) {
    return null;
  }

  @Override
  public CompletableFuture<ContentSummary> getContentSummary(String path) {
    return null;
  }

  @Override
  public CompletableFuture<Object> setQuota(String path, long namespaceQuota, long storagespaceQuota, StorageType type) {
    return null;
  }

  @Override
  public CompletableFuture<Object> fsync(String src, long inodeId, String client, long lastBlockLength) {
    return null;
  }

  @Override
  public CompletableFuture<Object> setTimes(String src, long mtime, long atime) {
    return null;
  }

  @Override
  public CompletableFuture<Object> createSymlink(String target, String link, FsPermission dirPerm, boolean createParent) {
    return null;
  }

  @Override
  public CompletableFuture<String> getLinkTarget(String path) {
    return null;
  }

  @Override
  public CompletableFuture<LocatedBlock> updateBlockForPipeline(ExtendedBlock block, String clientName) {
    return null;
  }

  @Override
  public CompletableFuture<Object> updatePipeline(String clientName, ExtendedBlock oldBlock, ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs) {
    return null;
  }

  @Override
  public CompletableFuture<Token<DelegationTokenIdentifier>> getDelegationToken(Text renewer) {
    return null;
  }

  @Override
  public CompletableFuture<Long> renewDelegationToken(Token<DelegationTokenIdentifier> token) {
    return null;
  }

  @Override
  public CompletableFuture<Object> cancelDelegationToken(Token<DelegationTokenIdentifier> token) {
    return null;
  }

  @Override
  public CompletableFuture<DataEncryptionKey> getDataEncryptionKey() {
    return null;
  }

  @Override
  public CompletableFuture<String> createSnapshot(String snapshotRoot, String snapshotName) {
    return null;
  }

  @Override
  public CompletableFuture<Object> deleteSnapshot(String snapshotRoot, String snapshotName) {
    return null;
  }

  @Override
  public CompletableFuture<Object> renameSnapshot(String snapshotRoot, String snapshotOldName, String snapshotNewName) {
    return null;
  }

  @Override
  public CompletableFuture<Object> allowSnapshot(String snapshotRoot) {
    return null;
  }

  @Override
  public CompletableFuture<Object> disallowSnapshot(String snapshotRoot) {
    return null;
  }

  @Override
  public CompletableFuture<SnapshotDiffReport> getSnapshotDiffReport(String snapshotRoot, String fromSnapshot, String toSnapshot) {
    return null;
  }

  @Override
  public CompletableFuture<SnapshotDiffReportListing> getSnapshotDiffReportListing(String snapshotRoot, String fromSnapshot, String toSnapshot, byte[] startPath, int index) {
    return null;
  }

  @Override
  public CompletableFuture<Long> addCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) {
    return null;
  }

  @Override
  public CompletableFuture<Object> modifyCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) {
    return null;
  }

  @Override
  public CompletableFuture<Object> removeCacheDirective(long id) {
    return null;
  }

  @Override
  public CompletableFuture<BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry>> listCacheDirectives(long prevId, CacheDirectiveInfo filter) {
    return null;
  }

  @Override
  public CompletableFuture<Object> addCachePool(CachePoolInfo info) {
    return null;
  }

  @Override
  public CompletableFuture<Object> modifyCachePool(CachePoolInfo req) {
    return null;
  }

  @Override
  public CompletableFuture<Object> removeCachePool(String pool) {
    return null;
  }

  @Override
  public CompletableFuture<BatchedRemoteIterator.BatchedEntries<CachePoolEntry>> listCachePools(String prevPool) {
    return null;
  }

  @Override
  public CompletableFuture<Object> modifyAclEntries(String src, List<AclEntry> aclSpec) {
    return null;
  }

  @Override
  public CompletableFuture<Object> removeAclEntries(String src, List<AclEntry> aclSpec) {
    return null;
  }

  @Override
  public CompletableFuture<Object> removeDefaultAcl(String src) {
    return null;
  }

  @Override
  public CompletableFuture<Object> removeAcl(String src) {
    return null;
  }

  @Override
  public CompletableFuture<Object> setAcl(String src, List<AclEntry> aclSpec) {
    return null;
  }

  @Override
  public CompletableFuture<AclStatus> getAclStatus(String src) {
    return null;
  }

  @Override
  public CompletableFuture<Object> createEncryptionZone(String src, String keyName) {
    return null;
  }

  @Override
  public CompletableFuture<EncryptionZone> getEZForPath(String src) {
    return null;
  }

  @Override
  public CompletableFuture<BatchedRemoteIterator.BatchedEntries<EncryptionZone>> listEncryptionZones(long prevId) {
    return null;
  }

  @Override
  public CompletableFuture<Object> reencryptEncryptionZone(String zone, HdfsConstants.ReencryptAction action) {
    return null;
  }

  @Override
  public CompletableFuture<BatchedRemoteIterator.BatchedEntries<ZoneReencryptionStatus>> listReencryptionStatus(long prevId) {
    return null;
  }

  @Override
  public CompletableFuture<Object> setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag) {
    return null;
  }

  @Override
  public CompletableFuture<List<XAttr>> getXAttrs(String src, List<XAttr> xAttrs) {
    return null;
  }

  @Override
  public CompletableFuture<List<XAttr>> listXAttrs(String src) {
    return null;
  }

  @Override
  public CompletableFuture<Object> removeXAttr(String src, XAttr xAttr) {
    return null;
  }

  @Override
  public CompletableFuture<Object> checkAccess(String path, FsAction mode) {
    return null;
  }

  @Override
  public CompletableFuture<Long> getCurrentEditLogTxid() {
    return null;
  }

  @Override
  public CompletableFuture<EventBatchList> getEditsFromTxid(long txid) {
    return null;
  }

  @Override
  public CompletableFuture<Object> setErasureCodingPolicy(String src, String ecPolicyName) {
    return null;
  }

  @Override
  public CompletableFuture<AddErasureCodingPolicyResponse[]> addErasureCodingPolicies(ErasureCodingPolicy[] policies) {
    return null;
  }

  @Override
  public CompletableFuture<Object> removeErasureCodingPolicy(String ecPolicyName) {
    return null;
  }

  @Override
  public CompletableFuture<Object> enableErasureCodingPolicy(String ecPolicyName) {
    return null;
  }

  @Override
  public CompletableFuture<Object> disableErasureCodingPolicy(String ecPolicyName) {
    return null;
  }

  @Override
  public CompletableFuture<ErasureCodingPolicyInfo[]> getErasureCodingPolicies() {
    return null;
  }

  @Override
  public CompletableFuture<Map<String, String>> getErasureCodingCodecs() {
    return null;
  }

  @Override
  public CompletableFuture<ErasureCodingPolicy> getErasureCodingPolicy(String src) {
    return null;
  }

  @Override
  public CompletableFuture<Object> unsetErasureCodingPolicy(String src) {
    return null;
  }

  @Override
  public CompletableFuture<ECTopologyVerifierResult> getECTopologyResultForPolicies(String... policyNames) {
    return null;
  }

  @Override
  public CompletableFuture<QuotaUsage> getQuotaUsage(String path) {
    return null;
  }

  @Override
  public CompletableFuture<BatchedRemoteIterator.BatchedEntries<OpenFileEntry>> listOpenFiles(long prevId) {
    return null;
  }

  @Override
  public CompletableFuture<BatchedRemoteIterator.BatchedEntries<OpenFileEntry>> listOpenFiles(long prevId, EnumSet<OpenFilesIterator.OpenFilesType> openFilesTypes, String path) {
    return null;
  }

  @Override
  public CompletableFuture<HAServiceProtocol.HAServiceState> getHAServiceState() {
    return null;
  }

  @Override
  public CompletableFuture<Object> msync() {
    return null;
  }

  @Override
  public CompletableFuture<Object> satisfyStoragePolicy(String path) {
    return null;
  }

  @Override
  public CompletableFuture<DatanodeInfo[]> getSlowDatanodeReport() {
    return null;
  }

  @Override
  public CompletableFuture<Path> getEnclosingRoot(String src) {
    return null;
  }
}
