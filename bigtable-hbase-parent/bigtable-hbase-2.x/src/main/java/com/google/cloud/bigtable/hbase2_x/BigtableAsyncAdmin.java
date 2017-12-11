package com.google.cloud.bigtable.hbase2_x;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ClusterStatus.Option;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.RawAsyncTable.CoprocessorCallable;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcChannel;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.CreateTableRequest.Split;
import com.google.bigtable.admin.v2.DeleteTableRequest.Builder;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter;
import com.google.cloud.bigtable.hbase.adapters.admin.TableAdapter;
import com.google.protobuf.ByteString;

/**
 * Bigtable impl of {@link AsyncAdmin}
 * @author spollapally
 */
public class BigtableAsyncAdmin implements AsyncAdmin {
  private final Logger LOG = new Logger(getClass());
  
  //private final Set<TableName> disabledTables; TODO

  private final Configuration configuration;
  private final BigtableOptions options;
  private final BigtableAsyncConnection asyncConnection;
  private final BigtableTableAdminClient bigtableTableAdminClient;

  private BigtableInstanceName bigtableInstanceName;
  private final ColumnDescriptorAdapter columnDescriptorAdapter = new ColumnDescriptorAdapter();
  private final TableAdapter tableAdapter;

  
  public BigtableAsyncAdmin(BigtableAsyncConnection asyncConnection) throws IOException {
    LOG.debug("Creating BigtableAsyncAdmin");
    this.configuration = asyncConnection.getConfiguration();
    this.options = asyncConnection.getOptions();
    this.asyncConnection = asyncConnection;
    this.bigtableTableAdminClient = asyncConnection.getSession().getTableAdminClient();
    
    //this.disabledTables = asyncConnection.getDisabledTables();
    this.bigtableInstanceName = options.getInstanceName();
    this.tableAdapter = new TableAdapter(options, columnDescriptorAdapter);
  }
  
  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, Optional<byte[][]> splitKeys) {
    if (desc.getTableName() == null) {
      return FutureUtils.failedFuture(new IllegalArgumentException("TableName cannot be null"));
    }
    
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    builder.setParent(bigtableInstanceName.toString());
    builder.setTableId(desc.getTableName().getQualifierAsString());
    builder.setTable(tableAdapter.adapt((HTableDescriptor)desc));
    if (splitKeys.isPresent()) {
      for (byte[] splitKey : splitKeys.get()) {
        builder.addInitialSplits(Split.newBuilder().setKey(ByteString.copyFrom(splitKey)).build());
      }
    }
    
    LOG.debug("Creating table with args: {}", builder);
    return FutureUtils.toCompletableFuture(bigtableTableAdminClient.createTableAsync(builder.build()))
        .thenRunAsync(() -> {
        });
    /*
    bigtableTableAdminClient.createTable(builder.build());
    return CompletableFuture.runAsync(() -> {});
    */
  }

  @Override
  public CompletableFuture<Void> createTable(TableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> disableTable(TableName arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }
  
  @Override
  public CompletableFuture<Void> deleteTable(TableName tableName) {
    /*
    Builder deleteBuilder = DeleteTableRequest.newBuilder();
    deleteBuilder.setName(toBigtableName(tableName));
   */
    throw new UnsupportedOperationException("deleteTable"); //TODO
  }

  @Override
  public CompletableFuture<Boolean> tableExists(TableName arg0) {
    throw new UnsupportedOperationException("tableExists"); //TODO impl
  }
  @Override
  public CompletableFuture<List<TableName>> listTableNames(Optional<Pattern> arg0, boolean arg1) {
    throw new UnsupportedOperationException("listTableNames"); //TODO ?
  }

  @Override
  public CompletableFuture<List<TableDescriptor>> listTables(Optional<Pattern> arg0, boolean arg1) {
    throw new UnsupportedOperationException("listTables"); //TODO ?
  }

  //-------****//
  
  @Override
  public CompletableFuture<Boolean> abortProcedure(long arg0, boolean arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
  }

  @Override
  public CompletableFuture<Void> addColumnFamily(TableName arg0, ColumnFamilyDescriptor arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> addReplicationPeer(String arg0, ReplicationPeerConfig arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> appendReplicationPeerTableCFs(String arg0,
      Map<TableName, ? extends Collection<String>> arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> assign(byte[] arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> balance(boolean arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> clearCompactionQueues(ServerName arg0, Set<String> arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<ServerName>> clearDeadServers(List<ServerName> arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> cloneSnapshot(String arg0, TableName arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> compact(TableName arg0, Optional<byte[]> arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] arg0, Optional<byte[]> arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> compactRegionServer(ServerName arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> arg0,
      CoprocessorCallable<S, R> arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(Function<RpcChannel, S> arg0,
      CoprocessorCallable<S, R> arg1, ServerName arg2) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> createNamespace(NamespaceDescriptor arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> decommissionRegionServers(List<ServerName> arg0, boolean arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> deleteColumnFamily(TableName arg0, byte[] arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> deleteNamespace(String arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> deleteSnapshot(String arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern arg0, Pattern arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> disableReplicationPeer(String arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> enableReplicationPeer(String arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> enableTable(TableName arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> execProcedure(String arg0, String arg1, Map<String, String> arg2) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<byte[]> execProcedureWithRet(String arg0, String arg1,
      Map<String, String> arg2) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> flush(TableName arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Void> flushRegion(byte[] arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<ClusterStatus> getClusterStatus() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<ClusterStatus> getClusterStatus(EnumSet<Option> arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionState(TableName arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionStateForRegion(byte[] arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Optional<Long>> getLastMajorCompactionTimestamp(TableName arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Optional<Long>> getLastMajorCompactionTimestampForRegion(byte[] arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<String> getLocks() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<NamespaceDescriptor> getNamespaceDescriptor(String arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getOnlineRegions(ServerName arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<String> getProcedures() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<QuotaSettings>> getQuota(QuotaFilter arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<RegionLoad>> getRegionLoads(ServerName arg0,
      Optional<TableName> arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<ReplicationPeerConfig> getReplicationPeerConfig(String arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<SecurityCapability>> getSecurityCapabilities() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<TableDescriptor> getTableDescriptor(TableName arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<RegionInfo>> getTableRegions(TableName arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isBalancerOn() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isCatalogJanitorOn() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isCleanerChoreOn() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isMasterInMaintenanceMode() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isMergeOn() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isNormalizerOn() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isProcedureFinished(String arg0, String arg1,
      Map<String, String> arg2) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isSnapshotFinished(SnapshotDescription arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isSplitOn() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isTableAvailable(TableName arg0, byte[][] arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isTableDisabled(TableName arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<Boolean> isTableEnabled(TableName arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<ServerName>> listDeadServers() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<ServerName>> listDecommissionedRegionServers() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<NamespaceDescriptor>> listNamespaceDescriptors() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<TableCFs>> listReplicatedTableCFs() {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<ReplicationPeerDescription>> listReplicationPeers(
      Optional<Pattern> arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots(Optional<Pattern> arg0) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
    
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern arg0,
      Pattern arg1) {
    throw new UnsupportedOperationException("listTableSnapshots"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName arg0, Optional<byte[]> arg1) {
    throw new UnsupportedOperationException("majorCompact"); //TODO
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] arg0, Optional<byte[]> arg1) {
    throw new UnsupportedOperationException("abortProcedure"); //TODO
  }

  @Override
  public CompletableFuture<Void> majorCompactRegionServer(ServerName arg0) {
    throw new UnsupportedOperationException("majorCompactRegionServer"); //TODO
  }

  @Override
  public CompletableFuture<Void> mergeRegions(byte[] arg0, byte[] arg1, boolean arg2) {
    throw new UnsupportedOperationException("mergeRegions"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> modifyColumnFamily(TableName arg0, ColumnFamilyDescriptor arg1) {
    throw new UnsupportedOperationException("modifyColumnFamily"); //TODO impl
  }

  @Override
  public CompletableFuture<Void> modifyNamespace(NamespaceDescriptor arg0) {
    throw new UnsupportedOperationException("modifyNamespace"); //TODO
  }

  @Override
  public CompletableFuture<Void> move(byte[] arg0, Optional<ServerName> arg1) {
    throw new UnsupportedOperationException("move"); //TODO ?
  }

  @Override
  public CompletableFuture<Boolean> normalize() {
    throw new UnsupportedOperationException("normalize"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> offline(byte[] arg0) {
    throw new UnsupportedOperationException("offline"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> recommissionRegionServer(ServerName arg0, List<byte[]> arg1) {
    throw new UnsupportedOperationException("recommissionRegionServer"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeer(String arg0) {
    throw new UnsupportedOperationException("removeReplicationPeer"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> removeReplicationPeerTableCFs(String arg0,
      Map<TableName, ? extends Collection<String>> arg1) {
    throw new UnsupportedOperationException("removeReplicationPeerTableCFs"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String arg0) {
    throw new UnsupportedOperationException("restoreSnapshot"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String arg0, boolean arg1) {
    throw new UnsupportedOperationException("restoreSnapshot"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> rollWALWriter(ServerName arg0) {
    throw new UnsupportedOperationException("rollWALWriter"); //TODO ?
  }

  @Override
  public CompletableFuture<Integer> runCatalogJanitor() {
    throw new UnsupportedOperationException("runCatalogJanitor"); //TODO ?
  }

  @Override
  public CompletableFuture<Boolean> runCleanerChore() {
    throw new UnsupportedOperationException("runCleanerChore"); //TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setBalancerOn(boolean arg0) {
    throw new UnsupportedOperationException("setBalancerOn"); //TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setCatalogJanitorOn(boolean arg0) {
    throw new UnsupportedOperationException("setCatalogJanitorOn"); //TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setCleanerChoreOn(boolean arg0) {
    throw new UnsupportedOperationException("abosetCleanerChoreOnrtProcedure"); //TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setMergeOn(boolean arg0) {
    throw new UnsupportedOperationException("setMergeOn"); //TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setNormalizerOn(boolean arg0) {
    throw new UnsupportedOperationException("setNormalizerOn"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> setQuota(QuotaSettings arg0) {
    throw new UnsupportedOperationException("setQuota"); //TODO ?
  }

  @Override
  public CompletableFuture<Boolean> setSplitOn(boolean arg0) {
    throw new UnsupportedOperationException("setSplitOn"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    throw new UnsupportedOperationException("shutdown"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> snapshot(SnapshotDescription arg0) {
    throw new UnsupportedOperationException("snapshot"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> split(TableName arg0) {
    throw new UnsupportedOperationException("split"); //TODO impl
  }

  @Override
  public CompletableFuture<Void> split(TableName arg0, byte[] arg1) {
    throw new UnsupportedOperationException("split"); //TODO impl
  }

  @Override
  public CompletableFuture<Void> splitRegion(byte[] arg0, Optional<byte[]> arg1) {
    throw new UnsupportedOperationException("splitRegion"); //TODO impl
  }

  @Override
  public CompletableFuture<Void> stopMaster() {
    throw new UnsupportedOperationException("stopMaster"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> stopRegionServer(ServerName arg0) {
    throw new UnsupportedOperationException("stopRegionServer"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> truncateTable(TableName arg0, boolean arg1) {
    throw new UnsupportedOperationException("truncateTable"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> unassign(byte[] arg0, boolean arg1) {
    throw new UnsupportedOperationException("unassign"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> updateConfiguration() {
    throw new UnsupportedOperationException("updateConfiguration"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> updateConfiguration(ServerName arg0) {
    throw new UnsupportedOperationException("updateConfiguration"); //TODO ?
  }

  @Override
  public CompletableFuture<Void> updateReplicationPeerConfig(String arg0,
      ReplicationPeerConfig arg1) {
    throw new UnsupportedOperationException("updateReplicationPeerConfig"); 
  }

}
