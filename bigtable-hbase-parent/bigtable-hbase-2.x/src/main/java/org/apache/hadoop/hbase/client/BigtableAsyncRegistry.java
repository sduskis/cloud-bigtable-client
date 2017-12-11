package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableClusterUtilities;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Bigtable Impl of {@link AsyncRegistry}}
 * 
 * @author spollapally
 */
public class BigtableAsyncRegistry implements AsyncRegistry {
  private BigtableClusterUtilities clusterUtilities;

  // TODO discuss BigtableClusterUtilities vs just hardcoded values
  public BigtableAsyncRegistry(Configuration conf) {
    try {
      BigtableOptions options = BigtableOptionsFactory.fromConfiguration(conf);
      clusterUtilities = new BigtableClusterUtilities(options);
    } catch (IOException | GeneralSecurityException e) {
      throw new UncheckedExecutionException(
          "Failed to get BigtableOptions from Hbase Configuration", e);
    }
  }

  @Override
  public void close() {
    clusterUtilities.close();
  }

  /**
   * getClusterId() is required for creating and asyncConnection see
   * {@link ConnectionFactory#createAsyncConnection()}
   */
  @Override
  public CompletableFuture<String> getClusterId() {
    // TODO -
    return CompletableFuture.completedFuture("TestClusterID");
  }

  @Override
  public CompletableFuture<Integer> getCurrentNrHRS() {
    return CompletableFuture.completedFuture(clusterUtilities.getClusterSize());
  }

  @Override
  public CompletableFuture<ServerName> getMasterAddress() {
    throw new UnsupportedOperationException("getMasterAddress");
  }

  @Override
  public CompletableFuture<Integer> getMasterInfoPort() {
    throw new UnsupportedOperationException("getMasterInfoPort");
  }

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocation() {
    throw new UnsupportedOperationException("getMetaRegionLocation");
  }

}
