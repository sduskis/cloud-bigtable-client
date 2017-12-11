package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncAdminBuilder;
import org.apache.hadoop.hbase.client.AsyncBufferedMutatorBuilder;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncRegistry;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import org.apache.hadoop.hbase.client.RawAsyncTable;
import org.apache.hadoop.hbase.security.User;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter.MutationAdapters;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncAdmin;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncTable;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncTableBuilderBase;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncTableRegionLocator;

/**
 * 
 * @author spollapally
 */
public class BigtableAsyncConnection implements AsyncConnection, Closeable {
  private final Logger LOG = new Logger(getClass());
  private final Configuration conf;
  private BigtableSession session;
  private BigtableOptions options;
  private volatile boolean closed = false;

  private Set<TableName> disabledTables = new HashSet<>();
  private MutationAdapters mutationAdapters;

  static {
    // TODO - factor in for othere stuff going in AbstractBigtableConnection
    Adapters.class.getName();
  }
  
  /*
  public BigtableAsyncConnection(Configuration conf) throws IOException {
    this(conf, null, null, null);
  }
  */

  // TODO: support other forms of constructors with ExecutorService etc..?
  public BigtableAsyncConnection(Configuration conf, AsyncRegistry registry, String clusterId,
      User user) throws IOException {
    LOG.debug("Creating BigtableAsyncConnection");
    this.conf = conf;

    BigtableOptions opts;
    try {
      opts = BigtableOptionsFactory.fromConfiguration(conf);
    } catch (IOException ioe) {
      LOG.error("Error loading BigtableOptions from Configuration.", ioe);
      throw ioe;
    }
    
    this.closed = false;
    this.session = new BigtableSession(opts);
    this.options = this.session.getOptions();
  }

  // TODO - explore options to reuse code from AbstractBigtableConnection
  public HBaseRequestAdapter createAdapter(TableName tableName) {
    if (mutationAdapters == null) {
      synchronized (this) {
        if (mutationAdapters == null) {
          mutationAdapters = new HBaseRequestAdapter.MutationAdapters(options, conf);
        }
      }
    }
    return new HBaseRequestAdapter(options, tableName, mutationAdapters);
  }

  public BigtableSession getSession() {
    return this.session;
  }

  public BigtableOptions getOptions() {
    return this.options;
  }
  
  @Override
  public void close() throws IOException {
    LOG.debug("closeing BigtableAsyncConnection");
    if (!this.closed) {
      this.session.close();
      /*
      shutdownBatchPool();
      if (this.bufferedMutatorExecutorService != null) {
        this.bufferedMutatorExecutorService.shutdown();
        this.bufferedMutatorExecutorService = null;
      }
      */
      
      this.closed = true;
    }
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder() {
    return new AsyncAdminBuilder() {
      
      @Override
      public AsyncAdminBuilder setStartLogErrorsCnt(int arg0) {
        throw new UnsupportedOperationException("setStartLogErrorsCnt");
      }
      
      @Override
      public AsyncAdminBuilder setRpcTimeout(long arg0, TimeUnit arg1) {
        throw new UnsupportedOperationException("setStartLogErrorsCnt");
      }
      
      @Override
      public AsyncAdminBuilder setRetryPause(long arg0, TimeUnit arg1) {
        throw new UnsupportedOperationException("setStartLogErrorsCnt");
      }
      
      @Override
      public AsyncAdminBuilder setOperationTimeout(long arg0, TimeUnit arg1) {
        throw new UnsupportedOperationException("setStartLogErrorsCnt");
      }
      
      @Override
      public AsyncAdminBuilder setMaxAttempts(int arg0) {
        throw new UnsupportedOperationException("setStartLogErrorsCnt");
      }
      
      @Override
      public AsyncAdmin build() {
        try {
          return new BigtableAsyncAdmin(BigtableAsyncConnection.this);
        } catch (IOException e) {
          LOG.error("failed to build BigtableAsyncAdmin", e);
          throw new UncheckedIOException("failed to build BigtableAsyncAdmin", e);
        }
      }
    };
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder(ExecutorService arg0) {
    throw new UnsupportedOperationException("getAdminBuilder ExecutorService"); // TODO
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName arg0) {
    throw new UnsupportedOperationException("getBufferedMutatorBuilder"); // TODO
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName arg0,
      ExecutorService arg1) {
    throw new UnsupportedOperationException("getBufferedMutatorBuilder"); // TODO
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  @Override
  public AsyncTableBuilder<RawAsyncTable> getRawTableBuilder(TableName arg0) {
    throw new UnsupportedOperationException("getRawTableBuilder"); // TODO
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    return new BigtableAsyncTableRegionLocator(tableName, options, session.getDataClient());
  }

  @Override
  public AsyncTableBuilder<AsyncTable> getTableBuilder(TableName tableName,
      ExecutorService executorService) {
    return new BigtableAsyncTableBuilderBase<AsyncTable>() {

      @Override
      public BigtableAsyncTable build() {
        return new BigtableAsyncTable(BigtableAsyncConnection.this, createAdapter(tableName));
      }
    };
  }

}
