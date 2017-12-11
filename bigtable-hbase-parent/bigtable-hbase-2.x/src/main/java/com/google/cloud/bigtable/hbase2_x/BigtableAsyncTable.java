package com.google.cloud.bigtable.hbase2_x;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScanResultConsumer;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.BatchExecutor;
import com.google.cloud.bigtable.hbase.BigtableTable;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.common.base.Preconditions;

/**
 * HBase 2.x specific implementation of {@link AsyncTable}.
 * @author spollapally
 */
@SuppressWarnings("deprecation")
public class BigtableAsyncTable implements AsyncTable{
  private final Logger LOG = new Logger(getClass());

  private final BigtableAsyncConnection asyncConnection;
  private final BigtableDataClient client;
  private final HBaseRequestAdapter hbaseAdapter;
  private final TableName tableName;
  private BatchExecutor batchExecutor;
  
  public BigtableAsyncTable(BigtableAsyncConnection asyncConnection,
      HBaseRequestAdapter hbaseAdapter) {
    this.asyncConnection = asyncConnection;
    BigtableSession session = asyncConnection.getSession();
    this.client = session.getDataClient();
    this.hbaseAdapter = hbaseAdapter;
    this.tableName = hbaseAdapter.getTableName();
  }
  
  protected synchronized BatchExecutor getBatchExecutor() {
    if (batchExecutor == null) {
      batchExecutor = new BatchExecutor(asyncConnection.getSession(), hbaseAdapter);
    }
    return batchExecutor;
  }
  
  @Override
  public CompletableFuture<Result> append(Append append) {
    ReadModifyWriteRowRequest request = hbaseAdapter.adapt(append);
    return FutureUtils.toCompletableFuture(client.readModifyWriteRowAsync(request))
        .thenApplyAsync((response) -> Adapters.ROW_ADAPTER.adaptResponse(response.getRow()));
  }

  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
    throw new UnsupportedOperationException("batch"); // TODO
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] rowParam, byte[] familyParam) {
    CheckAndMutateBuilder checkAndMutateBuilder = new CheckAndMutateBuilder() {
      private final byte[] row = Preconditions.checkNotNull(rowParam, "row is null");
      private final byte[] family = Preconditions.checkNotNull(familyParam, "family is null");
      private byte[] qualifier;
      private CompareOp op;
      private byte[] value;

      @Override
      public CheckAndMutateBuilder qualifier(byte[] qualifier) {
        this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier is null");
        return this;
      }
      
      @Override
      public CheckAndMutateBuilder ifNotExists() {
        this.op = CompareOp.EQUAL;
        this.value = null;
        return this;
      }
      
      @Override
      public CheckAndMutateBuilder ifMatches(CompareOperator compareOp, byte[] value) {
        CompareOperator cOp = Preconditions.checkNotNull(compareOp, "compareOp is null");
        try {
        op = CompareOp.valueOf(cOp.name());
      } catch (IllegalArgumentException e) {
        LOG.error("Failed to convert from CompareOperator to CompareOp for {}", compareOp);
        throw e;
      }
        
        this.value = Preconditions.checkNotNull(value, "value is null");
        return this;
      }

      @Override
      public CompletableFuture<Boolean> thenPut(Put put) {
        return makeCheckAndOpReq(put.getRow(), hbaseAdapter.adapt(put).getMutationsList());
      }
      
      @Override
      public CompletableFuture<Boolean> thenMutate(RowMutations rm) {
        List<Mutation> adaptedMutations = new ArrayList<>();
        for (org.apache.hadoop.hbase.client.Mutation mut : rm.getMutations()) {
          adaptedMutations.addAll(hbaseAdapter.adapt(mut).getMutationsList());
        }
        
        return makeCheckAndOpReq(rm.getRow(), adaptedMutations);
      }
      
      @Override
      public CompletableFuture<Boolean> thenDelete(Delete delete) {
        return makeCheckAndOpReq(delete.getRow(), hbaseAdapter.adapt(delete).getMutationsList());
      }
      
      private CompletableFuture<Boolean> makeCheckAndOpReq(byte[] actionRow, List<Mutation> mutations) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        try {
          CheckAndMutateRowRequest.Builder requestBuilder =
              BigtableTable.makeConditionalMutationRequestBuilder(
                  row,
                  family,
                  qualifier,
                  op,
                  value,
                  actionRow,
                  mutations, hbaseAdapter.getBigtableTableName().toString());
          return FutureUtils
              .toCompletableFuture(client.checkAndMutateRowAsync(requestBuilder.build()))
              .thenApply((resp) -> BigtableTable.wasMutationApplied(requestBuilder, resp));
        } catch (IOException e) {
          future.completeExceptionally(e);
        }

        future.completeExceptionally(new IllegalStateException("unexpected state")); 
        return future;
      }
    };
    
    return checkAndMutateBuilder;
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    MutateRowRequest request = hbaseAdapter.adapt(delete);
    return FutureUtils.toCompletableFuture(client.mutateRowAsync(request)).thenRunAsync(() -> {
    });
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> deletes) {
    // TODO use batch
    throw new UnsupportedOperationException("delete list");
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    /*
    FutureUtils.toCompletableFuture(client.readFlatRowsAsync(hbaseAdapter.adapt(get)))
    .thenApply((resp) -> {System.out.println();});
    */
    
    //TODO
    throw new UnsupportedOperationException("get");
  }

  @Override
  public List<CompletableFuture<Result>> get(List<Get> arg0) {
    // TODO use batch
    throw new UnsupportedOperationException("get list");
  }

  @Override
  public Configuration getConfiguration() {
    return this.asyncConnection.getConfiguration();
  }

  @Override
  public TableName getName() {
    return this.tableName;
  }

  @Override
  public long getOperationTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException("getOperationTimeout"); //TODO
  }

  @Override
  public long getReadRpcTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getReadRpcTimeout"); //TODO
  }

  @Override
  public long getRpcTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getRpcTimeout"); //TODO
  }

  @Override
  public long getScanTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getScanTimeout"); //TODO
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit arg0) {
    throw new UnsupportedOperationException("getWriteRpcTimeout"); //TODO
  }

  @Override
  public CompletableFuture<Result> increment(Increment arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompletableFuture<Void> mutateRow(RowMutations arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompletableFuture<Void> put(Put arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CompletableFuture<List<Result>> scanAll(Scan arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultScanner getScanner(Scan arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void scan(Scan arg0, ScanResultConsumer arg1) {
    // TODO Auto-generated method stub
    
  }

}
