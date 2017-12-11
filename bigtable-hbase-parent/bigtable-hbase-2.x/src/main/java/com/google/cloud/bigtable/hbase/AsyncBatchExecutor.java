package com.google.cloud.bigtable.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase2_x.FutureUtils;

/**
 * This class is experimental.. might go away
 * 
 * @author spollapally
 */
public class AsyncBatchExecutor extends BatchExecutor {

  public AsyncBatchExecutor(BigtableSession session, HBaseRequestAdapter requestAdapter) {
    super(session, requestAdapter);
    // TODO Auto-generated constructor stub
  }

  public <T> List<CompletableFuture<T>> batchAsync(List<? extends Row> actions) {
    List<CompletableFuture<T>> futures = new ArrayList<>(actions.size());
    BulkOperation bulkOperation = new BulkOperation(session, requestAdapter.getBigtableTableName());
    
    for (int i = 0; i < actions.size(); i++) {
      futures.add((CompletableFuture<T>) issueAsyncRequest(bulkOperation, actions.get(i)));
    }
    return futures;
  }
  
  private CompletableFuture<?> issueAsyncRequest(BulkOperation bulkOperation, Row row) {
    CompletableFuture<?> future = new CompletableFuture<>();

    try {
      if (row instanceof Get) {
        return FutureUtils
            .toCompletableFuture(bulkOperation.bulkRead.add(requestAdapter.adapt((Get) row)))
            .thenApply((response) -> Adapters.FLAT_ROW_ADAPTER.adaptResponse(response));
      } else if (row instanceof Put) {
        return FutureUtils.toCompletableFuture(
            bulkOperation.bulkMutation.add(requestAdapter.adaptEntry((Put) row)));
        //TODO .thenApply((response) -> Adapters.ROW_ADAPTER.adaptResponse(response));
      } else if (row instanceof Delete) {
        return FutureUtils.toCompletableFuture(
            bulkOperation.bulkMutation.add(requestAdapter.adaptEntry((Delete) row)));
        //TODO .thenApply((response) -> Adapters.ROW_ADAPTER.adaptResponse(response));
      } else if (row instanceof Append) {
        return FutureUtils
            .toCompletableFuture(
                asyncExecutor.readModifyWriteRowAsync(requestAdapter.adapt((Append) row)))
            .thenApply((response) -> Adapters.ROW_ADAPTER.adaptResponse(response.getRow()));
      } else if (row instanceof Increment) {
        return FutureUtils
            .toCompletableFuture(
                asyncExecutor.readModifyWriteRowAsync(requestAdapter.adapt((Increment) row)))
            .thenApply((response) -> Adapters.ROW_ADAPTER.adaptResponse(response.getRow()));
      } else if (row instanceof RowMutations) {
        return FutureUtils.toCompletableFuture(
            bulkOperation.bulkMutation.add(requestAdapter.adaptEntry((RowMutations) row)));
        //TODO .thenApply((response) -> Adapters.ROW_ADAPTER.adaptResponse(response));
      }
    } catch (InterruptedException e) {
      future.completeExceptionally(
          new IOException("Could not process the batch due to interrupt", e));
      return future;
    } catch (Throwable e) {
      future.completeExceptionally(new IOException("Could not process the batch", e));
    }

    LOG.error("Encountered unknown action type %s", row.getClass());
    future.completeExceptionally(new IOException(
        new IllegalArgumentException("Encountered unknown action type: " + row.getClass())));
    return future;
  }

}
