package com.google.cloud.bigtable.hbase2_x;

import java.util.concurrent.CompletableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Utilities for converting various Future types 
 * @author spollapally
 */
public class FutureUtils {

  public static <T> CompletableFuture<T> toCompletableFuture(
      final ListenableFuture<T> listenableFuture) {
    final CompletableFuture<T> completableFuture = new CompletableFuture<T>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        boolean result = listenableFuture.cancel(mayInterruptIfRunning);
        super.cancel(mayInterruptIfRunning);
        return result;
      }
    };

    Futures.addCallback(listenableFuture, new FutureCallback<T>() {
      public void onFailure(Throwable throwable) {
        completableFuture.completeExceptionally(throwable);
      }

      public void onSuccess(T t) {
        completableFuture.complete(t);
      }
    });
    return completableFuture;
  }

  public static <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }

}
