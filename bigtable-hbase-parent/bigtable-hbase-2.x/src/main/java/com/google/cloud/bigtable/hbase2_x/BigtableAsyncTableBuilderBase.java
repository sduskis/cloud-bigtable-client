package com.google.cloud.bigtable.hbase2_x;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;

/**
 * @author spollapally
 * @param <T> - Type parameter 
 */
public abstract class BigtableAsyncTableBuilderBase<T extends AsyncTable>
    implements AsyncTableBuilder<T> {

  public BigtableAsyncTableBuilderBase() {
  }
  
  @Override
  public AsyncTableBuilder<T> setMaxAttempts(int arg0) {
    throw new UnsupportedOperationException("setMaxAttempts"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setOperationTimeout(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setOperationTimeout"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setReadRpcTimeout(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setReadRpcTimeout"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setRetryPause(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setRetryPause"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setRpcTimeout(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setRpcTimeout"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setScanTimeout(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setScanTimeout"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setStartLogErrorsCnt(int arg0) {
    throw new UnsupportedOperationException("setStartLogErrorsCnt"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setWriteRpcTimeout(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setWriteRpcTimeout"); // TODO
  }
}
