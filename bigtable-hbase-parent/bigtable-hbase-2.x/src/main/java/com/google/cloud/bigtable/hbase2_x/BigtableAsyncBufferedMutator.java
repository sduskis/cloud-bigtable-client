package com.google.cloud.bigtable.hbase2_x;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncBufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;

public class BigtableAsyncBufferedMutator implements AsyncBufferedMutator {

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void flush() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Configuration getConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TableName getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getWriteBufferSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public CompletableFuture<Void> mutate(Mutation arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<CompletableFuture<Void>> mutate(List<? extends Mutation> arg0) {
    // TODO Auto-generated method stub
    return null;
  }

}
