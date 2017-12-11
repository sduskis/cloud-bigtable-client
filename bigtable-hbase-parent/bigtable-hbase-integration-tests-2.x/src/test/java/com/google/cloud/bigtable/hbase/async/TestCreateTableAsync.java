package com.google.cloud.bigtable.hbase.async;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;

/**
 * @author spollapally
 */
public class TestCreateTableAsync {
  @ClassRule
  public static SharedTestEnvRule sharedTestEnv = new SharedTestEnvRule();

  @Test
  public void testAsyncConnection() throws Exception {
    CompletableFuture<AsyncConnection> connFuture = sharedTestEnv.createAsyncConnection();
    Assert.assertNotNull("async connection should not be null", connFuture);

    AsyncConnection asyncCon = connFuture.get();
    AsyncAdmin asycAdmin = asyncCon.getAdmin();
    Assert.assertNotNull("asycAdmin should not be null", asycAdmin);

    Connection connection = sharedTestEnv.getConnection();
    Assert.assertNotNull("connection should not be null", connection);

    TableName tableName = TableName.valueOf("testAsyncTableCreation");
    try {
      CompletableFuture<Void> future = asycAdmin.createTable(new HTableDescriptor(tableName)
          .addFamily(new HColumnDescriptor(COLUMN_FAMILY)));
      future.get();
    } finally {
      connection.getAdmin().deleteTable(tableName);
      Assert.assertFalse("Table shoudn't exist", connection.getAdmin().tableExists(tableName));
    }
  }
}
