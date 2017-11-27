/*
 * Copyright 2017 Google Inc. All Rights Reserved. Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.google.cloud.bigtable.hbase2_x;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.security.User;

/**
 * HBase 2.x specific implementation of {@link AbstractBigtableConnection}.
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableConnection extends AbstractBigtableConnection {

  /**
   * <p>
   * Constructor for BigtableConnection.
   * </p>
   * @param conf a {@link Configuration} object.
   * @throws IOException if any.
   */
  public BigtableConnection(Configuration conf) throws IOException {
    super(conf);
  }

  public BigtableConnection(Configuration conf, ExecutorService pool, User user)
      throws IOException {
    super(conf);
  }

  /** {@inheritDoc} */
  @Override
  public Admin getAdmin() throws IOException {
    return new BigtableAdmin(getOptions(), getConfiguration(), this, getBigtableTableAdminClient(),
        getDisabledTables());
  }

  /** {@inheritDoc} */
  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    for (RegionLocator locator : locatorCache) {
      if (locator.getName().equals(tableName)) {
        return locator;
      }
    }

    RegionLocator newLocator =
        new BigtableRegionLocator2x(tableName, getOptions(),  getSession().getDataClient());

    if (locatorCache.add(newLocator)) {
      return newLocator;
    }

    for (RegionLocator locator : locatorCache) {
      if (locator.getName().equals(tableName)) {
        return locator;
      }
    }

    throw new IllegalStateException(newLocator + " was supposed to be in the cache");
  }

  
  @Override
  public TableBuilder getTableBuilder(TableName arg0, ExecutorService arg1) {
    throw new UnsupportedOperationException("getTableBuilder"); // TODO
  }
}
