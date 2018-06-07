package com.google.cloud.bigtable.hbase;

import com.google.cloud.bigtable.hbase1_x.BigtableConnection;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelProvider;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class Driver {
  public static void main() throws IOException {
    System.out.println(NettyChannelBuilder.class);
    System.out.println(NettyChannelProvider.class);
    System.out.println(DnsNameResolverProvider.class);
    BigtableConfiguration.configure("project", "instance");
    new BigtableConnection(new Configuration(false));
  }
}
