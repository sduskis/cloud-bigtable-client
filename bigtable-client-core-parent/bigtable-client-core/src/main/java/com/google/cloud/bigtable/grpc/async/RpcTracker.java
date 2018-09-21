/*
 * Copyright 2018 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc.async;

import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.grpc.ClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.Status;
import io.opencensus.common.Scope;
import io.opencensus.contrib.grpc.util.StatusConverter;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.EndSpanOptions;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

/**
 * Adds Spans and Stats to {@link AbstractRetryingOperation}.
 * @param <ResponseT>
 */
public class RpcTracker<ResponseT>
    extends ForwardingClientCallListener.SimpleForwardingClientCallListener<ResponseT> {
  /** Constant <code>LOG</code> */
  private static final Logger LOG = new Logger(AbstractRetryingOperation.class);

  private static final Tracer TRACER = Tracing.getTracer();
  private static final EndSpanOptions END_SPAN_OPTIONS_WITH_SAMPLE_STORE =
      EndSpanOptions.builder().setSampleToLocalSpanStore(true).build();
  private static String makeSpanName(String prefix, String fullMethodName) {
    return prefix + "." + fullMethodName.replace('/', '.');
  }

  private final BigtableAsyncRpc<?,ResponseT> rpc;
  private final Span operationSpan;

  private Timer.Context operationTimerContext;
  private Timer.Context rpcTimerContext;
  private int failedCount = 0;

  RpcTracker(BigtableAsyncRpc<?, ResponseT> rpc, ClientCall.Listener<ResponseT> delegate) {
    super(delegate);
    String spanName = makeSpanName("Operation", rpc.getMethodDescriptor().getFullMethodName());
    this.rpc = rpc;
    this.operationSpan = TRACER.spanBuilder(spanName).setRecordEvents(true).startSpan();
  }

  public void startOperation() {
    Preconditions.checkState(operationTimerContext == null);
    operationTimerContext = rpc.getRpcMetrics().timeOperation();
  }

  public Scope getRunScope() {
    Scope scope = TRACER.withSpan(operationSpan);
    rpcTimerContext = rpc.getRpcMetrics().timeRpc();
    addAnnotation("rpcStart",
        ImmutableMap.of("attempt", AttributeValue.longAttributeValue(failedCount)));
    return scope;
  }

  public void addAnnotation(String description, ImmutableMap<String,AttributeValue> map) {
    operationSpan.addAnnotation(description, map);
  }

  public void addAnnotation(String description) {
    operationSpan.addAnnotation(description);
  }

  @Override
  public void onClose(Status status, Metadata trailers) {
    try (Scope scope = TRACER.withSpan(operationSpan)) {
      rpcTimerContext.close();
      super.onClose(status, trailers);
    }
  }

  public int incrementFailedCount() {
    return ++failedCount;
  }

  public int getFailedCount() {
    return failedCount;
  }

  public void resetFailedCount() {
    this.failedCount = 0;
  }

  public void onRetry(long nextBackOff) {
    addAnnotation("retryWithBackoff",
        ImmutableMap.of("backoff", AttributeValue.longAttributeValue(nextBackOff)));
    rpc.getRpcMetrics().markRetry();
  }

  public void exhaustedRetries(Status status) {
    addAnnotation("exhaustedRetries");
    rpc.getRpcMetrics().markRetriesExhasted();
    if (status != null) {
      finalizeStats(status);
    }
  }

  public void failure(Status status, String channelId, Metadata trailers) {
    LOG.info("Could not complete RPC. Failure #%d, got: %s on channel %s.\nTrailers: %s",
        status.getCause(), failedCount, status, channelId, trailers);
    rpc.getRpcMetrics().markFailure();
    finalizeStats(status);
  }

  public void finalizeStats(Status status) {
    operationTimerContext.close();
    if (operationSpan != null) {
      operationSpan.setStatus(StatusConverter.fromGrpcStatus(status));
      operationSpan.end(END_SPAN_OPTIONS_WITH_SAMPLE_STORE);
    }
  }
}