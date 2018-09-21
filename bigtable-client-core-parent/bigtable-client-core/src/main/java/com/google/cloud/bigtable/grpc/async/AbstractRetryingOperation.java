/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import io.opencensus.common.Scope;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.api.core.ApiClock;
import com.google.api.gax.retrying.ExponentialRetryAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.retrying.TimedAttemptSettings;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.CallOptionsFactory;
import com.google.cloud.bigtable.grpc.io.ChannelPool;
import com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Deadline;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.Status.Code;
import org.threeten.bp.Duration;
import org.threeten.bp.temporal.ChronoUnit;

/**
 * A {@link ClientCall.Listener} that retries a {@link BigtableAsyncRpc} request.
 */
@SuppressWarnings("unchecked")
public abstract class AbstractRetryingOperation<RequestT, ResponseT, ResultT> {

  @SuppressWarnings("rawtypes")
  private static final ClientCall NULL_CALL = new ClientCall() {

    @Override
    public void start(Listener responseListener, Metadata headers) {
    }

    @Override
    public void request(int numMessages) {
    }

    @Override
    public void cancel(String message, Throwable cause) {
    }

    @Override
    public void halfClose() {
    }

    @Override
    public void sendMessage(Object message) {
    }
  };

  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(AbstractRetryingOperation.class);

  // The server-side has a 5 minute timeout. Unary operations should be timed-out on the client side
  // after 6 minutes.
  protected static final long UNARY_DEADLINE_MINUTES = 6l;

  protected class GrpcFuture<RespT> extends AbstractFuture<RespT> {
    /**
     * This gets called from {@link Future#cancel(boolean)} for cancel(true). If a user explicitly
     * cancels the Future, that should trigger a cancellation of the RPC.
     */
    @Override
    protected void interruptTask() {
      if (!isDone()) {
        AbstractRetryingOperation.this.cancel("Request interrupted.");
      }
    }

    @Override
    public boolean set(@Nullable RespT resp) {
      return super.set(resp);
    }

    @Override
    public boolean setException(Throwable throwable) {
      return super.setException(throwable);
    }
  }

  private final ExponentialRetryAlgorithm exponentialRetryAlgorithm;
  private final ApiClock clock;
  private TimedAttemptSettings currentBackoff;

  protected final BigtableAsyncRpc<RequestT, ResponseT> rpc;
  protected final RetryOptions retryOptions;
  protected final ScheduledExecutorService retryExecutorService;

  private final RequestT request;
  private final CallOptions callOptions;
  private final Metadata originalMetadata;

  protected final GrpcFuture<ResultT> completionFuture;
  private Object callLock = new String("");
  private ClientCall<RequestT, ResponseT> call = NULL_CALL;
  protected final RpcTracker tracker;

  /**
   * <p>Constructor for AbstractRetryingRpcListener.</p>
   *
   * @param retryOptions a {@link com.google.cloud.bigtable.config.RetryOptions} object.
   * @param request a RequestT object.
   * @param retryableRpc a {@link com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc} object.
   * @param callOptions a {@link io.grpc.CallOptions} object.
   * @param retryExecutorService a {@link java.util.concurrent.ScheduledExecutorService} object.
   * @param originalMetadata a {@link io.grpc.Metadata} object.
   * @param clock a {@link ApiClock} object
   */
  public AbstractRetryingOperation(
          RetryOptions retryOptions,
          RequestT request,
          BigtableAsyncRpc<RequestT, ResponseT> retryableRpc,
          CallOptions callOptions,
          ScheduledExecutorService retryExecutorService,
          Metadata originalMetadata,
          ApiClock clock) {
    this.retryOptions = retryOptions;
    this.request = request;
    this.rpc = retryableRpc;
    this.callOptions = callOptions;
    this.retryExecutorService = retryExecutorService;
    this.originalMetadata = originalMetadata;
    this.completionFuture = new GrpcFuture<>();
    this.clock = clock;
    this.exponentialRetryAlgorithm = createRetryAlgorithm(clock);
    this.tracker = new RpcTracker(rpc, new ClientCall.Listener<ResponseT>() {
      @Override
      public void onMessage(ResponseT message) {
        AbstractRetryingOperation.this.onMessage(message);
      }

      @Override
      public void onClose(Status status, Metadata trailers) {
        AbstractRetryingOperation.this._onClose(status, trailers);
      }
    });
  }

  public ClientCall.Listener<ResponseT> getListener(){
    return tracker;
  }

  protected abstract void onMessage(ResponseT message);

  private void _onClose(Status status, Metadata trailers) {
    try {
      synchronized (callLock) {
        call = NULL_CALL;
      }
      // OK
      if (status.isOk()) {
        if (onOK(trailers)) {
          finalizeStats(status);
        }
      } else {
        onError(status, trailers);
      }
    } catch (Exception e) {
      setException(e);
    }
  }

  protected void finalizeStats(Status status) {
    tracker.finalizeStats(status);
  }

  protected void onError(Status status, Metadata trailers) {
    Code code = status.getCode();
    // CANCELLED
    if (code == Status.Code.CANCELLED) {
      setException(status.asRuntimeException());
      // An explicit user cancellation is not considered a failure.
      finalizeStats(status);
      return;
    }

    String channelId = ChannelPool.extractIdentifier(trailers);
    // Non retry scenario
    if (!retryOptions.enableRetries() || !retryOptions.isRetryable(code)
    // Unauthenticated is special because the request never made it to
    // to the server, so all requests are retryable
        || !(isRequestRetryable() || code == Code.UNAUTHENTICATED || code == Code.UNAVAILABLE)) {
      tracker.failure(status, channelId, trailers);
      setException(status.asRuntimeException());
      return;
    }

    // Attempt retry with backoff
    Long nextBackOff = getNextBackoff();
    int failedCount = tracker.incrementFailedCount();

    // Backoffs timed out.
    if (nextBackOff == null) {
      LOG.info("All retries were exhausted. Failure #%d, got: %s on channel %s.\nTrailers: %s",
          status.getCause(), failedCount, status, channelId, trailers);
      setException(getExhaustedRetriesException(status));
    } else {
      LOG.info("Retrying failed call. Failure #%d, got: %s on channel %s.\nTrailers: %s",
          status.getCause(), failedCount, status, channelId, trailers);
      performRetry(nextBackOff);
    }
  }

  protected BigtableRetriesExhaustedException getExhaustedRetriesException(Status status) {
    tracker.exhaustedRetries(status);
    String message = String.format("Exhausted retries after %d failures.", tracker.getFailedCount());
    return new BigtableRetriesExhaustedException(message, status.asRuntimeException());
  }

  protected void performRetry(long nextBackOff) {
    tracker.onRetry(nextBackOff);
    retryExecutorService.schedule(getRunnable(), nextBackOff, TimeUnit.MILLISECONDS);
  }

  protected Runnable getRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        AbstractRetryingOperation.this.run();
      }
    };
  }

  protected boolean isRequestRetryable() {
    return rpc.isRetryable(getRetryRequest());
  }

  protected void setException(Exception exception) {
    completionFuture.setException(exception);
  }

  /**
   * A subclass has the opportunity to perform the final operations it needs now that the RPC is
   * successfully complete. If a subclass has to retry, due to the message, this method will return
   * false
   * @return true if the operation was really completed.
   */
  protected abstract boolean onOK(Metadata trailers);

  protected Long getNextBackoff() {
    if (currentBackoff == null) {
      // Historically, the client waited for "total timeout" after the first failure.  For now,
      // that behavior is preserved, even though that's not the ideal.
      //
      // TODO: Think through retries, and create policy that works with the mental model most
      //       users would have of relating to retries.  That would likely involve updating some
      //       default settings in addition to changing the algorithm.
      currentBackoff = exponentialRetryAlgorithm.createFirstAttempt();
    }
    currentBackoff = exponentialRetryAlgorithm.createNextAttempt(currentBackoff);
    if (!exponentialRetryAlgorithm.shouldRetry(currentBackoff)) {

      // TODO: consider creating a subclass of exponentialRetryAlgorithm to encapsulate this logic
      long timeLeftNs =  currentBackoff.getGlobalSettings().getTotalTimeout().toNanos() -
          (clock.nanoTime() - currentBackoff.getFirstAttemptStartTimeNanos());
      long timeLeftMs = TimeUnit.NANOSECONDS.toMillis(timeLeftNs);

      if (timeLeftMs > currentBackoff.getGlobalSettings().getInitialRetryDelay().toMillis()) {
        // The backoff algorithm doesn't always wait until the timeout is achieved.  Wait
        // one final time so that retries hit
        return timeLeftMs;
      } else {

        // Finish for real.
        return null;
      }
    } else {
      return currentBackoff.getRetryDelay().toMillis();
    }
  }

  @VisibleForTesting
  public boolean inRetryMode() {
    return currentBackoff != null;
  }

  /**
   * Either a response was found, or a timeout event occurred. Reset the information relating to
   * Status oriented exception handling.
   */
  protected void resetStatusBasedBackoff() {
    this.currentBackoff = null;
    tracker.resetFailedCount();
  }

  /**
   * <p>createBackoff.</p>
   *
   * @return a {@link ExponentialRetryAlgorithm} object.
   */
  private ExponentialRetryAlgorithm createRetryAlgorithm(ApiClock clock) {
    long timeoutMs = retryOptions.getMaxElapsedBackoffMillis();

    Deadline deadline = getOperationCallOptions().getDeadline();
    if (deadline != null) {
      timeoutMs = deadline.timeRemaining(TimeUnit.MILLISECONDS);
    }

    RetrySettings retrySettings = RetrySettings.newBuilder()

        .setJittered(true)

        // How long should the sleep be between RPC failure and the next RPC retry?
        .setInitialRetryDelay(toDuration(retryOptions.getInitialBackoffMillis()))

        // How fast should the retry delay increase?
        .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())

        // What is the maximum amount of sleep time between retries?
        // There needs to be some sane number for max retry delay, and it's unclear what that
        // number ought to be.  1 Minute time was chosen because some number is needed.
        .setMaxRetryDelay(Duration.of(1, ChronoUnit.MINUTES))

        // How long should we wait before giving up retries after the first failure?
        .setTotalTimeout(toDuration(timeoutMs))

        .build();
    return new ExponentialRetryAlgorithm(retrySettings, clock);
  }

  private static Duration toDuration(long millis) {
    return Duration.of(millis, ChronoUnit.MILLIS);
  }

  /**
   * Calls {@link BigtableAsyncRpc#newCall(CallOptions)} and
   * {@link BigtableAsyncRpc#start(Object, io.grpc.ClientCall.Listener, Metadata, ClientCall)} }
   * with this as the listener so that retries happen correctly.
   */
  protected void run() {
    try (Scope scope = tracker.getRunScope()) {
      Metadata metadata = new Metadata();
      metadata.merge(originalMetadata);
      synchronized (callLock) {
        // There's a subtle race condition in RetryingStreamOperation which requires a separate
        // newCall/start split. The call variable needs to be set before onMessage() happens; that
        // usually will occur, but some unit tests broke with a merged newCall and start.
        call = rpc.newCall(getRpcCallOptions());
        rpc.start(getRetryRequest(), tracker, metadata, call);
      }
    } catch (Exception e) {
      setException(e);
    }
  }

  protected ClientCall<RequestT, ResponseT> getCall() {
    return call;
  }

  /**
   * Returns the {@link CallOptions} that a user set for the entire Operation, which can span multiple RPCs/retries.
   * @return The {@link CallOptions}
   */
  protected CallOptions getOperationCallOptions() {
    return callOptions;
  }

  /**
   * Create an {@link CallOptions} that has a fail safe RPC deadline to make sure that unary
   * operations don't hang. This will have to be overridden for streaming RPCs like read rows.
   * <p>
   * The logic is as follows:
   * <ol>
   *   <li> If the user provides a deadline, use the deadline</li>
   *   <li> Else If this is a streaming read, don't set an explicit deadline.  The
   *   {@link com.google.cloud.bigtable.grpc.io.Watchdog} will handle hanging</li>
   *   <li> Else Set a deadline of {@link #UNARY_DEADLINE_MINUTES} minutes deadline.</li>
   * </ol>
   *
   * @see com.google.cloud.bigtable.grpc.io.Watchdog Watchdog which handles hanging for streaming
   * reads.
   *
   * @return a {@link CallOptions}
   */
  protected CallOptions getRpcCallOptions() {
    if (callOptions.getDeadline() != null || isStreamingRead()) {
      // If the user set a deadline, honor it.
      // If this is a streaming read, then the Watchdog will take affect and ensure that hanging does not occur.
      return getOperationCallOptions();
    } else {
      // Unary calls should fail after 6 minutes, if there isn't any response from the server.
      return callOptions.withDeadlineAfter(UNARY_DEADLINE_MINUTES, TimeUnit.MINUTES);
    }
  }

  // TODO(sduskis): This is only required because BigtableDataGrpcClient doesn't always use
  //      RetryingReadRowsOperation like it should.
  protected boolean isStreamingRead() {
    return request instanceof ReadRowsRequest &&
            !CallOptionsFactory.ConfiguredCallOptionsFactory.isGet((ReadRowsRequest) request);
  }

  protected RequestT getRetryRequest() {
    return request;
  }

  /**
   * Initial execution of the RPC.
   */
  public ListenableFuture<ResultT> getAsyncResult() {
    tracker.startOperation();
    run();
    return completionFuture;
  }

  /**
   * Cancels the RPC.
   */
  public void cancel() {
    cancel("User requested cancelation.");
  }

  public ResultT getBlockingResult() {
    try {
      return getAsyncResult().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      cancel();
      throw Status.CANCELLED.withCause(e).asRuntimeException();
    } catch (ExecutionException e) {
      cancel();
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  /**
   * Cancels the RPC with a specific message.
   *
   * @param message
   */
  protected void cancel(final String message) {
    call.cancel(message, null);
  }
}
