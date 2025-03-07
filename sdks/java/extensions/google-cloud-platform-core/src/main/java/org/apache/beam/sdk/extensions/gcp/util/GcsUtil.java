/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.gcp.util;

import static org.apache.beam.sdk.io.FileSystemUtils.wildcardToRegexp;
import static org.apache.beam.sdk.options.ExperimentalOptions.hasExperiment;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.RewriteResponse;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides operations on GCS. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class GcsUtil {

  /**
   * This is a {@link DefaultValueFactory} able to create a {@link GcsUtil} using any transport
   * flags specified on the {@link PipelineOptions}.
   */
  public static class GcsUtilFactory implements DefaultValueFactory<GcsUtil> {
    /**
     * Returns an instance of {@link GcsUtil} based on the {@link PipelineOptions}.
     *
     * <p>If no instance has previously been created, one is created and the value stored in {@code
     * options}.
     */
    @Override
    public GcsUtil create(PipelineOptions options) {
      LOG.debug("Creating new GcsUtil");
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      Storage.Builder storageBuilder = Transport.newStorageClient(gcsOptions);
      return new GcsUtil(
          storageBuilder.build(),
          storageBuilder.getHttpRequestInitializer(),
          gcsOptions.getExecutorService(),
          hasExperiment(options, "use_grpc_for_gcs"),
          gcsOptions.getGcpCredential(),
          gcsOptions.getGcsUploadBufferSizeBytes());
    }

    /** Returns an instance of {@link GcsUtil} based on the given parameters. */
    public static GcsUtil create(
        PipelineOptions options,
        Storage storageClient,
        HttpRequestInitializer httpRequestInitializer,
        ExecutorService executorService,
        Credentials credentials,
        @Nullable Integer uploadBufferSizeBytes) {
      return new GcsUtil(
          storageClient,
          httpRequestInitializer,
          executorService,
          hasExperiment(options, "use_grpc_for_gcs"),
          credentials,
          uploadBufferSizeBytes);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(GcsUtil.class);

  /** Maximum number of items to retrieve per Objects.List request. */
  private static final long MAX_LIST_ITEMS_PER_CALL = 1024;

  /** Matches a glob containing a wildcard, capturing the portion before the first wildcard. */
  private static final Pattern GLOB_PREFIX = Pattern.compile("(?<PREFIX>[^\\[*?]*)[\\[*?].*");

  /** Maximum number of requests permitted in a GCS batch request. */
  private static final int MAX_REQUESTS_PER_BATCH = 100;
  /** Maximum number of concurrent batches of requests executing on GCS. */
  private static final int MAX_CONCURRENT_BATCHES = 256;

  private static final FluentBackoff BACKOFF_FACTORY =
      FluentBackoff.DEFAULT.withMaxRetries(10).withInitialBackoff(Duration.standardSeconds(1));

  /////////////////////////////////////////////////////////////////////////////

  /** Client for the GCS API. */
  private Storage storageClient;

  private Supplier<BatchInterface> batchRequestSupplier;

  private final HttpRequestInitializer httpRequestInitializer;
  /** Buffer size for GCS uploads (in bytes). */
  private final @Nullable Integer uploadBufferSizeBytes;

  // Helper delegate for turning IOExceptions from API calls into higher-level semantics.
  private final ApiErrorExtractor errorExtractor = new ApiErrorExtractor();

  // Unbounded thread pool for codependent pipeline operations that will deadlock the pipeline if
  // starved for threads.
  // Exposed for testing.
  final ExecutorService executorService;

  private Credentials credentials;

  private GoogleCloudStorage googleCloudStorage;
  private GoogleCloudStorageOptions googleCloudStorageOptions;

  /** Rewrite operation setting. For testing purposes only. */
  @VisibleForTesting @Nullable Long maxBytesRewrittenPerCall;

  @VisibleForTesting @Nullable AtomicInteger numRewriteTokensUsed;

  /** Returns the prefix portion of the glob that doesn't contain wildcards. */
  public static String getNonWildcardPrefix(String globExp) {
    Matcher m = GLOB_PREFIX.matcher(globExp);
    checkArgument(m.matches(), String.format("Glob expression: [%s] is not expandable.", globExp));
    return m.group("PREFIX");
  }

  /** Returns true if the given {@code spec} contains wildcard. */
  public static boolean isWildcard(GcsPath spec) {
    return GLOB_PREFIX.matcher(spec.getObject()).matches();
  }

  @VisibleForTesting
  GcsUtil(
      Storage storageClient,
      HttpRequestInitializer httpRequestInitializer,
      ExecutorService executorService,
      Boolean shouldUseGrpc,
      Credentials credentials,
      @Nullable Integer uploadBufferSizeBytes) {
    this.storageClient = storageClient;
    this.httpRequestInitializer = httpRequestInitializer;
    this.uploadBufferSizeBytes = uploadBufferSizeBytes;
    this.executorService = executorService;
    this.credentials = credentials;
    this.maxBytesRewrittenPerCall = null;
    this.numRewriteTokensUsed = null;
    googleCloudStorageOptions =
        GoogleCloudStorageOptions.builder()
            .setAppName("Beam")
            .setGrpcEnabled(shouldUseGrpc)
            .build();
    googleCloudStorage =
        createGoogleCloudStorage(googleCloudStorageOptions, storageClient, credentials);
    this.batchRequestSupplier =
        () -> {
          // Capture reference to this so that the most recent storageClient and initializer
          // are used.
          GcsUtil util = this;
          return new BatchInterface() {
            final BatchRequest batch = util.storageClient.batch(util.httpRequestInitializer);

            @Override
            public <T> void queue(
                AbstractGoogleJsonClientRequest<T> request, JsonBatchCallback<T> cb)
                throws IOException {
              request.queue(batch, cb);
            }

            @Override
            public void execute() throws IOException {
              batch.execute();
            }

            @Override
            public int size() {
              return batch.size();
            }
          };
        };
  }

  // Use this only for testing purposes.
  protected void setStorageClient(Storage storageClient) {
    this.storageClient = storageClient;
  }

  // Use this only for testing purposes.
  protected void setBatchRequestSupplier(Supplier<BatchInterface> supplier) {
    this.batchRequestSupplier = supplier;
  }

  /**
   * Expands a pattern into matched paths. The pattern path may contain globs, which are expanded in
   * the result. For patterns that only match a single object, we ensure that the object exists.
   */
  public List<GcsPath> expand(GcsPath gcsPattern) throws IOException {
    Pattern p = null;
    String prefix = null;
    if (isWildcard(gcsPattern)) {
      // Part before the first wildcard character.
      prefix = getNonWildcardPrefix(gcsPattern.getObject());
      p = Pattern.compile(wildcardToRegexp(gcsPattern.getObject()));
    } else {
      // Not a wildcard.
      try {
        // Use a get request to fetch the metadata of the object, and ignore the return value.
        // The request has strong global consistency.
        getObject(gcsPattern);
        return ImmutableList.of(gcsPattern);
      } catch (FileNotFoundException e) {
        // If the path was not found, return an empty list.
        return ImmutableList.of();
      }
    }

    LOG.debug(
        "matching files in bucket {}, prefix {} against pattern {}",
        gcsPattern.getBucket(),
        prefix,
        p.toString());

    String pageToken = null;
    List<GcsPath> results = new ArrayList<>();
    do {
      Objects objects = listObjects(gcsPattern.getBucket(), prefix, pageToken);
      if (objects.getItems() == null) {
        break;
      }

      // Filter objects based on the regex.
      for (StorageObject o : objects.getItems()) {
        String name = o.getName();
        // Skip directories, which end with a slash.
        if (p.matcher(name).matches() && !name.endsWith("/")) {
          LOG.debug("Matched object: {}", name);
          results.add(GcsPath.fromObject(o));
        }
      }
      pageToken = objects.getNextPageToken();
    } while (pageToken != null);

    return results;
  }

  @VisibleForTesting
  @Nullable
  Integer getUploadBufferSizeBytes() {
    return uploadBufferSizeBytes;
  }

  private static BackOff createBackOff() {
    return BackOffAdapter.toGcpBackOff(BACKOFF_FACTORY.backoff());
  }

  /**
   * Returns the file size from GCS or throws {@link FileNotFoundException} if the resource does not
   * exist.
   */
  public long fileSize(GcsPath path) throws IOException {
    return getObject(path).getSize().longValue();
  }

  /** Returns the {@link StorageObject} for the given {@link GcsPath}. */
  public StorageObject getObject(GcsPath gcsPath) throws IOException {
    return getObject(gcsPath, createBackOff(), Sleeper.DEFAULT);
  }

  @VisibleForTesting
  StorageObject getObject(GcsPath gcsPath, BackOff backoff, Sleeper sleeper) throws IOException {
    Storage.Objects.Get getObject =
        storageClient.objects().get(gcsPath.getBucket(), gcsPath.getObject());
    try {
      return ResilientOperation.retry(
          getObject::execute, backoff, RetryDeterminer.SOCKET_ERRORS, IOException.class, sleeper);
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (e instanceof IOException && errorExtractor.itemNotFound((IOException) e)) {
        throw new FileNotFoundException(gcsPath.toString());
      }
      throw new IOException(
          String.format("Unable to get the file object for path %s.", gcsPath), e);
    }
  }

  /**
   * Returns {@link StorageObjectOrIOException StorageObjectOrIOExceptions} for the given {@link
   * GcsPath GcsPaths}.
   */
  public List<StorageObjectOrIOException> getObjects(List<GcsPath> gcsPaths) throws IOException {
    List<StorageObjectOrIOException[]> results = new ArrayList<>();
    executeBatches(makeGetBatches(gcsPaths, results));
    ImmutableList.Builder<StorageObjectOrIOException> ret = ImmutableList.builder();
    for (StorageObjectOrIOException[] result : results) {
      ret.add(result[0]);
    }
    return ret.build();
  }

  public Objects listObjects(String bucket, String prefix, @Nullable String pageToken)
      throws IOException {
    return listObjects(bucket, prefix, pageToken, null);
  }

  /**
   * Lists {@link Objects} given the {@code bucket}, {@code prefix}, {@code pageToken}.
   *
   * <p>For more details, see https://cloud.google.com/storage/docs/json_api/v1/objects/list.
   */
  public Objects listObjects(
      String bucket, String prefix, @Nullable String pageToken, @Nullable String delimiter)
      throws IOException {
    // List all objects that start with the prefix (including objects in sub-directories).
    Storage.Objects.List listObject = storageClient.objects().list(bucket);
    listObject.setMaxResults(MAX_LIST_ITEMS_PER_CALL);
    listObject.setPrefix(prefix);
    listObject.setDelimiter(delimiter);

    if (pageToken != null) {
      listObject.setPageToken(pageToken);
    }

    try {
      return ResilientOperation.retry(
          listObject::execute, createBackOff(), RetryDeterminer.SOCKET_ERRORS, IOException.class);
    } catch (Exception e) {
      throw new IOException(
          String.format("Unable to match files in bucket %s, prefix %s.", bucket, prefix), e);
    }
  }

  /**
   * Returns the file size from GCS or throws {@link FileNotFoundException} if the resource does not
   * exist.
   */
  @VisibleForTesting
  List<Long> fileSizes(List<GcsPath> paths) throws IOException {
    List<StorageObjectOrIOException> results = getObjects(paths);

    ImmutableList.Builder<Long> ret = ImmutableList.builder();
    for (StorageObjectOrIOException result : results) {
      ret.add(toFileSize(result));
    }
    return ret.build();
  }

  private Long toFileSize(StorageObjectOrIOException storageObjectOrIOException)
      throws IOException {
    if (storageObjectOrIOException.ioException() != null) {
      throw storageObjectOrIOException.ioException();
    } else {
      return storageObjectOrIOException.storageObject().getSize().longValue();
    }
  }

  @VisibleForTesting
  void setCloudStorageImpl(GoogleCloudStorage g) {
    googleCloudStorage = g;
  }

  @VisibleForTesting
  void setCloudStorageImpl(GoogleCloudStorageOptions g) {
    googleCloudStorageOptions = g;
  }

  /**
   * Opens an object in GCS.
   *
   * <p>Returns a SeekableByteChannel that provides access to data in the bucket.
   *
   * @param path the GCS filename to read from
   * @return a SeekableByteChannel that can read the object data
   */
  public SeekableByteChannel open(GcsPath path) throws IOException {
    return googleCloudStorage.open(new StorageResourceId(path.getBucket(), path.getObject()));
  }

  /**
   * Opens an object in GCS.
   *
   * <p>Returns a SeekableByteChannel that provides access to data in the bucket.
   *
   * @param path the GCS filename to read from
   * @param readOptions Fine-grained options for behaviors of retries, buffering, etc.
   * @return a SeekableByteChannel that can read the object data
   */
  @VisibleForTesting
  SeekableByteChannel open(GcsPath path, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    HashMap<String, String> baseLabels = new HashMap<>();
    baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
    baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "Storage");
    baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "GcsGet");
    baseLabels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.cloudStorageBucket(path.getBucket()));
    baseLabels.put(
        MonitoringInfoConstants.Labels.GCS_PROJECT_ID,
        String.valueOf(googleCloudStorageOptions.getProjectId()));
    baseLabels.put(MonitoringInfoConstants.Labels.GCS_BUCKET, path.getBucket());

    ServiceCallMetric serviceCallMetric =
        new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
    try {
      SeekableByteChannel channel =
          googleCloudStorage.open(
              new StorageResourceId(path.getBucket(), path.getObject()), readOptions);
      serviceCallMetric.call("ok");
      return channel;
    } catch (IOException e) {
      if (e.getCause() instanceof GoogleJsonResponseException) {
        serviceCallMetric.call(((GoogleJsonResponseException) e.getCause()).getDetails().getCode());
      }
      throw e;
    }
  }

  /** @deprecated Use {@link #create(GcsPath, CreateOptions)} instead. */
  @Deprecated
  public WritableByteChannel create(GcsPath path, String type) throws IOException {
    CreateOptions.Builder builder = CreateOptions.builder().setContentType(type);
    return create(path, builder.build());
  }

  /** @deprecated Use {@link #create(GcsPath, CreateOptions)} instead. */
  @Deprecated
  public WritableByteChannel create(GcsPath path, String type, Integer uploadBufferSizeBytes)
      throws IOException {
    CreateOptions.Builder builder =
        CreateOptions.builder()
            .setContentType(type)
            .setUploadBufferSizeBytes(uploadBufferSizeBytes);
    return create(path, builder.build());
  }

  @AutoValue
  public abstract static class CreateOptions {
    /**
     * If true, the created file is expected to not exist. Instead of checking for file presence
     * before writing a write exception may occur if the file does exist.
     */
    public abstract boolean getExpectFileToNotExist();

    /**
     * If non-null, the upload buffer size to be used. If null, the buffer size corresponds to {code
     * GCSUtil.getUploadBufferSizeBytes}
     */
    public abstract @Nullable Integer getUploadBufferSizeBytes();

    /** The content type for the created file, eg "text/plain". */
    public abstract @Nullable String getContentType();

    public static Builder builder() {
      return new AutoValue_GcsUtil_CreateOptions.Builder().setExpectFileToNotExist(false);
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setContentType(String value);

      public abstract Builder setUploadBufferSizeBytes(int value);

      public abstract Builder setExpectFileToNotExist(boolean value);

      public abstract CreateOptions build();
    }
  }

  /**
   * Creates an object in GCS and prepares for uploading its contents.
   *
   * @param path the GCS file to write to
   * @param options to be used for creating and configuring file upload
   * @return a WritableByteChannel that can be used to write data to the object.
   */
  public WritableByteChannel create(GcsPath path, CreateOptions options) throws IOException {
    AsyncWriteChannelOptions wcOptions = googleCloudStorageOptions.getWriteChannelOptions();
    @Nullable
    Integer uploadBufferSizeBytes =
        options.getUploadBufferSizeBytes() != null
            ? options.getUploadBufferSizeBytes()
            : getUploadBufferSizeBytes();
    if (uploadBufferSizeBytes != null) {
      wcOptions = wcOptions.toBuilder().setUploadChunkSize(uploadBufferSizeBytes).build();
    }
    GoogleCloudStorageOptions newGoogleCloudStorageOptions =
        googleCloudStorageOptions.toBuilder().setWriteChannelOptions(wcOptions).build();
    GoogleCloudStorage gcpStorage =
        createGoogleCloudStorage(
            newGoogleCloudStorageOptions, this.storageClient, this.credentials);
    StorageResourceId resourceId =
        new StorageResourceId(
            path.getBucket(),
            path.getObject(),
            // If we expect the file not to exist, we set a generation id of 0. This avoids a read
            // to identify the object exists already and should be overwritten.
            // See {@link GoogleCloudStorage#create(StorageResourceId, GoogleCloudStorageOptions)}
            options.getExpectFileToNotExist() ? 0L : StorageResourceId.UNKNOWN_GENERATION_ID);
    CreateObjectOptions.Builder createBuilder =
        CreateObjectOptions.builder().setOverwriteExisting(true);
    if (options.getContentType() != null) {
      createBuilder = createBuilder.setContentType(options.getContentType());
    }

    HashMap<String, String> baseLabels = new HashMap<>();
    baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
    baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "Storage");
    baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "GcsInsert");
    baseLabels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.cloudStorageBucket(path.getBucket()));
    baseLabels.put(
        MonitoringInfoConstants.Labels.GCS_PROJECT_ID,
        String.valueOf(googleCloudStorageOptions.getProjectId()));
    baseLabels.put(MonitoringInfoConstants.Labels.GCS_BUCKET, path.getBucket());

    ServiceCallMetric serviceCallMetric =
        new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
    try {
      WritableByteChannel channel = gcpStorage.create(resourceId, createBuilder.build());
      serviceCallMetric.call("ok");
      return channel;
    } catch (IOException e) {
      if (e.getCause() instanceof GoogleJsonResponseException) {
        serviceCallMetric.call(((GoogleJsonResponseException) e.getCause()).getDetails().getCode());
      }
      throw e;
    }
  }

  GoogleCloudStorage createGoogleCloudStorage(
      GoogleCloudStorageOptions options, Storage storage, Credentials credentials) {
    return new GoogleCloudStorageImpl(options, storage, credentials);
  }

  /** Returns whether the GCS bucket exists and is accessible. */
  public boolean bucketAccessible(GcsPath path) throws IOException {
    return bucketAccessible(path, createBackOff(), Sleeper.DEFAULT);
  }

  /**
   * Returns the project number of the project which owns this bucket. If the bucket exists, it must
   * be accessible otherwise the permissions exception will be propagated. If the bucket does not
   * exist, an exception will be thrown.
   */
  public long bucketOwner(GcsPath path) throws IOException {
    return getBucket(path, createBackOff(), Sleeper.DEFAULT).getProjectNumber().longValue();
  }

  /**
   * Creates a {@link Bucket} under the specified project in Cloud Storage or propagates an
   * exception.
   */
  public void createBucket(String projectId, Bucket bucket) throws IOException {
    createBucket(projectId, bucket, createBackOff(), Sleeper.DEFAULT);
  }

  /**
   * Returns whether the GCS bucket exists. This will return false if the bucket is inaccessible due
   * to permissions.
   */
  @VisibleForTesting
  boolean bucketAccessible(GcsPath path, BackOff backoff, Sleeper sleeper) throws IOException {
    try {
      return getBucket(path, backoff, sleeper) != null;
    } catch (AccessDeniedException | FileNotFoundException e) {
      return false;
    }
  }

  @VisibleForTesting
  @Nullable
  Bucket getBucket(GcsPath path, BackOff backoff, Sleeper sleeper) throws IOException {
    Storage.Buckets.Get getBucket = storageClient.buckets().get(path.getBucket());

    try {
      return ResilientOperation.retry(
          getBucket::execute,
          backoff,
          new RetryDeterminer<IOException>() {
            @Override
            public boolean shouldRetry(IOException e) {
              if (errorExtractor.itemNotFound(e) || errorExtractor.accessDenied(e)) {
                return false;
              }
              return RetryDeterminer.SOCKET_ERRORS.shouldRetry(e);
            }
          },
          IOException.class,
          sleeper);
    } catch (GoogleJsonResponseException e) {
      if (errorExtractor.accessDenied(e)) {
        throw new AccessDeniedException(path.toString(), null, e.getMessage());
      }
      if (errorExtractor.itemNotFound(e)) {
        throw new FileNotFoundException(e.getMessage());
      }
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(
          String.format(
              "Error while attempting to verify existence of bucket gs://%s", path.getBucket()),
          e);
    }
  }

  @VisibleForTesting
  void createBucket(String projectId, Bucket bucket, BackOff backoff, Sleeper sleeper)
      throws IOException {
    Storage.Buckets.Insert insertBucket = storageClient.buckets().insert(projectId, bucket);
    insertBucket.setPredefinedAcl("projectPrivate");
    insertBucket.setPredefinedDefaultObjectAcl("projectPrivate");

    try {
      ResilientOperation.retry(
          insertBucket::execute,
          backoff,
          new RetryDeterminer<IOException>() {
            @Override
            public boolean shouldRetry(IOException e) {
              if (errorExtractor.itemAlreadyExists(e) || errorExtractor.accessDenied(e)) {
                return false;
              }
              return RetryDeterminer.SOCKET_ERRORS.shouldRetry(e);
            }
          },
          IOException.class,
          sleeper);
      return;
    } catch (GoogleJsonResponseException e) {
      if (errorExtractor.accessDenied(e)) {
        throw new AccessDeniedException(bucket.getName(), null, e.getMessage());
      }
      if (errorExtractor.itemAlreadyExists(e)) {
        throw new FileAlreadyExistsException(bucket.getName(), null, e.getMessage());
      }
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(
          String.format(
              "Error while attempting to create bucket gs://%s for project %s",
              bucket.getName(), projectId),
          e);
    }
  }

  private static void executeBatches(List<BatchInterface> batches) throws IOException {
    ExecutorService executor =
        MoreExecutors.listeningDecorator(
            new ThreadPoolExecutor(
                MAX_CONCURRENT_BATCHES,
                MAX_CONCURRENT_BATCHES,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>()));

    List<CompletionStage<Void>> futures = new ArrayList<>();
    for (final BatchInterface batch : batches) {
      futures.add(MoreFutures.runAsync(() -> batch.execute(), executor));
    }

    try {
      MoreFutures.get(MoreFutures.allAsList(futures));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while executing batch GCS request", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof FileNotFoundException) {
        throw (FileNotFoundException) e.getCause();
      }
      throw new IOException("Error executing batch GCS request", e);
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Makes get {@link BatchInterface BatchInterfaces}.
   *
   * @param paths {@link GcsPath GcsPaths}.
   * @param results mutable {@link List} for return values.
   * @return {@link BatchInterface BatchInterfaces} to execute.
   * @throws IOException
   */
  @VisibleForTesting
  List<BatchInterface> makeGetBatches(
      Collection<GcsPath> paths, List<StorageObjectOrIOException[]> results) throws IOException {
    List<BatchInterface> batches = new ArrayList<>();
    for (List<GcsPath> filesToGet :
        Lists.partition(Lists.newArrayList(paths), MAX_REQUESTS_PER_BATCH)) {
      BatchInterface batch = batchRequestSupplier.get();
      for (GcsPath path : filesToGet) {
        results.add(enqueueGetFileSize(path, batch));
      }
      batches.add(batch);
    }
    return batches;
  }

  /**
   * Wrapper for rewriting that supports multiple calls as well as possibly deleting the source
   * file.
   *
   * <p>Usage: create, enqueue(), and execute batch. Then, check getReadyToEnqueue() if another
   * round of enqueue() and execute is required. Repeat until getReadyToEnqueue() returns false.
   */
  class RewriteOp extends JsonBatchCallback<RewriteResponse> {
    private final GcsPath from;
    private final GcsPath to;
    private final boolean deleteSource;
    private final boolean ignoreMissingSource;
    private boolean readyToEnqueue;
    private boolean performDelete;
    private GoogleJsonError lastError;
    @VisibleForTesting Storage.Objects.Rewrite rewriteRequest;

    public boolean getReadyToEnqueue() {
      return readyToEnqueue;
    }

    public GoogleJsonError getLastError() {
      return lastError;
    }

    public GcsPath getFrom() {
      return from;
    }

    public GcsPath getTo() {
      return to;
    }

    public void enqueue(BatchInterface batch) throws IOException {
      if (!readyToEnqueue) {
        throw new IOException(
            String.format(
                "Invalid state for Rewrite, from=%s, to=%s, readyToEnqueue=%s",
                from, to, readyToEnqueue));
      }
      if (performDelete) {
        Storage.Objects.Delete deleteRequest =
            storageClient.objects().delete(from.getBucket(), from.getObject());
        batch.queue(
            deleteRequest,
            new JsonBatchCallback<Void>() {
              @Override
              public void onSuccess(Void obj, HttpHeaders responseHeaders) {
                LOG.debug("Successfully deleted {} after moving to {}", from, to);
                readyToEnqueue = false;
                lastError = null;
              }

              @Override
              public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders)
                  throws IOException {
                if (e.getCode() == 404) {
                  LOG.info(
                      "Ignoring failed deletion of moved file {} which already does not exist: {}",
                      from,
                      e);
                  readyToEnqueue = false;
                  lastError = null;
                } else {
                  readyToEnqueue = true;
                  lastError = e;
                }
              }
            });
      } else {
        batch.queue(rewriteRequest, this);
      }
    }

    public RewriteOp(GcsPath from, GcsPath to, boolean deleteSource, boolean ignoreMissingSource)
        throws IOException {
      this.from = from;
      this.to = to;
      this.deleteSource = deleteSource;
      this.ignoreMissingSource = ignoreMissingSource;
      rewriteRequest =
          storageClient
              .objects()
              .rewrite(from.getBucket(), from.getObject(), to.getBucket(), to.getObject(), null);
      if (maxBytesRewrittenPerCall != null) {
        rewriteRequest.setMaxBytesRewrittenPerCall(maxBytesRewrittenPerCall);
      }
      readyToEnqueue = true;
    }

    @Override
    public void onSuccess(RewriteResponse rewriteResponse, HttpHeaders responseHeaders)
        throws IOException {
      lastError = null;
      if (rewriteResponse.getDone()) {
        if (deleteSource) {
          readyToEnqueue = true;
          performDelete = true;
        } else {
          readyToEnqueue = false;
        }
      } else {
        LOG.debug(
            "Rewrite progress: {} of {} bytes, {} to {}",
            rewriteResponse.getTotalBytesRewritten(),
            rewriteResponse.getObjectSize(),
            from,
            to);
        rewriteRequest.setRewriteToken(rewriteResponse.getRewriteToken());
        readyToEnqueue = true;
        if (numRewriteTokensUsed != null) {
          numRewriteTokensUsed.incrementAndGet();
        }
      }
    }

    @Override
    public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException {
      if (e.getCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
        if (ignoreMissingSource) {
          // Treat a missing source as a successful rewrite.
          readyToEnqueue = false;
          lastError = null;
        } else {
          throw new FileNotFoundException(from.toString());
        }
      } else {
        lastError = e;
        readyToEnqueue = true;
      }
    }
  }

  public void copy(Iterable<String> srcFilenames, Iterable<String> destFilenames)
      throws IOException {
    rewriteHelper(
        srcFilenames,
        destFilenames,
        /*deleteSource=*/ false,
        /*ignoreMissingSource=*/ false,
        /*ignoreExistingDest=*/ false);
  }

  public void rename(
      Iterable<String> srcFilenames, Iterable<String> destFilenames, MoveOptions... moveOptions)
      throws IOException {
    // Rename is implemented as a rewrite followed by deleting the source. If the new object is in
    // the same location, the copy is a metadata-only operation.
    Set<MoveOptions> moveOptionSet = Sets.newHashSet(moveOptions);
    final boolean ignoreMissingSrc =
        moveOptionSet.contains(StandardMoveOptions.IGNORE_MISSING_FILES);
    final boolean ignoreExistingDest =
        moveOptionSet.contains(StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);
    rewriteHelper(
        srcFilenames, destFilenames, /*deleteSource=*/ true, ignoreMissingSrc, ignoreExistingDest);
  }

  private void rewriteHelper(
      Iterable<String> srcFilenames,
      Iterable<String> destFilenames,
      boolean deleteSource,
      boolean ignoreMissingSource,
      boolean ignoreExistingDest)
      throws IOException {
    LinkedList<RewriteOp> rewrites =
        makeRewriteOps(
            srcFilenames, destFilenames, deleteSource, ignoreMissingSource, ignoreExistingDest);
    org.apache.beam.sdk.util.BackOff backoff = BACKOFF_FACTORY.backoff();
    while (true) {
      List<BatchInterface> batches = makeRewriteBatches(rewrites); // Removes completed rewrite ops.
      if (batches.isEmpty()) {
        break;
      }
      RewriteOp sampleErrorOp =
          rewrites.stream().filter(op -> op.getLastError() != null).findFirst().orElse(null);
      if (sampleErrorOp != null) {
        long backOffMillis = backoff.nextBackOffMillis();
        if (backOffMillis == org.apache.beam.sdk.util.BackOff.STOP) {
          throw new IOException(
              String.format(
                  "Error completing file copies with retries, sample: from %s to %s due to %s",
                  sampleErrorOp.getFrom().toString(),
                  sampleErrorOp.getTo().toString(),
                  sampleErrorOp.getLastError()));
        }
        LOG.warn(
            "Retrying with backoff unsuccessful copy requests, sample request: from {} to {} due to {}",
            sampleErrorOp.getFrom(),
            sampleErrorOp.getTo(),
            sampleErrorOp.getLastError());
        try {
          Thread.sleep(backOffMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(
              String.format(
                  "Interrupted backoff of file copies with retries, sample: from %s to %s due to %s",
                  sampleErrorOp.getFrom().toString(),
                  sampleErrorOp.getTo().toString(),
                  sampleErrorOp.getLastError()));
        }
      }
      executeBatches(batches);
    }
  }

  LinkedList<RewriteOp> makeRewriteOps(
      Iterable<String> srcFilenames,
      Iterable<String> destFilenames,
      boolean deleteSource,
      boolean ignoreMissingSource,
      boolean ignoreExistingDest)
      throws IOException {
    List<String> srcList = Lists.newArrayList(srcFilenames);
    List<String> destList = Lists.newArrayList(destFilenames);
    checkArgument(
        srcList.size() == destList.size(),
        "Number of source files %s must equal number of destination files %s",
        srcList.size(),
        destList.size());
    LinkedList<RewriteOp> rewrites = Lists.newLinkedList();
    for (int i = 0; i < srcList.size(); i++) {
      final GcsPath sourcePath = GcsPath.fromUri(srcList.get(i));
      final GcsPath destPath = GcsPath.fromUri(destList.get(i));
      if (ignoreExistingDest && !sourcePath.getBucket().equals(destPath.getBucket())) {
        throw new UnsupportedOperationException(
            "Skipping dest existence is only supported within a bucket.");
      }
      rewrites.addLast(new RewriteOp(sourcePath, destPath, deleteSource, ignoreMissingSource));
    }
    return rewrites;
  }

  List<BatchInterface> makeRewriteBatches(LinkedList<RewriteOp> rewrites) throws IOException {
    List<BatchInterface> batches = new ArrayList<>();
    BatchInterface batch = batchRequestSupplier.get();
    Iterator<RewriteOp> it = rewrites.iterator();
    while (it.hasNext()) {
      RewriteOp rewrite = it.next();
      if (!rewrite.getReadyToEnqueue()) {
        it.remove();
        continue;
      }
      rewrite.enqueue(batch);

      if (batch.size() >= MAX_REQUESTS_PER_BATCH) {
        batches.add(batch);
        batch = batchRequestSupplier.get();
      }
    }
    if (batch.size() > 0) {
      batches.add(batch);
    }
    return batches;
  }

  List<BatchInterface> makeRemoveBatches(Collection<String> filenames) throws IOException {
    List<BatchInterface> batches = new ArrayList<>();
    for (List<String> filesToDelete :
        Lists.partition(Lists.newArrayList(filenames), MAX_REQUESTS_PER_BATCH)) {
      BatchInterface batch = batchRequestSupplier.get();
      for (String file : filesToDelete) {
        enqueueDelete(GcsPath.fromUri(file), batch);
      }
      batches.add(batch);
    }
    return batches;
  }

  public void remove(Collection<String> filenames) throws IOException {
    // TODO(https://github.com/apache/beam/issues/19859): It would be better to add per-file retries
    // and backoff
    // instead of failing everything if a single operation fails.
    executeBatches(makeRemoveBatches(filenames));
  }

  private StorageObjectOrIOException[] enqueueGetFileSize(final GcsPath path, BatchInterface batch)
      throws IOException {
    final StorageObjectOrIOException[] ret = new StorageObjectOrIOException[1];

    Storage.Objects.Get getRequest =
        storageClient.objects().get(path.getBucket(), path.getObject());
    batch.queue(
        getRequest,
        new JsonBatchCallback<StorageObject>() {
          @Override
          public void onSuccess(StorageObject response, HttpHeaders httpHeaders)
              throws IOException {
            ret[0] = StorageObjectOrIOException.create(response);
          }

          @Override
          public void onFailure(GoogleJsonError e, HttpHeaders httpHeaders) throws IOException {
            IOException ioException;
            if (e.getCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
              ioException = new FileNotFoundException(path.toString());
            } else {
              ioException = new IOException(String.format("Error trying to get %s: %s", path, e));
            }
            ret[0] = StorageObjectOrIOException.create(ioException);
          }
        });
    return ret;
  }

  /** A class that holds either a {@link StorageObject} or an {@link IOException}. */
  // It is clear from the name that this class holds either StorageObject or IOException.
  @SuppressFBWarnings("NM_CLASS_NOT_EXCEPTION")
  @AutoValue
  public abstract static class StorageObjectOrIOException {

    /** Returns the {@link StorageObject}. */
    public abstract @Nullable StorageObject storageObject();

    /** Returns the {@link IOException}. */
    public abstract @Nullable IOException ioException();

    @VisibleForTesting
    public static StorageObjectOrIOException create(StorageObject storageObject) {
      return new AutoValue_GcsUtil_StorageObjectOrIOException(
          checkNotNull(storageObject, "storageObject"), null /* ioException */);
    }

    @VisibleForTesting
    public static StorageObjectOrIOException create(IOException ioException) {
      return new AutoValue_GcsUtil_StorageObjectOrIOException(
          null /* storageObject */, checkNotNull(ioException, "ioException"));
    }
  }

  private void enqueueDelete(final GcsPath file, BatchInterface batch) throws IOException {
    Storage.Objects.Delete deleteRequest =
        storageClient.objects().delete(file.getBucket(), file.getObject());
    batch.queue(
        deleteRequest,
        new JsonBatchCallback<Void>() {
          @Override
          public void onSuccess(Void obj, HttpHeaders responseHeaders) {
            LOG.debug("Successfully deleted {}", file);
          }

          @Override
          public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException {
            if (e.getCode() == 404) {
              LOG.info(
                  "Ignoring failed deletion of file {} which already does not exist: {}", file, e);
            } else {
              throw new IOException(String.format("Error trying to delete %s: %s", file, e));
            }
          }
        });
  }

  @VisibleForTesting
  interface BatchInterface {
    <T> void queue(AbstractGoogleJsonClientRequest<T> request, JsonBatchCallback<T> cb)
        throws IOException;

    void execute() throws IOException;

    int size();
  }
}
