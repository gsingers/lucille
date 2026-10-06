package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_ANONYMOUS;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;

import com.kmwllc.lucille.connector.FileConnectorStateManager;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import com.kmwllc.lucille.core.FailureClass;
import com.kmwllc.lucille.core.SourceException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.retries.DefaultRetryStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * A storage client for S3. Create using a configuration (commonly mapped to <b>s3</b>) which can contain
 * "region" and can contain <b>both</b> "accessKeyId" and "secretAccessKey". Set "anonymous" instead to send
 * unsigned requests to public buckets. This cannot be combined with "accessKeyId" / "secretAccessKey", and
 * defaults the region to us-east-1 when no "region" is given.
 */
public class S3StorageClient extends BaseStorageClient {

  private static final Logger log = LoggerFactory.getLogger(S3StorageClient.class);

  // us-east-1 acts as S3's global endpoint and redirects to the bucket's real region.
  static final Region ANONYMOUS_DEFAULT_REGION = Region.US_EAST_1;

  protected S3Client s3;

  public S3StorageClient(Config s3CloudOptions) {
    super(s3CloudOptions);
  }

  @Override
  protected void validateOptions(Config config) {
    if (config.hasPath(S3_ACCESS_KEY_ID) ^ config.hasPath(S3_SECRET_ACCESS_KEY)) {
      throw new IllegalArgumentException("'" + S3_ACCESS_KEY_ID + "' and '" + S3_SECRET_ACCESS_KEY +
          "' must be specified together or omitted together in Config for S3StorageClient.");
    }

    if (isAnonymous(config) && config.hasPath(S3_ACCESS_KEY_ID)) {
      throw new IllegalArgumentException("'" + S3_ANONYMOUS + "' cannot be combined with '" + S3_ACCESS_KEY_ID
          + "' / '" + S3_SECRET_ACCESS_KEY + "' in Config for S3StorageClient.");
    }
  }

  private static boolean isAnonymous(Config config) {
    return config.hasPath(S3_ANONYMOUS) && config.getBoolean(S3_ANONYMOUS);
  }

  @Override
  protected void initializeStorageClient() throws IOException {
    try {
      S3ClientBuilder builder = S3Client.builder();

      // Lucille retries a refused listing itself, for longer and with a count of the refusals that a distributed
      // crawl acts on. The SDK's own retries would hide the first few refusals of every burst from that count, so
      // they are turned off whenever Lucille's policy is on.
      if (SourceRetryPolicy.fromConfig(config).maxMillis() > 0) {
        builder = builder.overrideConfiguration(o -> o.retryStrategy(DefaultRetryStrategy.doNotRetry()));
      }

      if (config.hasPath(S3_REGION)) {
        builder = builder.region(Region.of(config.getString(S3_REGION)));
      } else if (isAnonymous(config)) {
        builder = builder.region(ANONYMOUS_DEFAULT_REGION);
      }

      // Unsigned requests, for public buckets that need no credentials at all.
      if (isAnonymous(config)) {
        builder = builder.credentialsProvider(AnonymousCredentialsProvider.create());
      } else if (config.hasPath(S3_ACCESS_KEY_ID) && config.hasPath(S3_SECRET_ACCESS_KEY)) {
        AwsBasicCredentials awsCred = AwsBasicCredentials.create(config.getString(S3_ACCESS_KEY_ID), config.getString(S3_SECRET_ACCESS_KEY));
        builder = builder.credentialsProvider(StaticCredentialsProvider.create(awsCred));
      }

      s3 = builder.build();
    } catch (Exception e) {
      throw new IOException("Error occurred building S3Client", e);
    }
  }

  @Override
  protected void shutdownStorageClient() throws IOException {
    if (s3 != null) {
      try {
        s3.close();
      } catch (Exception e) {
        throw new IOException("Error occurred closing S3Client", e);
      }
    }
  }

  @Override
  protected void traverseStorageClient(Publisher publisher, TraversalParams params, FileConnectorStateManager stateMgr) throws Exception {
    if (params.getBudget() != null) {
      traverseWithinBudget(publisher, params, stateMgr, getStartingDirectory(params));
    } else {
      traversePrefix(publisher, params, stateMgr, getStartingDirectory(params));
    }
  }

  /**
   * Traverses as traversePrefix does, but lists only as many prefixes as the budget allows and hands the rest back.
   * The prefixes still to be listed are kept on a stack, where traversePrefix keeps them in its recursion, so that
   * whatever is left when the budget runs out can be handed back. A prefix's objects are all published before any of
   * the prefixes beneath it is listed.
   */
  private void traverseWithinBudget(Publisher publisher, TraversalParams params, FileConnectorStateManager stateMgr,
      String startingPrefix) throws SourceException {
    TraversalBudget budget = params.getBudget();
    Deque<String> prefixes = new ArrayDeque<>();
    prefixes.push(startingPrefix);

    while (!prefixes.isEmpty()) {
      String prefix = prefixes.pop();

      if (!budget.mayList()) {
        budget.handBack(uriForDirectory(prefix, params));
        continue;
      }

      List<String> prefixesBeneath;
      try {
        prefixesBeneath = listPrefix(publisher, params, stateMgr, prefix);
      } catch (SourceException e) {
        // The source has failed on this prefix for good. The prefix is handed back unlisted, to be tried again as a
        // unit of its own, and so, as the stack unwinds, is everything else this unit had still to list.
        log.warn("Giving up on listing {} ({}); handing it and the rest of the unit back.", prefix, e.getFailureClass(), e);
        budget.sourceError(e.getFailureClass(), e);
        budget.listingFailed();
        budget.handBack(uriForDirectory(prefix, params));
        continue;
      }

      // pushed last first, so that they are listed in the order the store returned them
      for (int i = prefixesBeneath.size() - 1; i >= 0; i--) {
        prefixes.push(prefixesBeneath.get(i));
      }
    }
  }

  private void traversePrefix(Publisher publisher, TraversalParams params, FileConnectorStateManager stateMgr, String prefix)
      throws SourceException {
    for (String beneath : listPrefix(publisher, params, stateMgr, prefix)) {
      traversePrefix(publisher, params, stateMgr, beneath);
    }
  }

  /**
   * Lists one prefix, publishing its objects, and returns the prefixes beneath it that are to be traversed. A
   * request the store refuses or cannot answer is tried again for as long as the params' retry policy allows; the
   * objects published before the failure are published again on the retry, under the same IDs.
   *
   * @throws SourceException once the retries are used up, saying what kind of failure it was.
   */
  private List<String> listPrefix(Publisher publisher, TraversalParams params, FileConnectorStateManager stateMgr,
      String prefix) throws SourceException {
    ListObjectsV2Request request = ListObjectsV2Request.builder()
        .bucket(getBucketOrContainerName(params))
        .prefix(prefix)
        .delimiter("/")
        .maxKeys(maxNumOfPages)
        .build();
    TraversalBudget budget = params.getBudget();
    long firstFailureMillis = 0;

    for (int retry = 0; ; retry++) {
      List<String> prefixesBeneath = new ArrayList<>();
      try {
        s3.listObjectsV2Paginator(request).stream().forEachOrdered(resp -> {
          resp.contents().forEach(obj -> {
            S3FileReference fileRef = new S3FileReference(obj, params);
            processAndPublishFileIfValid(publisher, fileRef, params, stateMgr);
          });

          if (params.isRecursive()) {
            resp.commonPrefixes().forEach(cp -> {
              if (!isSkippedDirectory(uriForDirectory(cp.prefix(), params), params)) {
                prefixesBeneath.add(cp.prefix());
              }
            });
          }
        });
        return prefixesBeneath;
      } catch (SdkException e) {
        FailureClass failureClass = classify(e);
        if (retry == 0) {
          firstFailureMillis = System.currentTimeMillis();
        }
        if (budget != null) {
          budget.callRefused();
        }

        long waitMillis = failureClass.isOverload() ? params.getRetryPolicy().nextWaitMillis(retry + 1, firstFailureMillis) : -1;
        if (waitMillis < 0 || (budget != null && budget.isCancelled())) {
          throw new SourceException(failureClass, "Could not list " + uriForDirectory(prefix, params) + " after "
              + retry + " retries.", e);
        }

        log.info("Listing {} was refused ({}); retry {} in {} ms.", prefix, failureClass, retry + 1, waitMillis);
        if (!sleepUnlessCancelled(waitMillis, budget)) {
          // interrupted: the thread is being stopped, and must not spin on the SDK until the horizon
          throw new SourceException(failureClass, "Interrupted while waiting to list " + uriForDirectory(prefix, params)
              + " again.", e);
        }
      }
    }
  }

  /** Returns false if interrupted; the interrupt flag is left set. */
  private static boolean sleepUnlessCancelled(long millis, TraversalBudget budget) {
    long deadline = System.currentTimeMillis() + millis;
    try {
      while (System.currentTimeMillis() < deadline && (budget == null || !budget.isCancelled())) {
        Thread.sleep(Math.min(200, Math.max(1, deadline - System.currentTimeMillis())));
      }
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * Fetches an object, trying again on a refusal as a listing is tried again. A traversal that gives up on an
   * object's content skips the object (the base client logs it); the refusals are counted all the same.
   */
  private <T> T getWithRetry(TraversalParams params, String key, Supplier<T> fetch) {
    TraversalBudget budget = params.getBudget();
    long firstFailureMillis = 0;

    for (int retry = 0; ; retry++) {
      try {
        return fetch.get();
      } catch (SdkException e) {
        FailureClass failureClass = classify(e);
        if (retry == 0) {
          firstFailureMillis = System.currentTimeMillis();
        }
        if (budget != null) {
          budget.callRefused();
        }
        long waitMillis = failureClass.isOverload() ? params.getRetryPolicy().nextWaitMillis(retry + 1, firstFailureMillis) : -1;
        if (waitMillis < 0 || (budget != null && budget.isCancelled())) {
          throw e;
        }
        log.info("Fetching {} was refused ({}); retry {} in {} ms.", key, failureClass, retry + 1, waitMillis);
        if (!sleepUnlessCancelled(waitMillis, budget)) {
          throw e;
        }
      }
    }
  }

  /**
   * Says what kind of failure an SDK exception is. Throttling is the store asking for a lower rate; a 5xx or a
   * connection failure is the store being unavailable; anything else it answered with is an error of the request.
   */
  static FailureClass classify(SdkException e) {
    if (e instanceof AwsServiceException service) {
      if (service.isThrottlingException() || service.statusCode() == 429) {
        return FailureClass.THROTTLED;
      }
      if (service.statusCode() == 500 || service.statusCode() == 502 || service.statusCode() == 503 || service.statusCode() == 504) {
        return FailureClass.SOURCE_UNAVAILABLE;
      }
      return FailureClass.SOURCE_ERROR;
    }
    // The SDK could not get an answer at all. Only a failure on the wire (a connection refused, a timeout, a reset)
    // is the store being unavailable; a failure to sign, to find credentials or to read the config is not, and
    // would not pass if tried again.
    for (Throwable cause = e; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
      if (cause instanceof IOException) {
        return FailureClass.SOURCE_UNAVAILABLE;
      }
    }
    return FailureClass.SOURCE_ERROR;
  }

  @Override
  public List<URI> listSubdirectories(URI path, TraversalParams params) {
    ListObjectsV2Request request = ListObjectsV2Request.builder()
        .bucket(path.getAuthority())
        .prefix(startingDirectory(path))
        .delimiter("/")
        .maxKeys(maxNumOfPages)
        .build();

    // the path is in the same bucket as the traversal being split, so its prefixes resolve against the same URI
    return s3.listObjectsV2Paginator(request).commonPrefixes().stream()
        .map(cp -> uriForDirectory(cp.prefix(), params))
        .filter(prefixUri -> !isSkippedDirectory(prefixUri, params))
        .toList();
  }

  private URI uriForDirectory(String prefix, TraversalParams params) {
    URI paramsUri = params.getURI();
    try {
      return new URI(paramsUri.getScheme(), paramsUri.getAuthority(), "/" + prefix, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Unable to build S3 URI for prefix: " + prefix, e);
    }
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) throws IOException {
    String bucketName = uri.getAuthority();
    String objectKey = uri.getPath().substring(1);

    GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(objectKey).build();
    return s3.getObject(request);
  }

  @Override
  public void moveFile(URI filePath, URI folder) throws IOException {
    String sourceBucket = filePath.getAuthority();
    String sourceKey = filePath.getPath().substring(1);

    String destBucket = folder.getAuthority();
    String destKey = folder.getPath().substring(1) + sourceKey;

    CopyObjectRequest copyRequest = CopyObjectRequest.builder()
        .sourceBucket(sourceBucket).sourceKey(sourceKey)
        .destinationBucket(destBucket).destinationKey(destKey)
        .build();

    DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
        .bucket(sourceBucket).key(sourceKey)
        .build();

    s3.copyObject(copyRequest);
    s3.deleteObject(deleteRequest);
  }

  private String getStartingDirectory(TraversalParams params) {
    return startingDirectory(params.getURI());
  }

  private static String startingDirectory(URI pathURI) {
    String startingDirectory = Objects.equals(pathURI.getPath(), "/") ? "" : pathURI.getPath();
    if (startingDirectory.startsWith("/")) {
      return startingDirectory.substring(1);
    }
    return startingDirectory;
  }

  private String getBucketOrContainerName(TraversalParams params) {
    return params.getURI().getAuthority();
  }

  // Only for testing
  void setS3ClientForTesting(S3Client s3) {
    this.s3 = s3;
  }


  private class S3FileReference extends BaseFileReference {

    private final S3Object s3Obj;

    public S3FileReference(S3Object s3Obj, TraversalParams params) {
      // These are inexpensive calls - information stored in the s3 object.
      // "null" for creation time - this isn't available in a S3Object
      super(getFullPathHelper(s3Obj, params), s3Obj.lastModified(), s3Obj.size(), null);

      this.s3Obj = s3Obj;
    }

    @Override
    public String getName() {
      return s3Obj.key();
    }

    @Override
    public boolean isValidFile() {
      return !s3Obj.key().endsWith("/");
    }

    @Override
    public InputStream getContentStream(TraversalParams params) {
      String objKey = s3Obj.key();
      GetObjectRequest objectRequest = GetObjectRequest.builder().bucket(getBucketOrContainerName(params)).key(objKey).build();
      return getWithRetry(params, objKey, () -> s3.getObject(objectRequest));
    }

    @Override
    protected byte[] getFileContent(TraversalParams params) {
      GetObjectRequest request = GetObjectRequest.builder().bucket(getBucketOrContainerName(params)).key(s3Obj.key()).build();
      return getWithRetry(params, s3Obj.key(), () -> s3.getObjectAsBytes(request).asByteArray());
    }

    private static URI getFullPathHelper(S3Object s3Obj, TraversalParams params) {
      URI paramsURI = params.getURI();

      try {
        return new URI(paramsURI.getScheme(), paramsURI.getAuthority(), "/" + s3Obj.key(), null);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to build S3 URI for key: " + s3Obj.key(), e);
      }
    }
  }
}
