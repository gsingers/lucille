package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_ANONYMOUS;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;
import static com.kmwllc.lucille.connector.FileConnector.S3_TRAVERSAL_THREADS;

import com.kmwllc.lucille.connector.FileConnectorStateManager;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
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

  // directories listed at once; 1 walks the tree on the calling thread
  private final int traversalThreads;

  public S3StorageClient(Config s3CloudOptions) {
    super(s3CloudOptions);
    this.traversalThreads = s3CloudOptions.hasPath(S3_TRAVERSAL_THREADS) ? s3CloudOptions.getInt(S3_TRAVERSAL_THREADS) : 1;
  }

  @Override
  protected void validateOptions(Config config) {
    if (config.hasPath(S3_ACCESS_KEY_ID) ^ config.hasPath(S3_SECRET_ACCESS_KEY)) {
      throw new IllegalArgumentException("'" + S3_ACCESS_KEY_ID + "' and '" + S3_SECRET_ACCESS_KEY +
          "' must be specified together or omitted together in Config for S3StorageClient.");
    }

    if (config.hasPath(S3_TRAVERSAL_THREADS) && config.getInt(S3_TRAVERSAL_THREADS) < 1) {
      throw new IllegalArgumentException("'" + S3_TRAVERSAL_THREADS + "' must be at least 1 in Config for S3StorageClient.");
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

      if (traversalThreads > 1) {
        // the default pool holds 50 connections, which would cap the listings in flight below the thread count
        builder = builder.httpClientBuilder(Apache5HttpClient.builder().maxConnections(traversalThreads));
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
    if (traversalThreads == 1) {
      traversePrefix(publisher, params, stateMgr, getStartingDirectory(params));
      return;
    }

    // the state database binds a connection to each thread, so each traversal thread opens its own
    ParallelTreeWalker.ThreadResource state = stateMgr == null ? ParallelTreeWalker.NO_THREAD_RESOURCE : () -> {
      stateMgr.openStateForThread();
      return stateMgr::closeStateForThread;
    };
    new ParallelTreeWalker(traversalThreads, "S3Traversal", state).walk(getStartingDirectory(params), lister(params),
        prefix -> !isSkippedDirectory(uriForDirectory(prefix, params), params),
        obj -> processAndPublishFileIfValid(publisher, new S3FileReference(obj, params), params, stateMgr));
  }

  private ParallelTreeWalker.Lister<S3Object> lister(TraversalParams params) {
    return new ParallelTreeWalker.Lister<>() {
      @Override
      public ParallelTreeWalker.Page<S3Object> list(String prefix, String startAfter, String token) {
        ListObjectsV2Response response = s3.listObjectsV2(ListObjectsV2Request.builder()
            .bucket(getBucketOrContainerName(params))
            .prefix(prefix)
            .delimiter("/")
            .maxKeys(maxNumOfPages)
            .startAfter(token == null ? startAfter : null)
            .continuationToken(token)
            .build());
        return new ParallelTreeWalker.Page<>(response.contents(),
            response.commonPrefixes().stream().map(CommonPrefix::prefix).toList(),
            Boolean.TRUE.equals(response.isTruncated()) ? response.nextContinuationToken() : null);
      }

      @Override
      public String key(S3Object obj) {
        return obj.key();
      }
    };
  }

  private void traversePrefix(Publisher publisher, TraversalParams params, FileConnectorStateManager stateMgr, String prefix) {
    ListObjectsV2Request request = ListObjectsV2Request.builder()
        .bucket(getBucketOrContainerName(params))
        .prefix(prefix)
        .delimiter("/")
        .maxKeys(maxNumOfPages)
        .build();

    s3.listObjectsV2Paginator(request).stream().forEachOrdered(resp -> {
      resp.contents().forEach(obj -> {
        S3FileReference fileRef = new S3FileReference(obj, params);
        processAndPublishFileIfValid(publisher, fileRef, params, stateMgr);
      });

      resp.commonPrefixes().forEach(cp -> {
        URI prefixUri = uriForDirectory(cp.prefix(), params);
        if (!isSkippedDirectory(prefixUri, params)) {
          traversePrefix(publisher, params, stateMgr, cp.prefix());
        }
      });
    });
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
    URI pathURI = params.getURI();
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
      return s3.getObject(objectRequest);
    }

    @Override
    protected byte[] getFileContent(TraversalParams params) {
      return s3.getObjectAsBytes(
          GetObjectRequest.builder().bucket(getBucketOrContainerName(params)).key(s3Obj.key()).build()
      ).asByteArray();
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
