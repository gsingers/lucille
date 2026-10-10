package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.FILE_PATH;
import static com.kmwllc.lucille.connector.FileConnector.MAX_NUM_OF_PAGES;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_TRAVERSAL_THREADS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.connector.FileConnectorStateManager;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3StorageClientParallelTest {

  private static final Config TRAVERSAL = ConfigFactory.parseString("fileOptions { getFileContent: false }");

  private static FakeBucket bucket() {
    List<String> keys = new ArrayList<>();
    for (int d = 0; d < 30; d++) {
      for (int s = 0; s < 5; s++) {
        for (int f = 0; f < 4; f++) {
          keys.add("data/d" + d + "/s" + s + "/f" + f + ".txt");
        }
      }
    }
    for (int i = 0; i < 2_000; i++) {
      keys.add("data/big/IMG_" + String.format("%05d", i) + ".JPG");
    }
    keys.add("data/top.txt");
    keys.add("other/not-under-the-path.txt");
    return new FakeBucket(keys);
  }

  private static S3StorageClient client(FakeBucket bucket, int threads) {
    S3StorageClient client = new S3StorageClient(ConfigFactory.parseMap(Map.of(S3_REGION, "us-east-1",
        S3_TRAVERSAL_THREADS, threads, MAX_NUM_OF_PAGES, 50)));
    S3Client s3 = mock(S3Client.class);
    when(s3.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenAnswer(call -> bucket.list(call.getArgument(0)));
    client.setS3ClientForTesting(s3);
    client.initializeForTesting();
    return client;
  }

  private static Set<String> expectedPaths(FakeBucket bucket, String under, String skipped) {
    return bucket.keys().stream().filter(k -> k.startsWith(under) && (skipped == null || !k.startsWith(skipped)))
        .map(k -> "s3://bucket/" + k).collect(Collectors.toCollection(TreeSet::new));
  }

  @Test
  public void publishesEveryFileUnderThePathExactlyOnce() throws Exception {
    FakeBucket bucket = bucket();
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    client(bucket, 8).traverse(publisher, new TraversalParams(TRAVERSAL, URI.create("s3://bucket/data/"), ""));

    List<String> paths = messenger.getDocsSentForProcessing().stream().map(d -> d.getString(FILE_PATH)).toList();
    Set<String> expected = expectedPaths(bucket, "data/", null);
    assertEquals("each file once", expected.size(), paths.size());
    assertEquals(expected, new TreeSet<>(paths));
  }

  @Test
  public void doesNotListSkippedPaths() throws Exception {
    FakeBucket bucket = bucket();
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    Config skipping = TRAVERSAL.withFallback(
        ConfigFactory.parseString("filterOptions { pathsToSkip: [\"s3://bucket/data/big/\"] }"));

    client(bucket, 8).traverse(publisher, new TraversalParams(skipping, URI.create("s3://bucket/data/"), ""));

    Set<String> paths = messenger.getDocsSentForProcessing().stream().map(d -> d.getString(FILE_PATH))
        .collect(Collectors.toCollection(TreeSet::new));
    assertEquals(expectedPaths(bucket, "data/", "data/big/"), paths);
  }

  @Test
  public void bindsAStateToEachTraversalThread() throws Exception {
    FileConnectorStateManager stateMgr = mock(FileConnectorStateManager.class);
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), new TestMessenger(), "run1", "pipeline1");

    client(bucket(), 4).traverse(publisher, new TraversalParams(TRAVERSAL, URI.create("s3://bucket/data/"), ""),
        stateMgr);

    verify(stateMgr, atLeastOnce()).openStateForThread();
    long opened = countCalls(stateMgr, "openStateForThread");
    assertTrue("at most one state per thread", opened <= 4);
    assertEquals("every state closed", opened, countCalls(stateMgr, "closeStateForThread"));
    verify(stateMgr, atLeastOnce()).markFileEncountered(any());
  }

  @Test
  public void failsTheTraversalWhenAListingFails() throws Exception {
    FakeBucket bucket = bucket();
    S3Exception refused = (S3Exception) S3Exception.builder().message("refused").statusCode(503).build();
    S3StorageClient client = client(bucket, 4);
    S3Client s3 = mock(S3Client.class);
    when(s3.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(call -> {
      ListObjectsV2Request request = call.getArgument(0);
      if (request.prefix().equals("data/d7/")) {
        throw refused;
      }
      return bucket.list(request);
    });
    client.setS3ClientForTesting(s3);
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), new TestMessenger(), "run1", "pipeline1");

    Exception thrown = assertThrows(Exception.class,
        () -> client.traverse(publisher, new TraversalParams(TRAVERSAL, URI.create("s3://bucket/data/"), "")));
    assertSame(refused, thrown);
  }

  @Test
  public void rejectsFewerThanOneThread() {
    assertThrows(IllegalArgumentException.class,
        () -> new S3StorageClient(ConfigFactory.parseMap(Map.of(S3_TRAVERSAL_THREADS, 0))));
  }

  @Test
  public void buildsAClientWithAConnectionPerThread() throws Exception {
    S3StorageClient client = new S3StorageClient(ConfigFactory.parseMap(Map.of(S3_REGION, "us-east-1",
        S3_TRAVERSAL_THREADS, 200)));
    client.init();
    client.shutdown();
  }

  private static long countCalls(Object mock, String method) {
    return mockingDetails(mock).getInvocations().stream().filter(i -> i.getMethod().getName().equals(method)).count();
  }
}
