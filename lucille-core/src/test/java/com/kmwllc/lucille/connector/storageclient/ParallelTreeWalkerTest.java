package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.connector.storageclient.ParallelTreeWalker.Page;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class ParallelTreeWalkerTest {

  /** A tree of nested directories plus large flat directories whose names take several shapes. */
  private static FakeBucket bucket() {
    Random random = new Random(11);
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      String dir = "d" + random.nextInt(20) + "/s" + random.nextInt(10) + "/";
      keys.add(dir + "f" + i + ".txt");
    }
    for (int i = 0; i < 4_000; i++) {
      keys.add("big/" + UUID.nameUUIDFromBytes(new byte[] {(byte) i, (byte) (i >> 8)}) + ".bin");
      keys.add("big/IMG_" + String.format("%05d", i) + ".JPG");
    }
    for (int i = 0; i < 1_000; i++) {
      keys.add("big/Report " + (char) ('A' + random.nextInt(26)) + random.nextInt(100_000) + ".pdf");
      keys.add("big/z~" + Integer.toHexString(random.nextInt()) + "/inner.txt");
    }
    keys.add("top.txt");
    return new FakeBucket(keys);
  }

  private static ParallelTreeWalker.Lister<S3Object> lister(FakeBucket bucket, int maxKeys) {
    return new ParallelTreeWalker.Lister<>() {
      @Override
      public Page<S3Object> list(String directory, String startAfter, String token) {
        ListObjectsV2Response r = bucket.list(ListObjectsV2Request.builder().prefix(directory).delimiter("/")
            .maxKeys(maxKeys).startAfter(startAfter).continuationToken(token).build());
        return new Page<>(r.contents(), r.commonPrefixes().stream().map(CommonPrefix::prefix).toList(),
            r.isTruncated() ? r.nextContinuationToken() : null);
      }

      @Override
      public String key(S3Object file) {
        return file.key();
      }
    };
  }

  @Test
  public void visitsEveryFileExactlyOnce() throws Exception {
    FakeBucket bucket = bucket();
    for (int threads : new int[] {1, 4, 32}) {
      ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
      new ParallelTreeWalker(threads, "test", ParallelTreeWalker.NO_THREAD_RESOURCE)
          .walk("", lister(bucket, 50), dir -> true, obj -> seen.add(obj.key()));

      assertEquals(threads + " threads: each file once", bucket.keys().size(), seen.size());
      assertEquals(threads + " threads: every file", bucket.keys(), new TreeSet<>(seen));
    }
  }

  @Test
  public void listsEachDirectoryOnceAndSkipsWhatDescendRejects() throws Exception {
    FakeBucket bucket = bucket();
    ConcurrentLinkedQueue<String> offered = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
    new ParallelTreeWalker(16, "test", ParallelTreeWalker.NO_THREAD_RESOURCE).walk("", lister(bucket, 50), dir -> {
      offered.add(dir);
      return !dir.startsWith("d3/");
    }, obj -> seen.add(obj.key()));

    Set<String> expectedDirs = bucket.keys().stream().flatMap(ParallelTreeWalkerTest::parents)
        .filter(dir -> !dir.startsWith("d3/") || dir.equals("d3/")).collect(Collectors.toSet());
    assertEquals("each directory offered once", expectedDirs.size(), offered.size());
    assertEquals(expectedDirs, new HashSet<>(offered));
    Set<String> expectedFiles = bucket.keys().stream().filter(k -> !k.startsWith("d3/")).collect(Collectors.toSet());
    assertEquals(expectedFiles, new HashSet<>(seen));
  }

  @Test
  public void splitsALargeDirectoryIntoRangesListedAtOnce() throws Exception {
    FakeBucket bucket = bucket();
    AtomicInteger startAfterCalls = new AtomicInteger();
    ParallelTreeWalker.Lister<S3Object> base = lister(bucket, 50);
    ParallelTreeWalker.Lister<S3Object> counting = new ParallelTreeWalker.Lister<>() {
      @Override
      public Page<S3Object> list(String directory, String startAfter, String token) throws Exception {
        if (startAfter != null) {
          startAfterCalls.incrementAndGet();
        }
        return base.list(directory, startAfter, token);
      }

      @Override
      public String key(S3Object file) {
        return file.key();
      }
    };
    new ParallelTreeWalker(8, "test", ParallelTreeWalker.NO_THREAD_RESOURCE)
        .walk("big/", counting, dir -> true, obj -> { });
    assertTrue("big/ was cut into ranges", startAfterCalls.get() > 10);
  }

  @Test
  public void rethrowsTheFirstListingFailureAndStops() {
    FakeBucket bucket = bucket();
    RuntimeException boom = new RuntimeException("boom");
    ParallelTreeWalker.Lister<S3Object> base = lister(bucket, 50);
    ParallelTreeWalker.Lister<S3Object> failing = new ParallelTreeWalker.Lister<>() {
      @Override
      public Page<S3Object> list(String directory, String startAfter, String token) throws Exception {
        if (directory.equals("d5/")) {
          throw boom;
        }
        return base.list(directory, startAfter, token);
      }

      @Override
      public String key(S3Object file) {
        return file.key();
      }
    };
    Exception thrown = assertThrows(Exception.class, () -> new ParallelTreeWalker(4, "test",
        ParallelTreeWalker.NO_THREAD_RESOURCE).walk("", failing, dir -> true, obj -> { }));
    assertSame(boom, thrown);
  }

  @Test
  public void opensAndClosesAResourceOnEveryWorkerThread() throws Exception {
    AtomicInteger opened = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();
    ConcurrentLinkedQueue<String> openedOn = new ConcurrentLinkedQueue<>();
    new ParallelTreeWalker(4, "test", () -> {
      opened.incrementAndGet();
      openedOn.add(Thread.currentThread().getName());
      return closed::incrementAndGet;
    }).walk("", lister(bucket(), 50), dir -> true, obj -> { });

    assertTrue(opened.get() >= 1 && opened.get() <= 4);
    assertEquals("every resource closed before walk returns", opened.get(), closed.get());
    assertFalse(openedOn.contains(Thread.currentThread().getName()));
  }

  @Test
  public void failingToOpenAThreadResourceFailsTheWalk() {
    IllegalStateException cannotOpen = new IllegalStateException("no connection");
    Exception thrown = assertThrows(Exception.class, () -> new ParallelTreeWalker(2, "test", () -> {
      throw cannotOpen;
    }).walk("", lister(bucket(), 50), dir -> true, obj -> { }));
    assertSame(cannotOpen, thrown);
  }

  @Test
  public void cutsLieStrictlyBetweenTheirBounds() {
    List<String> page = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      page.add("d/" + String.format("%08x", i * 7919) + ".log");
    }
    Collections.sort(page);
    String last = page.get(page.size() - 1);
    List<String> cuts = KeyRanges.cuts("d/", page, page.get(0), last, "d/ffff", 8);
    assertFalse(cuts.isEmpty());
    String previous = last;
    for (String cut : cuts) {
      assertTrue(cut + " after " + previous, cut.compareTo(previous) > 0);
      assertTrue(cut + " before the bound", cut.compareTo("d/ffff") < 0);
      previous = cut;
    }
  }

  @Test
  public void cutsContinueDecimalNamesAcrossACarry() {
    List<String> page = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      page.add("d/e" + String.format("%07d", i) + ".txt");
    }
    List<String> cuts = KeyRanges.cuts("d/", page, page.get(0), page.get(999), null, 4);
    assertEquals(3, cuts.size());
    // the next page's keys start at e0001000: the first cut must reach past it, not stop in e0000:...e0000~
    assertTrue(cuts.get(0), cuts.get(0).compareTo("d/e0001000.txt") > 0);
  }

  private static java.util.stream.Stream<String> parents(String key) {
    List<String> dirs = new ArrayList<>();
    for (int i = key.indexOf('/'); i >= 0; i = key.indexOf('/', i + 1)) {
      dirs.add(key.substring(0, i + 1));
    }
    return dirs.stream();
  }
}
