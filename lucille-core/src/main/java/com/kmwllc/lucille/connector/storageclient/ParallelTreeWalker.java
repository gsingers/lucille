package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.util.ThreadNameUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Walks a hierarchical listing (an S3 prefix with a "/" delimiter, for example) on a fixed pool of threads.
 *
 * <p>Each task lists one directory, or one key range of a directory, page by page. Subdirectories found on a page are
 * queued as soon as the page arrives, and the walk ends when no task is outstanding. Nothing ever waits on another
 * task, so every thread is either listing or idle.
 *
 * <p>A directory's pages can only be read one after another, so a very large directory would otherwise be a serial
 * chain that sets the length of the whole walk. When a page comes back truncated, the rest of the directory is cut
 * into key ranges (see {@link KeyRanges}) that are listed at once, each starting after its lower bound and stopping at
 * its upper bound.
 */
public final class ParallelTreeWalker {

  private static final Logger log = LoggerFactory.getLogger(ParallelTreeWalker.class);

  /** How many ranges the unlisted part of a truncated directory is cut into. */
  static final int RANGES_PER_CUT = 8;

  /** For a walk that needs nothing set up on its threads. */
  public static final ThreadResource NO_THREAD_RESOURCE = () -> () -> { };

  /** Lists one page of a directory. Called concurrently. */
  public interface Lister<E> {

    /**
     * Lists one page of the directory.
     *
     * @param directory the directory, as the store names it (an S3 prefix ending in "/")
     * @param startAfter if no token is given, list only entries whose keys sort after this; null for the beginning
     * @param continuationToken the token from the previous page, or null for a first page
     */
    Page<E> list(String directory, String startAfter, String continuationToken) throws Exception;

    /** The full key of a file entry, comparable with subdirectory names and with startAfter. */
    String key(E file);
  }

  /**
   * One page of a listing.
   *
   * @param files the files on this page
   * @param subdirectories the subdirectories on this page, named as {@link Lister#list} accepts them
   * @param nextToken the token for the next page, or null if this is the last
   */
  public record Page<E>(List<E> files, List<String> subdirectories, String nextToken) { }

  /** Something each worker thread holds for its lifetime, such as a connection bound to the thread. */
  @FunctionalInterface
  public interface ThreadResource {
    AutoCloseable open() throws Exception;
  }

  private final int threads;
  private final String name;
  private final ThreadResource threadResource;

  /**
   * @param threads the number of directories listed at once
   * @param name names the worker threads
   * @param threadResource opened on each worker thread when it starts and closed when it ends
   */
  public ParallelTreeWalker(int threads, String name, ThreadResource threadResource) {
    if (threads < 1) {
      throw new IllegalArgumentException("threads must be at least 1, was " + threads);
    }
    this.threads = threads;
    this.name = name;
    this.threadResource = threadResource;
  }

  /**
   * Walks the tree under root, handing every file to onFile from the worker threads. Returns once every worker thread
   * has ended and closed its resource.
   *
   * @param descend whether to list a subdirectory
   * @throws Exception the first failure, from a listing or from opening a thread's resource, after the walk has stopped
   */
  public <E> void walk(String root, Lister<E> lister, Predicate<String> descend, Consumer<E> onFile)
      throws Exception {
    Walk<E> walk = new Walk<>(lister, descend, onFile);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(), threadFactory(walk));
    walk.executor = executor;
    try {
      walk.submit(root, null, null);
      walk.done.get();
    } catch (ExecutionException e) {
      walk.fail(e.getCause() instanceof Exception cause ? cause : e);
    } finally {
      executor.shutdownNow();
      // interrupts idle workers; each closes its resource as it ends
      while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        log.info("Waiting for {} traversal threads to finish their listings.", executor.getActiveCount());
      }
    }
    Exception failure = walk.failure.get();
    if (failure != null) {
      throw failure;
    }
  }

  private ThreadFactory threadFactory(Walk<?> walk) {
    AtomicInteger count = new AtomicInteger();
    String prefix = ThreadNameUtils.createName(name);
    return worker -> new Thread(() -> {
      AutoCloseable resource = null;
      try {
        resource = threadResource.open();
      } catch (Exception e) {
        // still run the worker: its tasks see the walk has stopped and only count themselves done
        walk.fail(e);
      }
      try {
        worker.run();
      } finally {
        closeQuietly(resource);
      }
    }, prefix + "-" + count.incrementAndGet());
  }

  private static void closeQuietly(AutoCloseable resource) {
    if (resource == null) {
      return;
    }
    try {
      resource.close();
    } catch (Exception e) {
      log.warn("Error closing a traversal thread's resource.", e);
    }
  }

  /** One walk's shared state: what is outstanding, and the first failure, which stops further listings. */
  private static final class Walk<E> {
    final Lister<E> lister;
    final Predicate<String> descend;
    final Consumer<E> onFile;
    final AtomicLong outstanding = new AtomicLong();
    final CompletableFuture<Void> done = new CompletableFuture<>();
    final AtomicReference<Exception> failure = new AtomicReference<>();
    ThreadPoolExecutor executor;

    Walk(Lister<E> lister, Predicate<String> descend, Consumer<E> onFile) {
      this.lister = lister;
      this.descend = descend;
      this.onFile = onFile;
    }

    void fail(Exception e) {
      failure.compareAndSet(null, e);
    }

    boolean stopped() {
      return failure.get() != null;
    }

    /** Queues the listing of the keys of dir in (after, upTo]; null bounds are open. */
    void submit(String dir, String after, String upTo) {
      outstanding.incrementAndGet();
      executor.execute(() -> {
        try {
          listRange(dir, after, upTo);
        } catch (Exception e) {
          fail(e);
        } finally {
          if (outstanding.decrementAndGet() == 0) {
            done.complete(null);
          }
        }
      });
    }

    private void listRange(String dir, String after, String upTo) throws Exception {
      String token = null;
      while (!stopped()) {
        Page<E> page = lister.list(dir, token == null ? after : null, token);
        RangeScan scan = scan(page, after, upTo);
        if (scan.pastRange || page.nextToken() == null || scan.last == null) {
          return;
        }
        List<String> cuts = KeyRanges.cuts(dir, scan.keys, scan.first, scan.last, upTo, RANGES_PER_CUT);
        for (int i = 0; i < cuts.size(); i++) {
          submit(dir, cuts.get(i), i + 1 < cuts.size() ? cuts.get(i + 1) : upTo);
        }
        if (!cuts.isEmpty()) {
          upTo = cuts.get(0);
        }
        token = page.nextToken();
      }
    }

    /** Hands on the page's entries that fall in (after, upTo], and notes its key span. */
    private RangeScan scan(Page<E> page, String after, String upTo) {
      RangeScan scan = new RangeScan(page.files().size() + page.subdirectories().size());
      for (E file : page.files()) {
        String key = lister.key(file);
        if (scan.add(key, after, upTo)) {
          onFile.accept(file);
        }
      }
      for (String sub : page.subdirectories()) {
        // a cut can fall among a subdirectory's keys, and the next range then lists the subdirectory again
        if (scan.add(sub, after, upTo) && descend.test(sub)) {
          submit(sub, null, null);
        }
      }
      return scan;
    }
  }

  /** The keys on one page, their least and greatest, and whether any lay past the range's end. */
  private static final class RangeScan {
    final List<String> keys;
    String first;
    String last;
    boolean pastRange;

    RangeScan(int size) {
      keys = new ArrayList<>(size);
    }

    /** Records the key, and says whether it lies in (after, upTo]. */
    boolean add(String key, String after, String upTo) {
      keys.add(key);
      if (first == null || key.compareTo(first) < 0) {
        first = key;
      }
      if (last == null || key.compareTo(last) > 0) {
        last = key;
      }
      if (upTo != null && key.compareTo(upTo) > 0) {
        pastRange = true;
        return false;
      }
      return after == null || key.compareTo(after) > 0;
    }
  }
}
