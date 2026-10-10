package com.kmwllc.lucille.connector.storageclient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * An in-memory bucket that answers ListObjectsV2 with S3's semantics for prefix, a "/" delimiter, max-keys, StartAfter
 * and continuation tokens. Under StartAfter, a common prefix is listed if any key under it sorts after StartAfter, even
 * when the prefix itself does not; a continuation token resumes after everything already listed.
 */
class FakeBucket {

  private final TreeSet<String> keys = new TreeSet<>();

  FakeBucket(Collection<String> keys) {
    this.keys.addAll(keys);
  }

  TreeSet<String> keys() {
    return keys;
  }

  ListObjectsV2Response list(ListObjectsV2Request request) {
    String prefix = request.prefix() == null ? "" : request.prefix();
    int maxKeys = request.maxKeys() == null ? 1000 : request.maxKeys();
    String from = request.continuationToken() != null ? request.continuationToken() : request.startAfter();

    List<S3Object> contents = new ArrayList<>();
    List<CommonPrefix> prefixes = new ArrayList<>();
    String last = null;
    String key = from == null ? keys.ceiling(prefix) : keys.higher(from);
    // resuming after a common prefix skips every key under it
    if (request.continuationToken() != null && from.endsWith("/") && key != null && key.startsWith(from)) {
      key = keys.higher(from + Character.MAX_VALUE);
    }
    while (key != null && key.startsWith(prefix)) {
      if (contents.size() + prefixes.size() == maxKeys) {
        return response(contents, prefixes, last);
      }
      int slash = key.indexOf('/', prefix.length());
      if (slash >= 0) {
        String common = key.substring(0, slash + 1);
        prefixes.add(CommonPrefix.builder().prefix(common).build());
        last = common;
        key = keys.higher(common + Character.MAX_VALUE);
      } else {
        contents.add(S3Object.builder().key(key).size(1L).build());
        last = key;
        key = keys.higher(key);
      }
    }
    return response(contents, prefixes, null);
  }

  private static ListObjectsV2Response response(List<S3Object> contents, List<CommonPrefix> prefixes, String next) {
    return ListObjectsV2Response.builder().contents(contents).commonPrefixes(prefixes).isTruncated(next != null)
        .nextContinuationToken(next).build();
  }
}
