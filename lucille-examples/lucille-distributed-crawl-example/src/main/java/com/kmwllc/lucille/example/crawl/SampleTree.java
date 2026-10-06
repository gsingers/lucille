package com.kmwllc.lucille.example.crawl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Writes a small, lopsided directory tree to crawl: most of it under one directory, as real trees often are, so
 * that bounded units have something to share out.
 *
 * <pre>
 *   root/readme.txt
 *   root/big/d0..d9/e0..e9/{a,b}.txt      111 directories, 200 files
 *   root/small0..small4/{a,b}.txt         5 directories, 10 files
 * </pre>
 *
 * <pre>
 *   java -cp 'target/lib/*:target/classes' com.kmwllc.lucille.example.crawl.SampleTree target/sample-tree
 * </pre>
 */
public class SampleTree {

  public static final int NUM_FILES = 211;

  public static void main(String[] args) throws IOException {
    Path root = Paths.get(args.length > 0 ? args[0] : "target/sample-tree");
    write(root);
    System.out.println("Wrote " + NUM_FILES + " files under " + root.toAbsolutePath());
  }

  public static void write(Path root) throws IOException {
    file(root.resolve("readme.txt"));
    for (int d = 0; d < 10; d++) {
      for (int e = 0; e < 10; e++) {
        Path dir = root.resolve("big").resolve("d" + d).resolve("e" + e);
        file(dir.resolve("a.txt"));
        file(dir.resolve("b.txt"));
      }
    }
    for (int s = 0; s < 5; s++) {
      Path dir = root.resolve("small" + s);
      file(dir.resolve("a.txt"));
      file(dir.resolve("b.txt"));
    }
  }

  private static void file(Path file) throws IOException {
    Files.createDirectories(file.getParent());
    Files.writeString(file, file.getFileName().toString());
  }
}
