package com.kmwllc.lucille.connector.storageclient;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Picks boundaries that cut the unlisted part of a large directory into key ranges that can be listed at once.
 *
 * <p>Names (keys after the directory's prefix) are read as mixed-radix numbers. Each position's digits are the
 * character classes seen there on the page just listed (digits, lower case, upper case, other printable ASCII), plus an
 * end-of-name digit below them all, so that {@code IMG_0999} is followed by {@code IMG_1000} rather than by
 * {@code IMG_099:}. The page spans some distance in that space; the ranges after it are 1, 2, 4, ... pages of that
 * distance wide, on the guess that the directory continues at the same density. A range that turns out larger is cut
 * again when its own first page is truncated.
 *
 * <p>Correctness depends only on the boundaries' order: each is checked to lie strictly between its neighbours by
 * {@link String#compareTo}, which orders ASCII as S3 does. A name outside the classes just gets fewer or worse cuts.
 */
final class KeyRanges {

  private static final String DIGITS = "0123456789";
  private static final String LOWER = "abcdefghijklmnopqrstuvwxyz";
  private static final String UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final String OTHER;

  static {
    StringBuilder other = new StringBuilder();
    for (char c = 32; c < 127; c++) {
      if (!Character.isLetterOrDigit(c)) {
        other.append(c);
      }
    }
    OTHER = other.toString();
  }

  private KeyRanges() { }

  /**
   * Boundaries b1 < b2 < ... in (last, upTo), so that the ranges (last, b1], (b1, b2], ..., (bn, upTo] cover what is
   * left of the directory. Empty if no useful cut can be made.
   *
   * @param dir the directory's prefix, which every key starts with
   * @param keys the keys on the page just listed, files and subdirectories
   * @param first the least of keys
   * @param last the greatest of keys
   * @param upTo the inclusive end of the range being listed, or null if it runs to the end of the directory
   * @param pieces how many ranges to make
   */
  static List<String> cuts(String dir, Collection<String> keys, String first, String last, String upTo, int pieces) {
    List<String> cuts = new ArrayList<>();
    if (!first.startsWith(dir) || !last.startsWith(dir)) {
      return cuts;
    }
    int width = 1;
    for (String key : keys) {
      width = Math.max(width, key.length() - dir.length() + 1);
    }
    String[] alphabets = alphabets(dir.length(), keys, width);
    BigInteger lo = value(first, dir.length(), alphabets);
    BigInteger hi = value(last, dir.length(), alphabets);
    BigInteger span = hi.subtract(lo);
    if (span.signum() <= 0) {
      return cuts;
    }
    BigInteger limit = capacity(alphabets);
    BigInteger offset = BigInteger.ZERO;
    String previous = last;
    for (int i = 0; i < pieces - 1; i++) {
      offset = offset.add(span.shiftLeft(i));
      BigInteger point = hi.add(offset);
      if (point.compareTo(limit) >= 0) {
        break;
      }
      String cut = dir + name(point, alphabets);
      if (cut.compareTo(previous) <= 0 || (upTo != null && cut.compareTo(upTo) >= 0)) {
        break;
      }
      cuts.add(cut);
      previous = cut;
    }
    return cuts;
  }

  /** For each position, the end-of-name digit (as '\0') followed by every class of character seen there. */
  private static String[] alphabets(int from, Collection<String> keys, int width) {
    boolean[][] seen = new boolean[width][4];
    for (String key : keys) {
      for (int i = 0; i < width && from + i < key.length(); i++) {
        char c = key.charAt(from + i);
        seen[i][c >= '0' && c <= '9' ? 0 : c >= 'a' && c <= 'z' ? 1 : c >= 'A' && c <= 'Z' ? 2 : 3] = true;
      }
    }
    String[] alphabets = new String[width];
    for (int i = 0; i < width; i++) {
      StringBuilder b = new StringBuilder("\0");
      b.append(seen[i][3] ? OTHER : "").append(seen[i][0] ? DIGITS : "").append(seen[i][2] ? UPPER : "")
          .append(seen[i][1] ? LOWER : "");
      char[] chars = b.toString().toCharArray();
      Arrays.sort(chars);
      alphabets[i] = new String(chars);
    }
    return alphabets;
  }

  private static BigInteger capacity(String[] alphabets) {
    BigInteger c = BigInteger.ONE;
    for (String a : alphabets) {
      c = c.multiply(BigInteger.valueOf(a.length()));
    }
    return c;
  }

  /** The name's value: each character's index in its position's alphabet, or of the greatest one below it. */
  private static BigInteger value(String key, int from, String[] alphabets) {
    BigInteger v = BigInteger.ZERO;
    boolean ended = false;
    for (int i = 0; i < alphabets.length; i++) {
      String a = alphabets[i];
      int digit = 0;
      if (!ended && from + i < key.length()) {
        int found = Arrays.binarySearch(a.toCharArray(), key.charAt(from + i));
        digit = found >= 0 ? found : Math.max(0, -found - 2);
      } else {
        ended = true;
      }
      v = v.multiply(BigInteger.valueOf(a.length())).add(BigInteger.valueOf(digit));
    }
    return v;
  }

  /** The name with the given value, cut at its first end-of-name digit. */
  private static String name(BigInteger value, String[] alphabets) {
    char[] chars = new char[alphabets.length];
    BigInteger v = value;
    for (int i = alphabets.length - 1; i >= 0; i--) {
      BigInteger[] qr = v.divideAndRemainder(BigInteger.valueOf(alphabets[i].length()));
      chars[i] = alphabets[i].charAt(qr[1].intValue());
      v = qr[0];
    }
    int end = 0;
    while (end < chars.length && chars[end] != '\0') {
      end++;
    }
    return new String(chars, 0, end);
  }
}
