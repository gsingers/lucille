package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.FailureClass;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.junit.Test;

public class TraversalBudgetTest {

  @Test
  public void testDirectoryLimit() {
    TraversalBudget budget = new TraversalBudget(3, null);

    assertTrue(budget.mayList());
    assertTrue(budget.mayList());
    assertTrue(budget.mayList());
    assertFalse(budget.mayList());
    assertFalse(budget.mayList());
    // a directory that was refused is not counted as listed
    assertEquals(3, budget.getDirectoriesListed());
  }

  @Test
  public void testFirstDirectoryIsAlwaysListed() throws Exception {
    // a time limit that has passed before the traversal lists anything
    TraversalBudget budget = new TraversalBudget(null, 0L);
    Thread.sleep(5);

    // otherwise a unit could hand back the very directory it was given, forever
    assertTrue(budget.mayList());
    assertFalse(budget.mayList());
    assertEquals(1, budget.getDirectoriesListed());
  }

  @Test
  public void testTimeLimit() throws Exception {
    TraversalBudget budget = new TraversalBudget(null, 200L);

    assertTrue(budget.mayList());
    assertTrue(budget.mayList());
    Thread.sleep(250);
    assertFalse(budget.mayList());
  }

  @Test
  public void testEitherLimitEndsTheDescent() {
    TraversalBudget budget = new TraversalBudget(1, 60_000L);
    assertTrue(budget.mayList());
    assertFalse(budget.mayList());
  }

  @Test
  public void testCancelledBudgetAllowsNothing() {
    boolean[] cancelled = {false};
    TraversalBudget budget = new TraversalBudget(null, null, () -> cancelled[0]);
    assertTrue(budget.mayList());

    cancelled[0] = true;
    assertFalse(budget.mayList());

    // not even the first directory, which a limit never refuses: nobody is waiting for this traversal's progress
    TraversalBudget fromTheStart = new TraversalBudget(null, null, () -> true);
    assertFalse(fromTheStart.mayList());
    assertEquals(0, fromTheStart.getDirectoriesListed());
  }

  @Test
  public void testSourceErrorEndsTheTraversal() {
    TraversalBudget budget = TraversalBudget.unlimited();
    assertTrue(budget.mayList());
    budget.callRefused();
    budget.callRefused();
    assertEquals(2, budget.getRefusedCalls());
    assertNull(budget.getSourceError());

    budget.sourceError(FailureClass.THROTTLED, new RuntimeException("wrapper", new IOException("429 Too Many Requests")));

    // nothing more may be listed, so the rest of the traversal is handed back
    assertFalse(budget.mayList());
    assertEquals(FailureClass.THROTTLED, budget.getSourceError().failureClass());
    assertEquals("IOException: 429 Too Many Requests", budget.getSourceError().cause());
    assertEquals(1, budget.getDirectoriesListed());
  }

  @Test
  public void testUnlimitedOnlyCounts() {
    TraversalBudget budget = TraversalBudget.unlimited();
    for (int i = 0; i < 1000; i++) {
      assertTrue(budget.mayList());
    }
    assertEquals(1000, budget.getDirectoriesListed());
    assertEquals(List.of(), budget.getHandedBack());
  }

  @Test
  public void testHandedBackDirectoriesAreKeptInOrder() {
    TraversalBudget budget = new TraversalBudget(1, null);
    budget.handBack(URI.create("s3://bucket/b/"));
    budget.handBack(URI.create("s3://bucket/a/"));

    assertEquals(List.of(URI.create("s3://bucket/b/"), URI.create("s3://bucket/a/")), budget.getHandedBack());
  }
}
