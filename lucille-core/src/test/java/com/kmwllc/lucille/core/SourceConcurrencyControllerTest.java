package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class SourceConcurrencyControllerTest {

  @Test
  public void testClimbsWhileCleanAndHalvesOnRefusals() {
    long[] now = {0};
    SourceConcurrencyController controller = new SourceConcurrencyController(2400, 600, 240, 24, 10_000, () -> now[0]);
    assertEquals(600, controller.current());

    // a quarter to start, a tenth more each clean interval, never past the maximum
    assertEquals(840, controller.adjust(0));
    assertEquals(1080, controller.adjust(0));
    for (int i = 0; i < 10; i++) {
      controller.adjust(0);
    }
    assertEquals(2400, controller.adjust(0));

    // the first refusal halves it
    now[0] = 100_000;
    assertEquals(1200, controller.adjust(37));
    // more refusals within the hold time are the same burst, and do not halve it again
    now[0] = 105_000;
    assertEquals(1200, controller.adjust(5));
    // after the hold, they do
    now[0] = 110_000;
    assertEquals(600, controller.adjust(1));
    // and never below the floor, one call per unit
    now[0] = 200_000;
    assertEquals(300, controller.adjust(1));
    now[0] = 300_000;
    assertEquals(150, controller.adjust(1));
    now[0] = 400_000;
    assertEquals(75, controller.adjust(1));
    now[0] = 500_000;
    assertEquals(37, controller.adjust(1));
    now[0] = 600_000;
    assertEquals(24, controller.adjust(1));
    now[0] = 700_000;
    assertEquals(24, controller.adjust(1));
  }

  @Test
  public void testPerUnit() {
    SourceConcurrencyController controller = new SourceConcurrencyController(2400, 2400, 240, 24, 10_000);
    assertEquals(100, controller.perUnit(24));
    assertEquals(2400, controller.perUnit(1));
    // never less than one call per unit, whatever the division says
    assertEquals(1, controller.perUnit(5000));
    assertEquals(2400, controller.perUnit(0));
  }

  @Test
  public void testInitialIsHeldWithinTheBounds() {
    assertEquals(24, new SourceConcurrencyController(2400, 1, 240, 24, 10_000).current());
    assertEquals(2400, new SourceConcurrencyController(2400, 9000, 240, 24, 10_000).current());
    assertThrows(IllegalArgumentException.class, () -> new SourceConcurrencyController(10, 5, 1, 20, 10_000));
    assertThrows(IllegalArgumentException.class, () -> new SourceConcurrencyController(10, 5, 0, 1, 10_000));
  }
}
