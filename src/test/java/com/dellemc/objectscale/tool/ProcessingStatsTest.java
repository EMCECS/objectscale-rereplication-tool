package com.dellemc.objectscale.tool;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProcessingStatsTest {
    @Test
    public void testZeroDuration() {
        long now = System.currentTimeMillis();
        ProcessingStats stats = new ProcessingStats(now);
        stats.incProcessedObjects(10);
        stats.setEndTimeMillis(now);
        // when duration is zero, per-second average should return zero
        Assertions.assertEquals(0, stats.getPerSecondAverage());

        stats.setEndTimeMillis(now + 1_000); // +1 second
        Assertions.assertEquals(10, stats.getPerSecondAverage());
    }
}
