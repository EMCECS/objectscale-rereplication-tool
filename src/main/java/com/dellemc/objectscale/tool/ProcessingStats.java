package com.dellemc.objectscale.tool;

import java.util.concurrent.atomic.AtomicLong;

public class ProcessingStats {
    private final AtomicLong processedObjects = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final long startTimeMillis;
    private long endTimeMillis;

    public ProcessingStats(long startTimeMillis) {
        this.startTimeMillis = startTimeMillis;
    }

    public void incProcessedObjects(int increment) {
        processedObjects.addAndGet(increment);
    }

    public void incProcessedObjects() {
        incProcessedObjects(1);
    }

    public long getProcessedObjects() {
        return processedObjects.get();
    }

    public void incErrors(int increment) {
        errors.addAndGet(increment);
    }

    public void incErrors() {
        incErrors(1);
    }

    public long getErrors() {
        return errors.get();
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public long getEndTimeMillis() {
        return endTimeMillis;
    }

    public void setEndTimeMillis(long endTimeMillis) {
        this.endTimeMillis = endTimeMillis;
    }

    public long getPerSecondAverage() {
        long endTime = endTimeMillis > 0 ? endTimeMillis / 1000 : System.currentTimeMillis() / 1000;
        long secondDuration = endTime - startTimeMillis / 1000;
        return getProcessedObjects() / secondDuration;
    }
}
