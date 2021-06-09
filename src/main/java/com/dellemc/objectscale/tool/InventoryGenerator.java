package com.dellemc.objectscale.tool;

import com.dellemc.objectscale.util.EnhancedThreadPoolExecutor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.ReplicationStatus;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsIterable;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class InventoryGenerator extends AbstractReplicationTool {
    private static final Logger log = LogManager.getLogger(InventoryGenerator.class);

    public static final String HEADER_AMZ_REPLICATION_STATUS = "x-amz-replication-status";
    public static final int QUEUE_SIZE = 5000;

    private final Config config;

    public InventoryGenerator(Config config) {
        super(config, null);
        this.config = config;
    }

    @Override
    String getGrossRecordsLabel() {
        return "Listed versions";
    }

    @Override
    String getFilteredRecordsLabel() {
        return "Output records";
    }

    @Override
    public void run() {
        final AtomicBoolean stillListing = new AtomicBoolean(true);
        Thread writerThread = null;
        try {
            // use a blocking queue to maintain order and limit memory
            BlockingQueue<Future<InventoryRow>> futureQueue = new LinkedBlockingQueue<>(QUEUE_SIZE);

            // configure output
            final CSVPrinter csvPrinter = CSVFormat.DEFAULT.withHeader(InventoryRow.Header.class).print(new FileWriter(config.getInventoryFile().toFile()));

            // start thread to write output
            // futureQueue.take() will block until the main thread submits more tasks to the HEAD thread pool
            // .get() on the returned future will block until that version's HEAD call returns and repl. status is populated
            writerThread = new Thread(() -> {
                try {
                    while (stillListing.get()) {
                        try {
                            while (true) {
                                InventoryRow inventoryRow = futureQueue.take().get();
                                // if configured, only print failed versions
                                if (config.filterType == FilterType.FailedCurrentVersionOnly
                                        && inventoryRow.getReplicationStatus() != ReplicationStatus.FAILED)
                                    continue;
                                csvPrinter.printRecord(inventoryRow.toFieldArray());
                                if (filteredRecords != null) filteredRecords.incProcessedObjects();
                            }
                        } catch (ExecutionException e) {
                            if (e.getCause() instanceof ListingCompleteException) {
                                // this should only happen when the last future is pulled from the queue and all others have been written
                                log.info("Listing terminator received in CSV writer thread");
                            } else {
                                logException(Level.WARN, "Unexpected ERROR", e);
                                if (filteredRecords != null) filteredRecords.incErrors();
                            }
                        } catch (InterruptedException e) {
                            logException(Level.INFO, "CSV writer thread interrupted", e);
                        }
                    }
                    log.info("CSV writer thread shutting down");
                    csvPrinter.flush();
                } catch (IOException e) {
                    logException(Level.ERROR, "Error writing to inventory file", e);
                }
            });
            writerThread.start();

            // TODO: use alternative query to filter and list all failed versions (and directly submit them to the queue)

            // configure thread pool for S3 HEADs
            final EnhancedThreadPoolExecutor executor = new EnhancedThreadPoolExecutor(
                    config.getThreadCount(),
                    new LinkedBlockingDeque<>(QUEUE_SIZE),
                    "s3-head-pool");

            // list versions
            log.info("Listing versions in [{}] using prefix [{}]", config.getBucket(), config.getPrefix());
            ListObjectVersionsIterable versionPages = s3Client.listObjectVersionsPaginator(builder -> builder
                    .bucket(config.getBucket())
                    .prefix(config.getPrefix()));

            // use a stream to convert to InventoryRow and filter
            versionPages.stream()
                    .flatMap(response -> {
                        if (grossRecords != null)
                            grossRecords.incProcessedObjects(response.versions().size() + response.deleteMarkers().size());
                        return Stream.concat( // merge versions and delete-markers
                                response.versions().stream().map(InventoryGenerator::inventoryRowFromObjectVersion),
                                response.deleteMarkers().stream().map(InventoryGenerator::inventoryRowFromDeleteMarker)
                        ).sorted(); // sort combined versions+deleteMarkers (this is how they are returned, but s3client separates)
                    })
                    // if not listing all versions, filter current version only
                    .filter(inventoryRow -> config.filterType == FilterType.AllVersions || inventoryRow.getIsLatest())
                    .forEachOrdered(inventoryRow -> {
                        // this stream can't be parallelized, so submit to a thread pool for HEADing each version to get repl. status
                        try {
                            futureQueue.put(executor.blockingSubmit(() -> {
                                // HEAD each version to get replication status
                                try {
                                    inventoryRow.setReplicationStatus(
                                            s3Client.headObject(builder -> builder.bucket(config.getBucket())
                                                    .key(inventoryRow.getKey())
                                                    .versionId(inventoryRow.getVersionId()))
                                                    .replicationStatus());
                                } catch (S3Exception e) {
                                    if (e.statusCode() == 405) {
                                        // we can still pull the replication status from a 405 (method not allowed)
                                        inventoryRow.setReplicationStatus(ReplicationStatus.fromValue(
                                                e.awsErrorDetails().sdkHttpResponse().firstMatchingHeader(HEADER_AMZ_REPLICATION_STATUS)
                                                        .orElse("Unknown")));
                                    } else {
                                        logException(Level.INFO, "HEAD failed for " + inventoryRow.getKey() + ":" + inventoryRow.getVersionId(), e);
                                        inventoryRow.setReplicationStatus(ReplicationStatus.fromValue("Unknown"));
                                    }
                                }
                                return inventoryRow;
                            }));
                        } catch (InterruptedException e) { // would come from futureQueue.put()
                            throw new RuntimeException(e);
                        }
                    });

            // break the loop in the writer thread
            stillListing.set(false);
            log.info("Listing complete; all HEAD operations sent to queue");

            // send terminating Future
            futureQueue.put(executor.blockingSubmit(() -> {
                throw new ListingCompleteException();
            }));
            log.info("Listing terminator sent to CSV writer thread");

            // wait a long time for heads to complete
            executor.shutdown();
            log.info("Waiting for HEAD operations to complete");
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                executor.shutdownNow();
                throw new RuntimeException("last " + QUEUE_SIZE + " HEAD requests taking more than an hour; bailing out");
            }
            log.info("All HEAD operations should be complete; waiting for CSV writer thread to finish");

            // don't close the CSV file before the writer is done
            writerThread.join();

            log.info("{} complete; exiting normally", InventoryGenerator.class.getSimpleName());

        } catch (IOException | InterruptedException e) {
            stillListing.set(false);
            // try to stop the CSV writer thread
            if (writerThread != null) writerThread.interrupt();
            throw new RuntimeException(e);
        } // try-with-resources will close the CSV file
    }

    static InventoryRow inventoryRowFromObjectVersion(ObjectVersion version) {
        return new InventoryRow(version.key(), version.versionId(), false, version.isLatest(),
                version.lastModified(), stripQuotes(version.eTag()), version.size(), version.owner().id(), null);
    }

    static InventoryRow inventoryRowFromDeleteMarker(DeleteMarkerEntry marker) {
        return new InventoryRow(marker.key(), marker.versionId(), true, marker.isLatest(),
                marker.lastModified(), null, 0L, marker.owner().id(), null);
    }

    static String stripQuotes(String value) {
        if (value == null) return null;
        value = value.trim();
        if (value.charAt(0) == '"') value = value.substring(1);
        if (value.charAt(value.length() - 1) == '"') value = value.substring(0, value.length() - 1);
        return value;
    }

    void logException(Level level, String message, Exception exception) {
        if (log.isDebugEnabled()) {
            log.log(level, message, exception);
        } else {
            log.log(level, message + " - " + exception);
        }
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    @EqualsAndHashCode(callSuper = true)
    @ToString(callSuper = true)
    public static class Config extends AbstractReplicationTool.Config {
        private final String prefix;
        @Builder.Default
        private final FilterType filterType = FilterType.FailedCurrentVersionOnly;
        private final boolean forceOverwrite;

        @Override
        public void validate() {
            super.validate();

            if (Files.exists(getInventoryFile()) && !forceOverwrite) {
                throw new IllegalArgumentException("inventoryFile already exists (use forceOverwrite to overwrite)");
            }
        }
    }

    enum FilterType {
        AllVersions, CurrentVersionOnly, FailedCurrentVersionOnly
    }

    static class ListingCompleteException extends RuntimeException {
    }
}
