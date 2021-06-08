package com.dellemc.objectscale.tool;

import com.dellemc.objectscale.util.EnhancedThreadPoolExecutor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.model.AccessControlPolicy;
import software.amazon.awssdk.services.s3.model.GetObjectAclResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ReplicationStatus;

import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ReReplicationTool extends AbstractVersionScanningTool {
    private static final Logger log = LogManager.getLogger(ReReplicationTool.class);

    public static final int QUEUE_SIZE = 500;

    private final Config config;

    public ReReplicationTool(Config config) {
        super(config, null);
        this.config = config;
    }

    @Override
    public void run() {

        // read from inventory file
        log.info("Reading object list from file {}", config.getInventoryFile());
        try (FileReader csvReader = new FileReader(config.getInventoryFile().toFile())) {
            CSVParser records = CSVFormat.DEFAULT
                    .withHeader(InventoryRow.Header.class)
                    .withIgnoreEmptyLines() // or else the last (empty) line will be parsed
                    .parse(csvReader);

            // build the InventoryRow stream
            Stream<InventoryRow> inventoryStream = StreamSupport.stream(records.spliterator(), false)
                    // filter out the header if present (determined by checking if the first column value is "Key")
                    .filter(record -> record.getRecordNumber() > 1 || !record.get(InventoryRow.Header.Key).equals(InventoryRow.Header.Key.name()))
                    .map(ReReplicationTool::inventoryRowFromCsvRecord);

            // configure thread pool for S3 updates
            final EnhancedThreadPoolExecutor executor = new EnhancedThreadPoolExecutor(
                    config.getThreadCount(),
                    new LinkedBlockingDeque<>(QUEUE_SIZE),
                    "s3-update-pool");

            inventoryStream.forEach(inventoryRow -> executor.blockingSubmit(() -> {
                // sanity check - if we've been given a full inventory, make sure we don't re-replicate versions that are
                // non-current or have already been successfully replicated
                if (inventoryRow.getIsLatest() != null && !inventoryRow.getIsLatest()) {
                    log.info("object [{}:{}] is not the latest version; skipping", inventoryRow.getKey(), inventoryRow.getVersionId());
                } else if (inventoryRow.getReplicationStatus() != null && inventoryRow.getReplicationStatus() != ReplicationStatus.FAILED) {
                    log.info("object [{}:{}] has not failed replication; skipping", inventoryRow.getKey(), inventoryRow.getVersionId());
                } else {
                    // update mtime of the object key by writing a new version
                    touchObject(inventoryRow);
                }
            }));

            // wait a long time for updates to complete
            executor.shutdown();
            log.info("Finished processing source file; waiting for re-replication jobs to complete");
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                executor.shutdownNow();
                throw new RuntimeException("last " + QUEUE_SIZE + " HEAD requests taking more than an hour; bailing out");
            }

            log.info("{} complete; exiting normally", ReReplicationTool.class.getSimpleName());

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } // try-with-resources will close the inventory file
    }

    static InventoryRow inventoryRowFromCsvRecord(CSVRecord record) {
        return new InventoryRow(
                // the first field should always be present
                record.get(InventoryRow.Header.Key),
                // user might provide a flat list of keys, in which case the rest of the fields will be null
                record.isSet(InventoryRow.Header.VersionId.name()) ? record.get(InventoryRow.Header.VersionId) : null,
                record.isSet(InventoryRow.Header.IsDeleteMarker.name()) ? Boolean.valueOf(record.get(InventoryRow.Header.IsDeleteMarker)) : null,
                record.isSet(InventoryRow.Header.IsLatest.name()) ? Boolean.valueOf(record.get(InventoryRow.Header.IsLatest)) : null,
                record.isSet(InventoryRow.Header.LastModified.name()) ? Instant.parse(record.get(InventoryRow.Header.LastModified)) : null,
                record.isSet(InventoryRow.Header.ETag.name()) ? record.get(InventoryRow.Header.ETag) : null,
                record.isSet(InventoryRow.Header.Size.name()) ? Long.valueOf(record.get(InventoryRow.Header.Size)) : null,
                record.isSet(InventoryRow.Header.OwnerId.name()) ? record.get(InventoryRow.Header.OwnerId) : null,
                record.isSet(InventoryRow.Header.ReplicationStatus.name()) ? ReplicationStatus.fromValue(record.get(InventoryRow.Header.ReplicationStatus)) : null);
    }

    void touchObject(InventoryRow inventoryRow) {
        final AccessControlPolicy acl;
        if (config.reReplicateCustomAcls) {
            // no other way to deal with custom ACLs then to GET and PUT them
            // and we don't know who the default owner would be, so can't infer a canned ACL either
            log.info("retrieving ACL for object version [{}:{}]", inventoryRow.getKey(), inventoryRow.getVersionId());
            acl = aclFromResponse(s3Client.getObjectAcl(builder -> builder
                    .bucket(config.getBucket())
                    .key(inventoryRow.getKey())
                    .versionId(inventoryRow.getVersionId())));
        } else {
            acl = null;
        }

        // TODO: do we need to support MPU copy on ObjectScale?  (ECS doesn't require it)
        log.info("re-replicating object version [{}:{}] by issuing a PUT+COPY call", inventoryRow.getKey(), inventoryRow.getVersionId());
        String versionIdStr = inventoryRow.getVersionId() != null ? "?versionId=" + inventoryRow.getVersionId() : "";
        String newVersionId = s3Client.copyObject(builder -> builder
                .copySource(String.format("%s/%s%s", config.getBucket(), inventoryRow.getKey(), versionIdStr))
                .destinationBucket(config.getBucket())
                .destinationKey(inventoryRow.getKey())
                .metadataDirective(MetadataDirective.COPY)
        ).versionId();

        if (config.reReplicateCustomAcls) {
            // set ACL on the new version
            log.info("replicating ACL for new object version [{}:{}]", inventoryRow.getKey(), newVersionId);
            s3Client.putObjectAcl(builder -> builder
                    .bucket(config.getBucket())
                    .key(inventoryRow.getKey())
                    .versionId(newVersionId)
                    .accessControlPolicy(acl));
        }
    }

    AccessControlPolicy aclFromResponse(GetObjectAclResponse response) {
        return AccessControlPolicy.builder()
                .owner(response.owner())
                .grants(response.grants()).build();
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    @EqualsAndHashCode(callSuper = true)
    @ToString
    public static class Config extends AbstractVersionScanningTool.Config {
        private final boolean reReplicateCustomAcls;
    }
}
