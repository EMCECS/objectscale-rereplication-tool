package com.dellemc.objectscale.tool;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.core.retry.ClockSkew;
import software.amazon.awssdk.services.s3.model.ReplicationStatus;

import java.io.FileWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReReplicationProcessorTest extends AbstractTest {
    public static final int OBJECT_COUNT = 1200;

    String bucket = "rereplication-tool-test";
    Pattern objectNumPattern = Pattern.compile("object-([0-9]*)");

    @Override
    String getBucket() {
        return bucket;
    }

    @BeforeAll
    @Override
    public void setup() throws Exception {
        super.setup();

        createObjectVersions(OBJECT_COUNT);
    }

    /**
     * generate a sub-set of keys as an inventory to re-replicate
     * filterOutOdds => filter out delete markers
     */
    List<InventoryRow> generateInventoryObjects(int rangeFrom, int rangeTo, boolean filterOutOdds) {
        return s3Client.listObjectVersionsPaginator(builder -> builder.bucket(getBucket())).stream()
                .flatMap(response ->
                        Stream.concat( // merge versions and delete-markers
                                response.versions().stream().map(InventoryGenerator::inventoryRowFromObjectVersion),
                                response.deleteMarkers().stream().map(InventoryGenerator::inventoryRowFromDeleteMarker)
                        ).sorted()) // sort merged versions properly
                .filter(row -> {
                    Matcher matcher = objectNumPattern.matcher(row.getKey());
                    if (!matcher.matches()) return false;
                    int objectNum = Integer.parseInt(matcher.group(1));
                    if (filterOutOdds && objectNum % 2 != 0) return false;
                    return objectNum >= rangeFrom && objectNum <= rangeTo;
                })
                // make sure these rows get re-replicated
                .peek(row -> row.setReplicationStatus(ReplicationStatus.FAILED))
                .collect(Collectors.toList());
    }

    @Test
    public void testWithKeysOnly() throws Exception {
        final Instant testStartTime = delayAndGetStartTime();

        // generate temp file
        Path inventoryFile = Files.createTempFile("rereplication-inventory", "csv");
        inventoryFile.toFile().deleteOnExit();
        // map inventory rows to object-key strings
        List<String> keys = generateInventoryObjects(50, 100, true).stream().map(InventoryRow::getKey).collect(Collectors.toList());
        // write file
        Files.write(inventoryFile, keys, StandardCharsets.UTF_8);

        // tool should touch only the keys in the list
        ReReplicationProcessor tool = new ReReplicationProcessor(ReReplicationProcessor.Config.builder()
                .endpoint(URI.create(s3Endpoint))
                .awsProfile(awsProfile)
                .bucket(bucket)
                .inventoryFile(inventoryFile)
                .build());
        tool.run();

        // verify only the keys we want are touched
        Assertions.assertEquals(26, keys.size());
        verifyOnlyTheseKeysWereTouched(keys, testStartTime);
    }

    @Test
    public void testWithVersionIds() throws Exception {
        final Instant testStartTime = delayAndGetStartTime();

        // generate temp file
        Path inventoryFile = Files.createTempFile("rereplication-inventory", "csv");
        inventoryFile.toFile().deleteOnExit();
        // map inventory rows to object-key strings
        List<InventoryRow> rows = generateInventoryObjects(200, 250, true);
        List<String> rowStrings = rows.stream().map(this::rowToKeyAndVersion).collect(Collectors.toList());
        // write file
        Files.write(inventoryFile, rowStrings, StandardCharsets.UTF_8);

        // tool should touch only the keys in the list
        ReReplicationProcessor tool = new ReReplicationProcessor(ReReplicationProcessor.Config.builder()
                .endpoint(URI.create(s3Endpoint))
                .awsProfile(awsProfile)
                .bucket(bucket)
                .inventoryFile(inventoryFile)
                .build());
        tool.run();

        // verify only the keys we want are touched
        List<String> keys = rows.stream().map(InventoryRow::getKey).distinct().collect(Collectors.toList());
        Assertions.assertEquals(26, keys.size());
        verifyOnlyTheseKeysWereTouched(keys, testStartTime);
    }

    String rowToKeyAndVersion(InventoryRow row) {
        return String.join(",", Arrays.asList(row.getKey(), row.getVersionId()));
    }

    @Test
    void testWithFullInventoryRows() throws Exception {
        final Instant testStartTime = delayAndGetStartTime();

        // generate temp file
        Path inventoryFile = Files.createTempFile("rereplication-inventory", "csv");
        inventoryFile.toFile().deleteOnExit();
        // map inventory rows to object-key strings
        List<InventoryRow> rows = generateInventoryObjects(300, 350, false);
        // write file
        try (CSVPrinter csvPrinter = CSVFormat.DEFAULT.withHeader(InventoryRow.Header.class).print(new FileWriter(inventoryFile.toFile()))) {
            for (InventoryRow row : rows) {
                csvPrinter.printRecord(row.toFieldArray());
            }
        }

        // tool should touch only the keys in the list
        ReReplicationProcessor tool = new ReReplicationProcessor(ReReplicationProcessor.Config.builder()
                .endpoint(URI.create(s3Endpoint))
                .awsProfile(awsProfile)
                .bucket(bucket)
                .inventoryFile(inventoryFile)
                .build());
        tool.run();

        // verify only the keys we want are touched
        List<String> keys = rows.stream().map(InventoryRow::getKey).distinct().collect(Collectors.toList());
        Assertions.assertEquals(51, keys.size());
        verifyOnlyTheseKeysWereTouched(keys, testStartTime);
    }

    Instant delayAndGetStartTime() throws Exception {
        // wait 5 seconds to age the objects created in the setup
        Thread.sleep(5000);

        // adjust for clock skew
        Optional<Instant> serverTime = ClockSkew.getServerTime(s3Client.headBucket(builder -> builder.bucket(getBucket())).sdkHttpResponse());
        Instant clientTime = Instant.now();
        if (serverTime.isPresent()) {
            return clientTime.minus(ClockSkew.getClockSkew(clientTime, serverTime.get()));
        } else {
            return clientTime;
        }
    }

    // also checks that no object key has more than 2 versions or 1 version and 2 delete markers
    void verifyOnlyTheseKeysWereTouched(List<String> keys, Instant testStartTime) {
        Map<String, AtomicInteger> dmCounts = Collections.synchronizedMap(new HashMap<>());
        Map<String, AtomicInteger> versionCounts = Collections.synchronizedMap(new HashMap<>());
        s3Client.listObjectVersionsPaginator(builder -> builder.bucket(getBucket())).forEach(response -> {
            response.versions().forEach(version -> {
                incrementCount(versionCounts, version.key());
                if (keys.contains(version.key()) && version.isLatest()) {
                    Assertions.assertTrue(version.lastModified().isAfter(testStartTime),
                            version.key() + ":" + version.versionId() + " mtime [" + version.lastModified() + "] is before testStartTime [" + testStartTime + "]");
                } else {
                    Assertions.assertFalse(version.lastModified().isAfter(testStartTime),
                            version.key() + ":" + version.versionId() + " mtime [" + version.lastModified() + "] is after testStartTime [" + testStartTime + "]");
                }
            });
            response.deleteMarkers().forEach(dm -> {
                incrementCount(dmCounts, dm.key());
                Assertions.assertFalse(dm.lastModified().isAfter(testStartTime),
                        dm.key() + ":" + dm.versionId() + " mtime [" + dm.lastModified() + "] is after testStartTime [" + testStartTime + "]");
            });
        });
        keys.forEach(key -> {
            Assertions.assertTrue(versionCounts.containsKey(key));
            if (key.matches("^.*[13579]$")) // odd numbers should have delete-markers
                Assertions.assertTrue(dmCounts.containsKey(key));
            if (dmCounts.containsKey(key)) {
                Assertions.assertTrue(versionCounts.get(key).get() <= 1);
                Assertions.assertTrue(dmCounts.get(key).get() <= 2);
            } else {
                Assertions.assertTrue(versionCounts.get(key).get() <= 2);
            }
        });
    }

    void incrementCount(Map<String, AtomicInteger> counts, String key) {
        AtomicInteger count = counts.get(key);
        if (count == null) {
            synchronized (counts) {
                count = counts.get(key);
                if (count == null) {
                    count = new AtomicInteger();
                    counts.put(key, count);
                }
            }
        }
        count.incrementAndGet();
    }

    @Builder
    @EqualsAndHashCode
    static class KeyAndVersion {
        String key;
        String version;
    }

    @AfterAll
    @Override
    public void teardown() throws Exception {
        cleanBucket(getBucket());

        super.teardown();
    }
}
