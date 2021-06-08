package com.dellemc.objectscale.tool;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.*;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Setting up CRR and injecting replication failures is hard, so this class simply inventories a versioned bucket
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class InventoryToolTest extends AbstractTest {
    public static final int OBJECT_COUNT = 2100;
    String bucket = "inventory-tool-basic-test";

    @Override
    String getBucket() {
        return bucket;
    }

    @Override
    @BeforeAll
    public void setup() throws Exception {
        super.setup();

        createObjectVersions(OBJECT_COUNT);
    }

    @Test
    public void testStripQuotes() {
        List<String> etags = Arrays.asList("abcdef0123456789", "\"abcdef0123456789\"", " \"abcdef0123456789\" ");
        String strippedEtag = "abcdef0123456789";
        etags.forEach(etag -> Assertions.assertEquals(strippedEtag, InventoryTool.stripQuotes(etag),
                "etag [" + etag + "] was not stripped properly"));
    }

    @Test
    public void testBasicInventory() throws IOException {
        Path inventoryFile = Files.createTempFile("rereplication-inventory", "csv");
        inventoryFile.toFile().deleteOnExit();
        InventoryTool tool = new InventoryTool(InventoryTool.Config.builder()
                .endpoint(URI.create(s3Endpoint))
                .awsProfile(awsProfile)
                .bucket(bucket)
                .inventoryFile(inventoryFile)
                .filterType(InventoryTool.FilterType.AllVersions)
                .build());

        tool.run();

        List<CSVRecord> records = CSVFormat.DEFAULT.withHeader(InventoryRow.Header.class)
                .withSkipHeaderRecord() // or else the header row will be parsed as data
                .withIgnoreEmptyLines() // or else the last (empty) line will be parsed
                .parse(new FileReader(inventoryFile.toFile())).getRecords();

        Assertions.assertEquals(OBJECT_COUNT + OBJECT_COUNT / 2, records.size());

        // map CSV records to InventoryRows
        List<InventoryRow> rows = records.stream().map(ReReplicationTool::inventoryRowFromCsvRecord).collect(Collectors.toList());

        rows.forEach(row -> {
            Assertions.assertTrue(row.getKey().matches("object-[0-9]*"));
            Assertions.assertNotNull(row.getVersionId());
            Assertions.assertNotNull(row.getIsDeleteMarker());
            Assertions.assertNotNull(row.getIsLatest());
            Assertions.assertNotNull(row.getLastModified());
            Assertions.assertEquals("d41d8cd98f00b204e9800998ecf8427e", row.getETag());
            Assertions.assertEquals(0, row.getSize());
            Assertions.assertNotNull(row.getOwnerId());
            Assertions.assertNotNull(row.getReplicationStatus());
        });
    }

    @Override
    @AfterAll
    public void teardown() throws Exception {
        cleanBucket(getBucket());

        super.teardown();
    }
}
