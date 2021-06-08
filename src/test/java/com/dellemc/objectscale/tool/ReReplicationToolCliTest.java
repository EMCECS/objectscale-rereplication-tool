package com.dellemc.objectscale.tool;

import org.apache.commons.cli.DefaultParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReReplicationToolCliTest {
    @Test
    public void testInventoryToolCli() throws Exception {
        String endpoint = "endpoint-1", bucket = "bucket-1", accessKey = "accessKey-1";
        String secretKey = "secretKey-1", profile = "profile-1", file = "file-1";
        int threads = 9;
        String prefix = "prefix-1";

        String[] args = {
                "-e", endpoint,
                "-b", bucket,
                "-a", accessKey,
                "-s", secretKey,
                "-p", profile,
                "-f", file,
                "-t", "" + threads,
                "-i",
                "--prefix", prefix,
                "--force-overwrite",
        };

        InventoryTool.Config config = (InventoryTool.Config) ReReplicationToolCli.parseConfig(
                new DefaultParser().parse(ReReplicationToolCli.options(), args));

        Assertions.assertEquals(endpoint, config.getEndpoint().toString());
        Assertions.assertEquals(bucket, config.getBucket());
        Assertions.assertEquals(accessKey, config.getAccessKey());
        Assertions.assertEquals(secretKey, config.getSecretKey());
        Assertions.assertEquals(profile, config.getAwsProfile());
        Assertions.assertEquals(file, config.getInventoryFile().toString());
        Assertions.assertEquals(threads, config.getThreadCount());
        Assertions.assertEquals(InventoryTool.FilterType.FailedCurrentVersionOnly, config.getFilterType());
        Assertions.assertEquals(prefix, config.getPrefix());
        Assertions.assertTrue(config.isForceOverwrite());
    }

    @Test
    public void testInventoryToolCurrentVersionAndDefaults() throws Exception {
        String endpoint = "endpoint-1", bucket = "bucket-1", accessKey = "accessKey-1";
        String secretKey = "secretKey-1", file = "file-1";

        String[] args = {
                "-e", endpoint,
                "-b", bucket,
                "-a", accessKey,
                "-s", secretKey,
                "-f", file,
                "-i",
                "--current-version"
        };

        InventoryTool.Config config = (InventoryTool.Config) ReReplicationToolCli.parseConfig(
                new DefaultParser().parse(ReReplicationToolCli.options(), args));

        Assertions.assertEquals(endpoint, config.getEndpoint().toString());
        Assertions.assertEquals(bucket, config.getBucket());
        Assertions.assertEquals(accessKey, config.getAccessKey());
        Assertions.assertEquals(secretKey, config.getSecretKey());
        Assertions.assertNull(config.getAwsProfile());
        Assertions.assertEquals(file, config.getInventoryFile().toString());
        Assertions.assertEquals(AbstractVersionScanningTool.Config.DEFAULT_THREAD_COUNT, config.getThreadCount());
        Assertions.assertEquals(InventoryTool.FilterType.CurrentVersionOnly, config.getFilterType());
        Assertions.assertNull(config.getPrefix());
        Assertions.assertFalse(config.isForceOverwrite());
    }

    @Test
    public void testInventoryToolAllVersions() throws Exception {
        String endpoint = "endpoint-1", bucket = "bucket-1", accessKey = "accessKey-1";
        String secretKey = "secretKey-1", file = "file-1";

        String[] args = {
                "-e", endpoint,
                "-b", bucket,
                "-a", accessKey,
                "-s", secretKey,
                "-f", file,
                "-i",
                "--all-versions"
        };

        InventoryTool.Config config = (InventoryTool.Config) ReReplicationToolCli.parseConfig(
                new DefaultParser().parse(ReReplicationToolCli.options(), args));

        Assertions.assertEquals(InventoryTool.FilterType.AllVersions, config.getFilterType());
    }

    @Test
    public void testReReplicationToolCli() throws Exception {
        String endpoint = "endpoint-1", bucket = "bucket-1", accessKey = "accessKey-1";
        String secretKey = "secretKey-1", profile = "profile-1", file = "file-1";
        int threads = 9;

        String[] args = {
                "-e", endpoint,
                "-b", bucket,
                "-a", accessKey,
                "-s", secretKey,
                "-p", profile,
                "-f", file,
                "-t", "" + threads,
                "-r",
                "--re-replicate-delete-markers",
                "--re-replicate-custom-acls"
        };

        ReReplicationTool.Config config = (ReReplicationTool.Config) ReReplicationToolCli.parseConfig(
                new DefaultParser().parse(ReReplicationToolCli.options(), args));

        Assertions.assertEquals(endpoint, config.getEndpoint().toString());
        Assertions.assertEquals(bucket, config.getBucket());
        Assertions.assertEquals(accessKey, config.getAccessKey());
        Assertions.assertEquals(secretKey, config.getSecretKey());
        Assertions.assertEquals(profile, config.getAwsProfile());
        Assertions.assertEquals(file, config.getInventoryFile().toString());
        Assertions.assertEquals(threads, config.getThreadCount());
        Assertions.assertTrue(config.isReReplicateDeleteMarkers());
        Assertions.assertTrue(config.isReReplicateCustomAcls());
    }

    @Test
    public void testReReplicationToolCliDefaults() throws Exception {
        String endpoint = "endpoint-1", bucket = "bucket-1", accessKey = "accessKey-1";
        String secretKey = "secretKey-1", file = "file-1";

        String[] args = {
                "-e", endpoint,
                "-b", bucket,
                "-a", accessKey,
                "-s", secretKey,
                "-f", file,
                "-r"
        };

        ReReplicationTool.Config config = (ReReplicationTool.Config) ReReplicationToolCli.parseConfig(
                new DefaultParser().parse(ReReplicationToolCli.options(), args));

        Assertions.assertEquals(endpoint, config.getEndpoint().toString());
        Assertions.assertEquals(bucket, config.getBucket());
        Assertions.assertEquals(accessKey, config.getAccessKey());
        Assertions.assertEquals(secretKey, config.getSecretKey());
        Assertions.assertNull(config.getAwsProfile());
        Assertions.assertEquals(file, config.getInventoryFile().toString());
        Assertions.assertEquals(AbstractVersionScanningTool.Config.DEFAULT_THREAD_COUNT, config.getThreadCount());
        Assertions.assertFalse(config.isReReplicateDeleteMarkers());
        Assertions.assertFalse(config.isReReplicateCustomAcls());
    }
}
