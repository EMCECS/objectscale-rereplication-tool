package com.dellemc.objectscale.tool;

import com.dellemc.objectscale.util.TestConfig;
import com.dellemc.objectscale.util.TestProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
import software.amazon.awssdk.services.s3.model.ObjectVersion;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractTest {
    String s3Endpoint;
    String awsProfile;
    URI proxyUri;
    S3Client s3Client;

    abstract String getBucket();

    @BeforeAll
    public void setup() throws Exception {
        s3Endpoint = TestConfig.getPropertyNotEmpty(TestProperties.S3_ENDPOINT);
        awsProfile = TestConfig.getPropertyNotEmpty(TestProperties.AWS_PROFILE);
        String proxyUriStr = TestConfig.getProperties().getProperty(TestProperties.PROXY_ENDPOINT);
        if (proxyUriStr != null) {
            proxyUri = URI.create(proxyUriStr);
            System.setProperty("http.proxyHost", proxyUri.getHost());
            System.setProperty("http.proxyPort", "" + proxyUri.getPort());
        }
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(s3Endpoint))
                .credentialsProvider(ProfileCredentialsProvider.create(awsProfile))
                .build();
        // create bucket
        s3Client.createBucket(builder -> builder.bucket(getBucket()));
        // enable versioning
        s3Client.putBucketVersioning(builder -> builder.bucket(getBucket())
                .versioningConfiguration(builder1 -> builder1
                        .status(BucketVersioningStatus.ENABLED)));
    }

    @AfterAll
    public void teardown() throws Exception {
        if (s3Client != null) {
            s3Client.deleteBucket(builder -> builder.bucket(getBucket()));
            s3Client.close();
        }
    }

    /**
     * creates <code>objectKeyCount</code> object keys named <code>object-N</code> where N = 0 to
     * <code>objectKeyCount-1</code> - all odd-numbered keys will then be deleted, such that there are
     * <code>objectKeyCount/2</code> delete markers and a total of <code>objectKeyCount*1.5</code> object versions
     */
    void createObjectVersions(int objectKeyCount) throws Exception {
        // create versions - nest in a ForkJoinPool to increase threads from the common pool (which only has nCPU-1 threads)
        new ForkJoinPool(32).submit(() ->
                IntStream.range(0, objectKeyCount).parallel().forEach(value -> {
                    String key = "object-" + value;
                    s3Client.putObject(builder -> builder.bucket(getBucket()).key(key), RequestBody.empty());
                    if (value % 2 == 1) {
                        // odds will be delete markers
                        s3Client.deleteObject(builder -> builder.bucket(getBucket()).key(key));
                    }
                })
        ).get();
    }

    // convenience method for child classes
    void cleanBucket(String bucket) throws Exception {
        if (s3Client != null) {
            List<ObjectVersion> versions = new ArrayList<>();
            List<DeleteMarkerEntry> dms = new ArrayList<>();
            s3Client.listObjectVersionsPaginator(builder -> builder.bucket(bucket))
                    .forEach(page -> {
                        versions.addAll(page.versions());
                        dms.addAll(page.deleteMarkers());
                    });
            new ForkJoinPool(32).submit(() -> {
                versions.stream().parallel().forEach(v -> s3Client.deleteObject(builder -> builder.bucket(bucket).key(v.key()).versionId(v.versionId())));
                dms.stream().parallel().forEach(dm -> s3Client.deleteObject(builder -> builder.bucket(bucket).key(dm.key()).versionId(dm.versionId())));
            }).get();
        }
    }
}
