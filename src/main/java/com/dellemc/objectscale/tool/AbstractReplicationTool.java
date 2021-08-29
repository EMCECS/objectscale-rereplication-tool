package com.dellemc.objectscale.tool;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.AttributeMap;

import java.net.URI;
import java.nio.file.Path;

public abstract class AbstractReplicationTool implements Runnable, AutoCloseable {
    private static final Logger log = LogManager.getLogger(AbstractReplicationTool.class);

    protected final Config config;
    protected final S3Client s3Client;
    private final boolean createdClient;
    private boolean closed = false;
    protected ProcessingStats grossRecords;
    protected ProcessingStats filteredRecords;

    public AbstractReplicationTool(Config config, S3Client s3Client) {
        this.config = config;
        if (s3Client != null) {
            this.s3Client = s3Client;
            this.createdClient = false;
        } else {
            this.s3Client = createClient(config);
            this.createdClient = true;
        }
    }

    abstract String getGrossRecordsLabel();

    abstract String getFilteredRecordsLabel();

    @Override
    public synchronized void close() {
        if (!closed) {
            if (createdClient && s3Client != null) {
                try {
                    s3Client.close();
                } catch (Exception ignored) {
                }
            }
            closed = true;
        }
    }

    S3Client createClient(Config config) {
        AwsCredentialsProvider credentialsProvider;
        if (!Strings.isBlank(config.awsProfile)) {
            credentialsProvider = ProfileCredentialsProvider.create(config.awsProfile);
        } else if (!Strings.isBlank(config.accessKey)) {
            credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(config.accessKey, config.secretKey));
        } else {
            credentialsProvider = DefaultCredentialsProvider.create();
        }

        SdkHttpClient httpClient;
        if (config.disableSslValidation) {
            httpClient = ApacheHttpClient.builder().buildWithDefaults(
                    AttributeMap.builder()
                            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                            .build());
        } else {
            httpClient = ApacheHttpClient.builder().build();
        }

        return S3Client.builder()
                .endpointOverride(config.endpoint)
                .credentialsProvider(credentialsProvider)
                .region(Region.US_EAST_1) // TODO: would this ever need to be different?
                .httpClient(httpClient)
                .build();
    }

    public ProcessingStats getGrossRecords() {
        return grossRecords;
    }

    public void setGrossRecords(ProcessingStats grossRecords) {
        this.grossRecords = grossRecords;
    }

    public ProcessingStats getFilteredRecords() {
        return filteredRecords;
    }

    public void setFilteredRecords(ProcessingStats filteredRecords) {
        this.filteredRecords = filteredRecords;
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    @EqualsAndHashCode
    @ToString(exclude = "secretKey")
    public static class Config {
        public static final int DEFAULT_THREAD_COUNT = 32;

        private final URI endpoint;
        private final String bucket;
        private final String accessKey;
        private final String secretKey;
        private final String awsProfile;
        private final Path inventoryFile;
        @Builder.Default
        private final int threadCount = DEFAULT_THREAD_COUNT;
        private final boolean disableSslValidation;

        /**
         * Validate this configuration
         */
        public void validate() {
            if (endpoint == null)
                throw new IllegalArgumentException("endpoint is required");

            if (Strings.isBlank(bucket))
                throw new IllegalArgumentException("bucket is required");

            if (inventoryFile == null)
                throw new IllegalArgumentException("inventoryFile is required");

            if (!Strings.isBlank(accessKey) && Strings.isBlank(secretKey))
                throw new IllegalArgumentException("when using accessKey, you must provide a secretKey");

            if (disableSslValidation)
                log.warn("SSL validation is disabled - this is NOT safe!");
        }
    }
}
