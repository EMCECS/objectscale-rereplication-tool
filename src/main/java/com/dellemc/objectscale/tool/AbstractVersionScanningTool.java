package com.dellemc.objectscale.tool;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.logging.log4j.util.Strings;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.nio.file.Path;

public abstract class AbstractVersionScanningTool implements Runnable, AutoCloseable {
    protected final Config config;
    protected final S3Client s3Client;
    private final boolean createdClient;
    private boolean closed = false;

    public AbstractVersionScanningTool(Config config, S3Client s3Client) {
        this.config = config;
        if (s3Client != null) {
            this.s3Client = s3Client;
            this.createdClient = false;
        } else {
            this.s3Client = createClient(config);
            this.createdClient = true;
        }
    }

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

        return S3Client.builder()
                .endpointOverride(config.endpoint)
                .credentialsProvider(credentialsProvider)
                .build();
    }

    @SuperBuilder(toBuilder = true)
    @Getter
    @EqualsAndHashCode
    @ToString
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
        }
    }
}
