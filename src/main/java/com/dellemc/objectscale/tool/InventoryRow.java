package com.dellemc.objectscale.tool;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.s3.model.ReplicationStatus;

import java.time.Instant;

@AllArgsConstructor
@Getter
public class InventoryRow implements Comparable<InventoryRow> {
    private final String key;
    private final String versionId;
    private final Boolean isDeleteMarker;
    private final Boolean isLatest;
    private final Instant lastModified;
    private final String eTag;
    private final Long size;
    private final String ownerId;
    @Setter
    private ReplicationStatus replicationStatus;

    public Object[] toFieldArray() {
        return new Object[]{ // should match Header values below
                key, versionId, isDeleteMarker, isLatest, lastModified, eTag, size, ownerId, replicationStatus
        };
    }

    @Override
    public int compareTo(InventoryRow o) {
        int result = key.compareTo(o.key);
        if (result == 0) result = versionId.compareTo(o.versionId);
        return result;
    }

    enum Header {
        Key, VersionId, IsDeleteMarker, IsLatest, LastModified, ETag, Size, OwnerId, ReplicationStatus
    }
}
