# ObjectScale CRR Inventory and Re-Replication Tool

A tool to inventory replication status or re-trigger replication for object versions in a bucket with an associated CRR policy.

## Usage

1. Install Java
1. Download the latest [released version](https://github.com/EMCECS/objectscale-rereplication-tool/releases) of the tool.
1. Run the tool:  
```text
java -jar rereplication-tool-1.0.jar -e <endpoint> -b <bucket> (-i|-r) -f <inventory-file> [options]
```  

### Running an Inventory (`-i`)

To run an inventory, specify the `-i` option.  An inventory will list all the versions in the bucket,
filter based on optional criteria, and generate a CSV report.

#### Inventory Options

Option | Flag | Description
--|--|--
Filter: Current Versions Only | `--current-version` | Only inventories the current object versions. WARNING: this could produce a very large file
Filter: Failed Current Versions | `--failed-current-version` | Only inventories current object versions that have failed replication (this is the default)
Filter: All Versions | `--all-versions` | Inventories all object versions in the bucket. WARNING: this could produce a massive file
Filter: Bucket Prefix | `--prefix` | Only inventories object keys that are under the given prefix

### Re-triggering Replication (`-r`)

To re-trigger replication for failed current versions, specify the `-r` option, and provide an inventory
with the failed object versions to re-trigger.  The tool will read the inventory file and re-trigger replication
by issueing PUT+COPY operations for any failed current versions.

Note that the tool will also accept a flat list of object keys, or a 2-column CSV with object keys in the first column
and version IDs in the second column.  You can use this to explicitly trigger replication for those keys and versions.  Be aware that
triggering replication means writing a new current version of each object by copying a previous version.  Please
be cautious when using an explicit list of keys or version IDs that does not include is-latest or replication-status
details.

#### Re-Replication Options

Option | Flag | Description
--|--|--
Support Custom ACLs | `re-replicate-custom-acls` | Adds support for custom ACLs during re-replication. Disabled by default. Most users should not need to worry about custom ACLs, but if you know your application is using per-object ACLs, you will need to enable this to maintain them

## Report Fields

The inventory report will generate a CSV with the following fields (in this order):

Field|Data Type|Description
--|--|--
Key|string|The object key (name)
VersionId|string|The object version ID
IsDeleteMarker|boolean|True if the version is a delete-marker
IsLatest|boolean|True if the version is the current (latest) version of the key
LastModified|datetime|The time the version was written to the bucket
ETag|string|The S3 ETag (almost always the MD5-hex of the data)
Size|number|The size of the object data in bytes
OwnerId|string|The object owner (S3 user)
ReplicationStatus|string|Replication status of the object (`PENDING`, `COMPLETE`, or `FAILED`)

Note: The CSV file will have a header row with these field names in it.

## Full CLI Syntax
```text
usage: java -jar rereplication-tool-1.2.jar -e <endpoint> -b <bucket>
            (-i|-r) -f <inventory-file> [options]
options:
 -a,--access-key <access-key>         The AWS Access Key ID to access the
                                      bucket (if not using an AWS profile)
    --all-versions                    Inventory all object versions
 -b,--bucket <bucket-name>            The bucket to inventory
 -c,--current-version                 Only inventory the current object
                                      versions (do not include
                                      previous/non-current versions)
 -d,--debug                           Debug logging
 -e,--endpoint <endpoint-uri>         ObjectScale S3 endpoint URL. This
                                      includes scheme and port (i.e.
                                      https://10.1.4.5:9021)
 -f,--file <inventory-file>           The file to read when triggering
                                      re-replication, or write when
                                      generating an inventory (in CSV
                                      format)
    --failed-current-version          Only inventory the current object
                                      versions that failed replication (do
                                      not include previous/non-current
                                      versions or replicated versions) -
                                      this is the default
    --force-overwrite                 When performing inventory, if the
                                      inventory file already exists,
                                      overwrite it
 -h,--help                            Print this help text
 -i,--inventory                       Perform an inventory of the bucket
                                      and output to CSV
 -p,--profile <profile-name>          The AWS CLI profile to use for
                                      credentials, if other than default
                                      (configuration must be set for this
                                      profile)
    --prefix <bucket-prefix>          Only inventory objects in the bucket
                                      that are under this prefix
 -r,--re-replicate                    Trigger re-replication of a list of
                                      objects from a provided file.
                                      Re-replication is triggered by
                                      COPYing the object to itself to
                                      create a new version, which will
                                      trigger CRR policy replication for
                                      that new version
    --re-replicate-custom-acls        Adds support for custom ACLs during
                                      re-replication. WARNING: this will
                                      triple the API calls to S3 and take
                                      longer to complete
 -s,--secret-key <secret-key>         The AWS Secret Key to access the
                                      bucket (if not using an AWS profile)
 -t,--threads <thread-count>          The size of the thread pool used to
                                      HEAD and COPY objects for inventory
                                      or re-replication
    --unsafe-disable-ssl-validation   Disables SSL/TLS certificate
                                      validation - this is NOT safe!
 -v,--verbose                         Verbose logging
```

# Dependency Licenses

To generate a dependency license report, simply execute the following build task:

```shell
./gradlew generateLicenseReport
```
