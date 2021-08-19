package com.dellemc.objectscale.tool;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.net.URI;
import java.nio.file.Paths;
import java.util.concurrent.*;

public class ReReplicationCli {
    private static final Logger log = LogManager.getLogger(ReReplicationCli.class);
    public static final String RELEASE_VERSION = ReReplicationCli.class.getPackage().getImplementationVersion();

    static Options options() {
        Options options = new Options();

        options.addOption(Option.builder("e").longOpt("endpoint")
                .desc("ObjectScale S3 endpoint URL. This includes scheme and port (i.e. https://10.1.4.5:9021)")
                .hasArg().argName("endpoint-uri").required().build());
        options.addOption(Option.builder("b").longOpt("bucket").desc("The bucket to inventory ")
                .hasArg().argName("bucket-name").required().build());
        options.addOption(Option.builder("a").longOpt("access-key").desc("The AWS Access Key ID to access the bucket (if not using an AWS profile)")
                .hasArg().argName("access-key").build());
        options.addOption(Option.builder("s").longOpt("secret-key").desc("The AWS Secret Key to access the bucket (if not using an AWS profile)")
                .hasArg().argName("secret-key").build());
        options.addOption(Option.builder("p").longOpt("profile").desc("The AWS CLI profile to use for credentials, if other than default (configuration must be set for this profile)")
                .hasArg().argName("profile-name").build());
        options.addOption(Option.builder("f").longOpt("file")
                .desc("The file to read when triggering re-replication, or write when generating an inventory (in CSV format)")
                .hasArg().argName("inventory-file").build());
        options.addOption(Option.builder("t").longOpt("threads")
                .desc("The size of the thread pool used to HEAD and COPY objects for inventory or re-replication")
                .hasArg().argName("thread-count").build());
        options.addOption(Option.builder().longOpt("unsafe-disable-ssl-validation")
                .desc("Disables SSL/TLS certificate validation - this is NOT safe!").build());

        OptionGroup commandGroup = new OptionGroup();
        commandGroup.addOption(Option.builder("i").longOpt("inventory")
                .desc("Perform an inventory of the bucket and output to CSV")
                .build());
        commandGroup.addOption(Option.builder("r").longOpt("re-replicate")
                .desc("Trigger re-replication of a list of objects from a provided file. Re-replication is triggered by COPYing the object to itself to create a new version, which will trigger CRR policy replication for that new version")
                .build());
        commandGroup.setRequired(true);
        options.addOptionGroup(commandGroup);

        // inventory options
        OptionGroup filterGroup = new OptionGroup(); // filter sub-group
        filterGroup.addOption(Option.builder("c").longOpt("current-version")
                .desc("Only inventory the current object versions (do not include previous/non-current versions)")
                .build());
        filterGroup.addOption(Option.builder().longOpt("failed-current-version")
                .desc("Only inventory the current object versions that failed replication (do not include previous/non-current versions or replicated versions) - this is the default")
                .build());
        filterGroup.addOption(Option.builder().longOpt("all-versions")
                .desc("Inventory all object versions").build());
        options.addOptionGroup(filterGroup);
        options.addOption(Option.builder().longOpt("prefix")
                .desc("Only inventory objects in the bucket that are under this prefix")
                .hasArg().argName("bucket-prefix").build());
        options.addOption(Option.builder().longOpt("force-overwrite")
                .desc("When performing inventory, if the inventory file already exists, overwrite it").build());

        options.addOption(Option.builder().longOpt("re-replicate-custom-acls")
                .desc("Adds support for custom ACLs during re-replication. WARNING: this will triple the API calls to S3 and take longer to complete")
                .build());

        // logging options
        options.addOption(Option.builder("v").longOpt("verbose").desc("Verbose logging").build());
        options.addOption(Option.builder("d").longOpt("debug").desc("Debug logging").build());

        // help output
        options.addOption(Option.builder("h").longOpt("help").desc("Print this help text").build());

        return options;
    }

    static AbstractReplicationTool.Config parseConfig(CommandLine commandLine) {
        AbstractReplicationTool.Config config;
        if (commandLine.hasOption("re-replicate")) {
            config = ReReplicationProcessor.Config.builder()
                    .reReplicateCustomAcls(commandLine.hasOption("re-replicate-custom-acls"))
                    .build();
        } else {
            config = InventoryGenerator.Config.builder()
                    .filterType(filterTypeFromCli(commandLine))
                    .forceOverwrite(commandLine.hasOption("force-overwrite"))
                    .prefix(commandLine.getOptionValue("prefix"))
                    .build();
        }

        config = config.toBuilder()
                .endpoint(URI.create(commandLine.getOptionValue("endpoint")))
                .bucket(commandLine.getOptionValue("bucket"))
                .accessKey(commandLine.getOptionValue("access-key"))
                .secretKey(commandLine.getOptionValue("secret-key"))
                .awsProfile(commandLine.getOptionValue("profile"))
                .inventoryFile(Paths.get(commandLine.getOptionValue("file")))
                .disableSslValidation(commandLine.hasOption("unsafe-disable-ssl-validation"))
                .build();

        if (commandLine.hasOption("threads")) {
            config = config.toBuilder()
                    .threadCount(Integer.parseInt(commandLine.getOptionValue("threads")))
                    .build();
        }

        return config;
    }

    static InventoryGenerator.FilterType filterTypeFromCli(CommandLine commandLine) {
        if (commandLine.hasOption("current-version")) {
            return InventoryGenerator.FilterType.CurrentVersionOnly;
        } else if (commandLine.hasOption("all-versions")) {
            return InventoryGenerator.FilterType.AllVersions;
        } else {
            return InventoryGenerator.FilterType.FailedCurrentVersionOnly;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("rereplication-tool - a tool to inventory replication status or re-trigger replication for object versions in a bucket with an associated CRR policy.");
        System.out.println("Version: " + RELEASE_VERSION + "\n");

        // check for help option
        CommandLine commandLine = new DefaultParser().parse(new Options().addOption(Option.builder("h").build()), args, true);
        if (commandLine.hasOption('h')) {
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("java -jar rereplication-tool-1.0.jar -e <endpoint> -b <bucket> (-i|-r) -f <inventory-file> [options]",
                    "options:", options(), null);
            System.out.println();

        } else {
            commandLine = new DefaultParser().parse(options(), args);

            // set log level
            if (commandLine.hasOption('d')) {
                Configurator.setLevel(LogManager.getRootLogger().getName(), Level.DEBUG);
            } else if (commandLine.hasOption('v')) {
                Configurator.setLevel(LogManager.getRootLogger().getName(), Level.INFO);
            }

            AbstractReplicationTool.Config config = parseConfig(commandLine);
            log.info("parsed options:\n{}", config);
            config.validate();

            try (AbstractReplicationTool tool = commandLine.hasOption("inventory")
                    ? new InventoryGenerator((InventoryGenerator.Config) config)
                    : new ReReplicationProcessor((ReReplicationProcessor.Config) config)) {
                long now = System.currentTimeMillis();
                ProcessingStats grossRecords = new ProcessingStats(now), filteredRecords = new ProcessingStats(now);
                tool.setGrossRecords(grossRecords);
                tool.setFilteredRecords(filteredRecords);
                CompletableFuture<Void> toolFuture = CompletableFuture.runAsync(tool);

                ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
                ScheduledFuture<?> statsFuture = executor.scheduleAtFixedRate(() -> {
                    // print stats every second
                    System.out.print(getStatsLine(tool) + "\r");
                }, 1, 1, TimeUnit.SECONDS);

                toolFuture.join(); // waits for tool to complete

                // record end-time for stats
                long endTime = System.currentTimeMillis();
                grossRecords.setEndTimeMillis(endTime);
                filteredRecords.setEndTimeMillis(endTime);

                // stop the stats printer
                statsFuture.cancel(true);
                executor.shutdown();
                System.out.println(getStatsLine(tool));
                System.out.println("Done.");
            } // try-with-resources will close the tool (and associated S3Client)
        }
    }

    static String getStatsLine(AbstractReplicationTool tool) {
        return String.format("%s: %d (%d/s) [%d errors], %s: %d (%d/s) [%d errors]\r",
                tool.getGrossRecordsLabel(), tool.getGrossRecords().getProcessedObjects(),
                tool.getGrossRecords().getPerSecondAverage(), tool.getGrossRecords().getErrors(),
                tool.getFilteredRecordsLabel(), tool.getFilteredRecords().getProcessedObjects(),
                tool.getFilteredRecords().getPerSecondAverage(), tool.getFilteredRecords().getErrors());
    }
}
