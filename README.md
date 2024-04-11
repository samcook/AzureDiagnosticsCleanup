# AzureDiagnosticsCleanup

A utility to clean up Azure Diagnostics logs tables in an Azure Storage Account.

Over time the diagnostics tables can grow to a large size, and they are not automatically trimmed.

## Usage

Obtain the URL and a SAS token for the storage account you want to clean up. The SAS token will need enough permissions to read and delete tables and entities.

Update the `config.json` file with the appropriate settings, or pass values on the command line.

### Modes

#### WAD Metrics

These metrics are stored in tables with the prefix `WADMetrics`, with a new table created every 10 days. Cleanup is relatively easy, as we can just drop older tables.

```
AzureDiagnosticsCleanup.exe --mode WADMetrics --cutoffdate 2024-01-01T00:00:00Z
```

#### WAD Logs

These logs are stored in tables such as `WADPerformanceCountersTable`, `WADWindowsEventLogsTable` and `WADDiagnosticInfrastructureLogsTable`.

Any table with `PartitionKey` values such as `0638308144800000000` (which is a `DateTime.Ticks` value representing the time the log was written) can be cleaned up in this mode.

Due to the fact that all of the records are stored in a single table, in order to clean up entries without dumping everything we need to query all the entities with a `PartitionKey` value less than the cutoff date, and then delete them. As a result this mode can be slow. Running the cleanup from a host within Azure can help throughput significantly.

```
AzureDiagnosticsCleanup.exe --mode WADData --Data:TableName WADPerformanceCountersTable --cutoffdate 2024-01-01T00:00:00Z
```

#### Linux Logs

Similar to WAD Logs, these logs are stored in tables such as `LinuxCpuVer2v0`, `LinuxDiskVer2v0`, `LinuxMemoryVer2v0` and `LinuxsyslogVer2v0`.

The partition key format for this mode is `0000000000000000001___0636951868800000000` (where the first section's last digit is `0` through `9`, and the second section is a `DateTime.Ticks` value representing the time the log entry was written).

This mode also requires querying all entities with a `PartitionKey` value less than the cutoff date, and then deleting them.

```
AzureDiagnosticsCleanup.exe --mode LinuxData --Data:TableName LinuxCpuVer2v0 --cutoffdate 2024-01-01T00:00:00Z
```
