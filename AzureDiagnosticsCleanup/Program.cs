using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks.Dataflow;
using Azure;
using Azure.Data.Tables;
using Azure.Identity;
using Microsoft.Extensions.Configuration;

namespace AzureDiagnosticsCleanup
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("AzureDiagnosticsCleanup");
            var cts = new CancellationTokenSource();

            Console.CancelKeyPress += (sender, e) =>
            {
                Console.WriteLine("Cancelling...");
                cts.Cancel();
                e.Cancel = true;
            };

            var configBuilder = new ConfigurationBuilder()
                .AddJsonFile("config.json")
                .AddCommandLine(args, new Dictionary<string, string>
                {
                    {"-m", "Mode"}
                });

            var config = configBuilder.Build();

            ServicePointManager.DefaultConnectionLimit = Math.Max(2, Convert.ToInt32(config["MaxDOP"] ?? "1"));

            var adc = new AzureDiagnosticsCleanup(GetTableServiceClient(config));

            switch (config["Mode"])
            {
                case "WADMetrics":
                    await adc.CleanupWADMetricsTablesAsync(
                            "WADMetricsP",
                            DateTimeOffset.Parse(config["CutOffDate"] ?? throw new ApplicationException("Cutoff Date not specified"), DateTimeFormatInfo.InvariantInfo),
                            config.GetValue("DryRun", true),
                            cts.Token)
                        .ConfigureAwait(false);
                    break;

                case "WADData":
                    await adc.CleanupWADDataAsync(
                            config["Data:TableName"] ?? throw new ApplicationException("Table name not specified"),
                            DateTimeOffset.Parse(config["CutOffDate"] ?? throw new ApplicationException("Cutoff Date not specified"), DateTimeFormatInfo.InvariantInfo),
                            config.GetValue("MaxDOP", 1),
                            config.GetValue("StatusIntervalSeconds", 1),
                            config.GetValue("DryRun", true),
                            cts.Token)
                        .ConfigureAwait(false);

                    break;

                case "LinuxData":
                    await adc.CleanupLinuxDataAsync(
                            config["Data:TableName"] ?? throw new ApplicationException("Table name not specified"),
                            DateTimeOffset.Parse(config["CutOffDate"] ?? throw new ApplicationException("Cutoff Date not specified"), DateTimeFormatInfo.InvariantInfo),
                            config.GetValue("MaxDOP", 1),
                            config.GetValue("StatusIntervalSeconds", 1),
                            config.GetValue("DryRun", true),
                            cts.Token)
                        .ConfigureAwait(false);

                    break;

                default:
                    Console.WriteLine("Unknown mode");
                    return;
            }
        }

        private static TableServiceClient GetTableServiceClient(IConfiguration config)
        {
            switch (config["AzureTableStorage:AuthMode"]?.ToLowerInvariant())
            {
                case "sastoken":
                    Console.WriteLine("AuthMode: Using SAS token");

                    return new TableServiceClient(
                        new Uri(config["AzureTableStorage:Url"] ?? throw new ApplicationException("Table storage URL not specified")),
                        new AzureSasCredential(config["AzureTableStorage:SASToken"] ?? throw new ApplicationException("SAS Token not specified"))
                    );

                case "entraid":
                    Console.WriteLine("AuthMode: Using Azure Identity DefaultAzureCredential provider");

                    return new TableServiceClient(
                        new Uri(config["AzureTableStorage:Url"] ?? throw new ApplicationException("Table storage URL not specified")),
                        new DefaultAzureCredential(true));

                default:
                    throw new ApplicationException("Unknown authentication mode");
            }
        }
    }

    // Based on https://www.nomad.ee/soft/azure_cleanup.shtml

    public class AzureDiagnosticsCleanup
    {
        private readonly TableServiceClient tableServiceClient;
        private static readonly string[] Columns = { "PartitionKey", "RowKey" };

        public AzureDiagnosticsCleanup(TableServiceClient tableServiceClient)
        {
            this.tableServiceClient = tableServiceClient;
        }

        public async Task CleanupWADMetricsTablesAsync(string tablePrefix, DateTimeOffset cutoff, bool dryRun, CancellationToken token = default)
        {
            Console.WriteLine($"{nameof(CleanupWADMetricsTablesAsync)}({tablePrefix}, {cutoff:u})");

            var rangeStart = tablePrefix;
            var rangeEnd = tablePrefix[..^1] + (char)(tablePrefix[^1] + 1); // a bit hacky, but probably fine for this purpose
            var filter = $"TableName ge '{rangeStart}' and TableName lt '{rangeEnd}'";

            Console.WriteLine(filter);

            string? previousName = null;

            await foreach (var table in tableServiceClient.QueryAsync(filter).OrderBy(x => x.Name)
                               .WithCancellation(CancellationToken.None)
                               .ConfigureAwait(false))
            {
                if (token.IsCancellationRequested)
                {
                    break;
                }

                _ = TryParseWADMetricsTableName(previousName, out var previousNamePrefix, out var previousIntervalStart);

                if (!TryParseWADMetricsTableName(table.Name, out var currentNamePrefix, out var currentIntervalStart))
                {
                    // skip over entries that don't match the pattern
                    continue;
                }

                // if the prefix has changed, ignore the previous details
                if (previousNamePrefix != currentNamePrefix)
                {
                    previousName = null;
                    previousNamePrefix = null;
                    previousIntervalStart = null;
                }
                
                //Console.WriteLine($"{previousName} => {table.Name}, {previousIntervalStart} => {currentIntervalStart}, {previousNamePrefix}/{currentNamePrefix}");

                if (previousName != null && previousIntervalStart != null && currentIntervalStart < cutoff)
                {
                    Console.WriteLine($"Delete {previousName}");

                    if (!dryRun)
                    {
                        await tableServiceClient.DeleteTableAsync(previousName, CancellationToken.None).ConfigureAwait(false);
                    }
                }

                previousName = table.Name;
            }
        }

        private static bool TryParseWADMetricsTableName(string? tableName, out string? prefix, out DateTimeOffset? date)
        {
            if (tableName == null)
            {
                prefix = null;
                date = null;
                return false;
            }

            // WADMetricsPT1HP10DV2Syyyymmdd
            // WADMetricsPT1MP10DV2Syyyymmdd
            // yyyymmdd is creation date of the table

            var match = Regex.Match(tableName, @"(.+PT\d+\wP\d+\wV2S)(\d{4})(\d{2})(\d{2})");

            if (!match.Success)
            {
                prefix = null;
                date = null;
                return false;
            }

            prefix = match.Groups[1].Value;
            date = new DateTimeOffset(
                    int.Parse(match.Groups[2].Value),
                    int.Parse(match.Groups[3].Value),
                    int.Parse(match.Groups[4].Value),
                    0, 0, 0, TimeSpan.Zero
                );
            return true;
        }

        public async Task CleanupLinuxDataAsync(string table, DateTimeOffset cutoff, int maxDop, int statusIntervalSeconds, bool dryRun, CancellationToken token = default)
        {
            Console.WriteLine($"{nameof(CleanupLinuxDataAsync)}({table}, {cutoff:u})");

            for (var i = 0; i <= 9; i++)
            {
                if (token.IsCancellationRequested)
                {
                    break;
                }

                var fromPartitionKey = $"{i:D19}___{0:D19}";                // 0000000000000000001___0000000000000000000
                var toPartitionKey = $"{i:D19}___{cutoff.UtcTicks:D19}";    // 0000000000000000001___0636951868800000000

                var filter = $"PartitionKey ge '{fromPartitionKey}' and PartitionKey lt '{toPartitionKey}'";

                await CleanupTableDataAsync(
                        table,
                        filter,
                        cutoff,
                        maxDop,
                        statusIntervalSeconds,
                        x => long.TryParse(x?[22..], out var lastPartitionKeyLong) ? $"{new DateTimeOffset(lastPartitionKeyLong, TimeSpan.Zero):u}" : null,
                        dryRun,
                        token)
                    .ConfigureAwait(false);
            }
        }

        public async Task CleanupWADDataAsync(string table, DateTimeOffset cutoff, int maxDop, int statusIntervalSeconds, bool dryRun, CancellationToken token = default)
        {
            Console.WriteLine($"{nameof(CleanupWADDataAsync)}({table}, {cutoff:u})");

            var filter = $"PartitionKey le '{cutoff.UtcTicks:D19}'";

            await CleanupTableDataAsync(
                    table,
                    filter,
                    cutoff,
                    maxDop,
                    statusIntervalSeconds,
                    x => long.TryParse(x, out var lastPartitionKeyLong) ? $"{new DateTimeOffset(lastPartitionKeyLong, TimeSpan.Zero):u}" : null,
                    dryRun,
                    token)
                .ConfigureAwait(false);
        }

        private async Task CleanupTableDataAsync(string table, string filter, DateTimeOffset cutoff, int maxDop, int statusIntervalSeconds, Func<string?, string?> lastPartitionKeyStatusParser, bool dryRun, CancellationToken token = default)
        {
            Console.WriteLine($"Filter: {filter}");

            var sw = Stopwatch.StartNew();

            var toDelete = new List<TableTransactionAction>(100);
            var queued = 0L;
            var deleted = 0L;
            var transactions = 0;
            string? lastPartitionKey = null;

            var tableClient = tableServiceClient.GetTableClient(table);
            var actionBlock = new ActionBlock<(string? BatchId, List<TableTransactionAction> ToDelete)>(
                async x =>
                {
                    await ProcessTableTransactionAsync(tableClient, x.BatchId, x.ToDelete, dryRun).ConfigureAwait(false);
                    Interlocked.Add(ref deleted, x.ToDelete.Count);
                    Interlocked.Increment(ref transactions);
                },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = maxDop,
                    BoundedCapacity = maxDop * 4
                });

            var timer = new Timer(state =>
            {
                // ReSharper disable AccessToModifiedClosure
                Console.WriteLine($"Last: {lastPartitionKeyStatusParser(lastPartitionKey)}, Queued: {queued:N0}, Deleted: {deleted:N0}, Txns: {transactions:N0}, QD: {actionBlock.InputCount}, Time: {sw.Elapsed:g}, Del/s: {deleted/sw.Elapsed.TotalSeconds:F0}/s, Txn/s: {transactions/sw.Elapsed.TotalSeconds:F0}/s");
                // ReSharper restore AccessToModifiedClosure
            }, null, TimeSpan.Zero, TimeSpan.FromSeconds(statusIntervalSeconds));

            try
            {
                await foreach (var entity in tableClient.QueryAsync<TableEntity>(
                                       filter: filter,
                                       select: Columns,
                                       cancellationToken: CancellationToken.None)
                                   .ConfigureAwait(false))
                {
                    //Console.WriteLine($"{ConvertAzTimeToDateTime(Convert.ToInt64(entity.PartitionKey))} {entity.PartitionKey} {entity.RowKey}");

                    //if (queued >= 10000)
                    //{
                    //    break;
                    //}

                    if (toDelete.Count > 0 && (entity.PartitionKey != lastPartitionKey || toDelete.Count == 100))
                    {
                        //Console.WriteLine($"Delete {lastPartitionKey}, items: {toDelete.Count}, qd: {actionBlock.InputCount}");

                        var result = await actionBlock.SendAsync((lastPartitionKey, toDelete), CancellationToken.None).ConfigureAwait(false);
                        if (!result)
                        {
                            throw new ApplicationException("Failed to send to action block");
                        }

                        toDelete = new List<TableTransactionAction>(100);
                    }

                    if (token.IsCancellationRequested)
                    {
                        break;
                    }

                    toDelete.Add(new TableTransactionAction(TableTransactionActionType.Delete, entity));

                    lastPartitionKey = entity.PartitionKey;
                    queued++;
                }

                if (toDelete.Count != 0)
                {
                    //Console.WriteLine($"Delete {lastPartitionKey}, items: {toDelete.Count}, qd: {actionBlock.InputCount}");

                    var result = await actionBlock.SendAsync((lastPartitionKey, toDelete), CancellationToken.None).ConfigureAwait(false);
                    if (!result)
                    {
                        throw new ApplicationException("Failed to send to action block");
                    }
                }
            }
            catch (ApplicationException ex) when (ex.Message == "Failed to send to action block")
            {
                Console.WriteLine("Failed to send to action block");
            }

            Console.WriteLine("Completing action block");

            actionBlock.Complete();
            await actionBlock.Completion.ConfigureAwait(false);

            sw.Stop();

            var waitHandle = new ManualResetEvent(false);
            timer.Dispose(waitHandle);
            waitHandle.WaitOne();

            Console.WriteLine($"{table}: Deleted {deleted:N0} entries in {transactions:N0} transactions in {sw.Elapsed:g}, Del/s: {deleted/sw.Elapsed.TotalSeconds:F0}/s, Txn/s: {transactions / sw.Elapsed.TotalSeconds:F0}/s");
        }

        private static async Task ProcessTableTransactionAsync(TableClient tableClient, string? batchId, ICollection<TableTransactionAction> toDelete, bool dryRun)
        {
            //Console.WriteLine($"Enter ProcessTableTransactionAsync({batchId}) with {toDelete.Count} items");
            if (!dryRun)
            {
                var sw = Stopwatch.StartNew();

                try
                {
                    await tableClient.SubmitTransactionAsync(toDelete, CancellationToken.None).ConfigureAwait(false);
                }
                catch (TableTransactionFailedException ex)
                {
                    sw.Stop();

                    Console.WriteLine($"SubmitTransactionAsync failed after {sw.ElapsedMilliseconds}ms: {ex}");

                    // try one more time
                    //await tableClient.SubmitTransactionAsync(toDelete, CancellationToken.None).ConfigureAwait(false);
                }
            }
            else
            {
                //Console.WriteLine($"Dry run: delete {batchId} with {toDelete.Count} items");
            }
            //Console.WriteLine($"Leave ProcessTableTransactionAsync({batchId})");
        }
    }
}
