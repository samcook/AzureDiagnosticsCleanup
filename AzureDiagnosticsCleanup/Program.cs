using System;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Threading.Tasks.Dataflow;
using Azure;
using Azure.Data.Tables;
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
                Console.WriteLine("Canceling...");
                cts.Cancel();
                e.Cancel = true;
            };

            var configBuilder = new ConfigurationBuilder().AddJsonFile("config.json");
            var config = configBuilder.Build();

            ServicePointManager.DefaultConnectionLimit = Math.Max(2, Convert.ToInt32(config["MaxDOP"] ?? "1"));

            var adc = new AzureDiagnosticsCleanup(
                new TableServiceClient(
                    new Uri(config["AzureTableStorage:Url"] ?? throw new ApplicationException("Table storage URL not specified")),
                    new AzureSasCredential(config["AzureTableStorage:SASToken"] ?? throw new ApplicationException("SAS Token not specified"))));

            await adc.CleanupAzureDiagnosticsDataAsync(
                    config["AzureTableStorage:TableName"] ?? throw new ApplicationException("Table name not specified"),
                    DateTimeOffset.Parse(config["CutOffDate"] ?? throw new ApplicationException("Cutoff Date not specified"), DateTimeFormatInfo.InvariantInfo),
                    Convert.ToInt32(config["MaxDOP"] ?? "1"),
                    Convert.ToInt32(config["StatusIntervalSeconds"] ?? "1"),
                    cts.Token)
                .ConfigureAwait(false);
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

        private static async Task ProcessBatch(TableClient tableClient, string? batchId, ICollection<TableTransactionAction> toDelete)
        {
            //Console.WriteLine($"Enter ProcessBatch({batchId}) with {toDelete.Count} items");
            await tableClient.SubmitTransactionAsync(toDelete, CancellationToken.None).ConfigureAwait(false);
            //Console.WriteLine($"Leave ProcessBatch({batchId})");
        }

        public async Task CleanupAzureDiagnosticsDataAsync(string table, DateTimeOffset cutoff, int maxDop, int statusIntervalSeconds, CancellationToken token = default)
        {
            var sw = Stopwatch.StartNew();

            var toDelete = new List<TableTransactionAction>(100);
            var queued = 0L;
            var deleted = 0L;
            var transactions = 0;
            string? lastPartitionKey = null;

            Console.WriteLine($"{nameof(CleanupAzureDiagnosticsDataAsync)}({table}, {cutoff})");

            var tableClient = tableServiceClient.GetTableClient(table);
            var actionBlock = new ActionBlock<(string? BatchId, List<TableTransactionAction> ToDelete)>(
                async x =>
                {
                    await ProcessBatch(tableClient, x.BatchId, x.ToDelete);
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
                // ReSharper disable once AccessToModifiedClosure
                Console.WriteLine($"Queued: {queued}, Deleted: {deleted}, Txns: {transactions}, Queue Depth: {actionBlock.InputCount}, Elapsed: {sw.Elapsed:g}, Avg del rate: {deleted/sw.Elapsed.TotalSeconds:F0}/sec");
            }, null, TimeSpan.Zero, TimeSpan.FromSeconds(statusIntervalSeconds));

            await foreach (var entity in tableClient.QueryAsync<TableEntity>(
                                   filter: $"PartitionKey le '0{ConvertDateTimeToAzTime(cutoff)}'",
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
                    if (!result) {
                        Console.WriteLine("Failed to send to action block");
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
                    Console.WriteLine("Failed to send to action block");
                }
            }

            actionBlock.Complete();
            await actionBlock.Completion.ConfigureAwait(false);

            sw.Stop();

            var waitHandle = new ManualResetEvent(false);
            timer.Dispose(waitHandle);
            waitHandle.WaitOne();

            Console.WriteLine($"Deleted {deleted} entries in {transactions} transactions in {sw.Elapsed:g}, avg del rate: {deleted/sw.Elapsed.TotalSeconds:F0}/sec");
        }

        public static long ConvertDateTimeToAzTime(DateTimeOffset dateTimeOffset)
        {
            var t = dateTimeOffset.AddYears(1600);

            var az = (t.ToUnixTimeSeconds() + 11644473600) * (long)1e7;

            return az;
        }

        public static DateTimeOffset ConvertAzTimeToDateTime(long azTime)
        {
            var t = azTime / (long)1e7 - 11644473600;

            var dt = DateTimeOffset.FromUnixTimeSeconds(t).AddYears(-1600);

            return dt;
        }
    }
}
