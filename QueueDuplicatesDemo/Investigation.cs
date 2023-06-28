using Azure.Storage.Queues;

namespace QueueDuplicatesDemo
{
    public class Investigation
    {
        private readonly string _storageConnectionString = "<connection-string>";

        private readonly QueueServiceClient _queueServiceClient;
        private readonly string _investigationUid;
        private readonly Dictionary<int, QueueClient> _queueClients;

        private readonly int _queueCount;
        private readonly int _itemsPerQueue;

        public Investigation(int queueCount = 20, int itemsPerQueue = 50000)
        {
            _queueCount = queueCount;
            _itemsPerQueue = itemsPerQueue;
            _queueClients = new Dictionary<int, QueueClient>(queueCount);

            _queueServiceClient = CreateQueueServiceClient();
            _investigationUid = Guid.NewGuid().ToString("N")[..8];
        }

        public async Task Run()
        {
            Console.WriteLine($">>> Starting investigation run [{_investigationUid}]");

            await InitializeQueues();

            var tasks = new List<Task>();

            foreach (var queueClient in _queueClients.Values)
            {
                var task = QueueItems(queueClient);
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);

            Console.WriteLine($"<<< Finished investigation run [{_investigationUid}]");
        }

        private QueueServiceClient CreateQueueServiceClient()
        {
            var queueClientOptions = new QueueClientOptions
            {
                MessageEncoding = QueueMessageEncoding.None
            };

            //queueClientOptions.Retry.Delay = TimeSpan.FromSeconds(2);

            return new QueueServiceClient(_storageConnectionString, queueClientOptions);
        }

        private async Task InitializeQueues()
        {
            for (int i = 1; i <= _queueCount; i++)
            {
                var queueName = CreateQueueName(i);
                var queueClient = await GetQueueClientAsync(queueName);
                _queueClients.Add(i, queueClient);
            }
        }

        private string CreateQueueName(int i)
        {
            return $"queue-{_investigationUid}-{i.ToString().PadLeft(3, '0')}";
        }

        private async Task<QueueClient> GetQueueClientAsync(string queueName)
        {
            var queueClient = _queueServiceClient.GetQueueClient(queueName);
            var response = await queueClient.CreateIfNotExistsAsync();
            if (response != null)
            {
                Console.WriteLine($"Created migration queue [{queueClient.Name}]");
            }

            return queueClient;
        }

        private async Task QueueItems(QueueClient queueClient)
        {
            Console.WriteLine($"Starting queueing [{_itemsPerQueue}] items into the queue [{queueClient.Name}]");

            await Parallel.ForEachAsync(Enumerable.Range(1, _itemsPerQueue), async (item, token) =>
            {
                try
                {
                    var result = await queueClient.SendMessageAsync($"ItemIndex: {item}", timeToLive: TimeSpan.FromSeconds(-1));
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            });

            Console.WriteLine($"Finished queueing [{_itemsPerQueue}] items into the queue [{queueClient.Name}]");
        }
    }
}
