using QueueDuplicatesDemo;

var investigation = new Investigation(queueCount: 20, itemsPerQueue: 50000);

await investigation.Run();
