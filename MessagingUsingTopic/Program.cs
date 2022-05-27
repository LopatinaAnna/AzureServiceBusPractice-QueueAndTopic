using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace MessagingUsingTopic
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Enter Service Bus connection string:");
            string serviceBusConnectionString = Console.ReadLine();

            Console.WriteLine("Enter topic name:");
            string topicName = Console.ReadLine();

            Console.WriteLine("Enter subscription name:");
            string subscriptionName = Console.ReadLine();

            Console.WriteLine("Sending a message to a topic...");
            SendMessageAsync(serviceBusConnectionString, topicName).GetAwaiter().GetResult();

            Console.WriteLine("Retrieving a message from topic...");
            ReceiveMessageAsync(serviceBusConnectionString, topicName, subscriptionName).GetAwaiter().GetResult();
        }

        static async Task SendMessageAsync(string serviceBusConnectionString, string topicName)
        {
            await using var client = new ServiceBusClient(serviceBusConnectionString);
            await using ServiceBusSender sender = client.CreateSender(topicName);

            try
            {
                var message = new ServiceBusMessage($"Testing {DateTime.Now}");
                await sender.SendMessageAsync(message);
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        static async Task ReceiveMessageAsync(string serviceBusConnectionString, string topicName, string subscriptionName)
        {
            var client = new ServiceBusClient(serviceBusConnectionString);

            var processorOptions = new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = 1,
                AutoCompleteMessages = false
            };

            ServiceBusProcessor processor = client.CreateProcessor(topicName, subscriptionName, processorOptions);

            processor.ProcessMessageAsync += MessageHandler;
            processor.ProcessErrorAsync += ErrorHandler;

            await processor.StartProcessingAsync();

            Console.Read();

            await processor.DisposeAsync();
            await client.DisposeAsync();
        }

        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            Console.WriteLine($"Received message: SequenceNumber:{args.Message.SequenceNumber} Body:{args.Message.Body}");
            await args.CompleteMessageAsync(args.Message);
        }

        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine($"Message handler encountered an exception {args.Exception}.");
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {args.FullyQualifiedNamespace}");
            Console.WriteLine($"- Entity Path: {args.EntityPath}");
            Console.WriteLine($"- Executing Action: {args.ErrorSource}");
            return Task.CompletedTask;
        }
    }
}
