using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;


namespace MessagingUsingQueue
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Enter Service Bus connection string:");
            string serviceBusConnectionString = Console.ReadLine();

            Console.WriteLine("Enter queue name:");
            string queueName = Console.ReadLine();

            Console.WriteLine("Sending a message to a queue...");
            SendMessageAsync(serviceBusConnectionString, queueName).GetAwaiter().GetResult();

            Console.WriteLine("Receiving a message from a queue...");
            ReceiveMessageAsync(serviceBusConnectionString, queueName).GetAwaiter().GetResult();
        }

        static async Task SendMessageAsync(string serviceBusConnectionString, string queueName)
        {
            await using var client = new ServiceBusClient(serviceBusConnectionString);
            await using ServiceBusSender sender = client.CreateSender(queueName);

            try
            {
                var message = new ServiceBusMessage($"Testing {DateTime.Now}");
                await sender.SendMessageAsync(message);
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
            finally
            {
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }
        }

        static async Task ReceiveMessageAsync(string serviceBusConnectionString, string queueName)
        {
            var client = new ServiceBusClient(serviceBusConnectionString);

            var processorOptions = new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = 1,
                AutoCompleteMessages = false
            };

            await using ServiceBusProcessor processor = client.CreateProcessor(queueName, processorOptions);

            processor.ProcessMessageAsync += MessageHandler;
            processor.ProcessErrorAsync += ErrorHandler;

            await processor.StartProcessingAsync();

            Console.Read();

            await processor.CloseAsync();
        }

        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received: {body}");

            await args.CompleteMessageAsync(args.Message);
        }

        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }
    }
}
