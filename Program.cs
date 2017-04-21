namespace ServiceBusStreamInterop
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    public class Program
    {
        const string ConnectionString = "Endpoint=sb://...";
        const string Topic = "sample";
        const string Subscription = "sample";

        public static void Main(string[] args)
        {
            const string payload = "foo bar";

            Console.WriteLine($"Sending messages with content '{payload}':");
            Console.WriteLine();

            var ms = new MemoryStream();
            using (var sw = new StreamWriter(ms, System.Text.Encoding.UTF8, 256, true))
            {
                sw.Write(payload);
            }
            ms.Position = 0;

            var program = new Program();

            program.SendLegacy(ms).Wait();
            program.SendNew(ms).Wait();

            program.ReceiveLegacy().Wait();
            program.ReceiveLegacy().Wait();
        }

        public async Task SendLegacy(MemoryStream stream)
        {
            var msg = new BrokeredMessage(stream)
            {
                MessageId = Guid.NewGuid().ToString(),
                Label = "WindowsAzure.ServiceBus",
            };

            var factory = MessagingFactory.CreateFromConnectionString(ConnectionString);
            var sender = await factory.CreateMessageSenderAsync(Topic);
            await sender.SendAsync(msg);
            await sender.CloseAsync();
            await factory.CloseAsync();

            Console.WriteLine($"#{msg.MessageId} is sent via {msg.Label}");
            Console.WriteLine();
        }

        public async Task SendNew(MemoryStream stream)
        {
            var msg = new Message(stream.ToArray())
            {
                MessageId = Guid.NewGuid().ToString(),
                Label = "Microsoft.Azure.ServiceBus",
            };

            var topicClient = new Microsoft.Azure.ServiceBus.TopicClient(ConnectionString, Topic);
            await topicClient.SendAsync(msg);
            await topicClient.CloseAsync();

            Console.WriteLine($"#{msg.MessageId} is sent via {msg.Label}");
            Console.WriteLine();
        }

        public async Task ReceiveLegacy()
        {
            var factory = MessagingFactory.CreateFromConnectionString(ConnectionString);
            var receiver = await factory.CreateMessageReceiverAsync($"{Topic}/subscriptions/{Subscription}", Microsoft.ServiceBus.Messaging.ReceiveMode.ReceiveAndDelete);

            var msg = await receiver.ReceiveAsync();

            Console.WriteLine($"#{msg.MessageId} received (from {msg.Label}):");

            var stream = msg.GetBody<Stream>();
            using (var sr = new StreamReader(stream))
            {
                var content = await sr.ReadToEndAsync();
                Console.WriteLine($"  Content: '{content}'");
            }

            Console.WriteLine();

            await receiver.CloseAsync();
            await factory.CloseAsync();
        }
    }
}
