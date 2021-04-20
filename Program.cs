using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Azure.Messaging.ServiceBus;

using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Primitives;
using Microsoft.Azure.ServiceBus.Core;
using System.Text;
using Newtonsoft.Json;

namespace ServiceBusDemo
{
    class Program
    {

        public static string connectionString = "Endpoint=sb://sb-demosk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=P8WDU/Q5D0kksAJP0U5U9XkkQeOe96JY+qKUxA/+OVM=;TransportType=AmqpWebSockets";
        public static string queueName = "my-notification";
        //public static string subscriptionName = "<SERVICE BUS - TOPIC SUBSCRIPTION NAME>";

        public static async Task Main(string[] args)
        {    
            Console.WriteLine("======================================================");

            // Send message using AMQP
            //await SendMessageAsync();

            // Receive message using AMQP
            //await ReceiveMessageAsync();

            // Send message using WS
            //await WSSendMessageAsync();
            
            // Send batch of messages
            await WSSendMessagesInBatchAsync(3);
            
            // Receive message using WS Listner
            await WSReceiveMessageListenerAsync();

            // Receive messages using WS Batches
            //await WSReceiveMessagesAsync(3);

            Console.Read();
        }

        static async Task SendMessageAsync()
        {
            // create a Service Bus client 
            await using (ServiceBusClient client = new ServiceBusClient(connectionString))
            {
                // create a sender for the queue 
                ServiceBusSender sender = client.CreateSender(queueName);

                // create a message that we can send
                ServiceBusMessage message = new ServiceBusMessage("AMQP Message :"+DateTime.Now);

                // send the message
                await sender.SendMessageAsync(message);
                Console.WriteLine($"Sent a single message to the queue: {queueName} using AMQP");
            }
        } 

        static async Task ReceiveMessageAsync()
        {
            // create a Service Bus receiver client 
            await using (ServiceBusClient client = new ServiceBusClient(connectionString))
            {
                // create the options to use for configuring the processor
                var options = new ServiceBusProcessorOptions
                {
                    // By default or when AutoCompleteMessages is set to true, the processor will complete the message after executing the message handler
                    // Set AutoCompleteMessages to false to [settle messages](https://docs.microsoft.com/en-us/azure/service-bus-messaging/message-transfers-locks-settlement#peeklock) on your own.
                    // In both cases, if the message handler throws an exception without settling the message, the processor will abandon the message.
                    AutoCompleteMessages = false,

                    // I can also allow for multi-threading
                    MaxConcurrentCalls = 2
                };

                // create a processor that we can use to process the messages
                //ServiceBusProcessor processor = client.CreateProcessor(queueName, subscriptionName, new ServiceBusProcessorOptions());
                //https://docs.microsoft.com/en-us/dotnet/api/overview/azure/messaging.servicebus-readme-pre#send-and-receive-a-message
                ServiceBusProcessor processor = client.CreateProcessor(queueName, options);

                // configure/add message and error handler to use
                processor.ProcessMessageAsync += MessageHandler;
                processor.ProcessErrorAsync += ErrorHandler;

                // start processing 
                await processor.StartProcessingAsync();

                Console.WriteLine("Wait for a minute and then press any key to end the processing");
                Console.Read();

                // stop processing 
                Console.WriteLine("\nStopping the receiver...");
                await processor.StopProcessingAsync();
                Console.WriteLine("Stopped receiving messages");
                }
        }

        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            //Console.WriteLine($"Received: {body} from subscription: {subscriptionName}");
            Console.WriteLine($"Received: {body} ");

            // complete the message. messages is deleted from the queue. 
            await args.CompleteMessageAsync(args.Message);
        }

        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        } 


        static async Task WSSendMessageAsync()
        {                
            var builder = new ServiceBusConnectionStringBuilder("Endpoint=sb://sb-demosk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=P8WDU/Q5D0kksAJP0U5U9XkkQeOe96JY+qKUxA/+OVM=");
            /*
            var tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(
                builder.SasKeyName,
                builder.SasKey);

            var sender = new MessageSender(
                builder.Endpoint,
                "queueName",
                tokenProvider,
                TransportType.AmqpWebSockets);
*/
            var sender = new MessageSender(connectionString, queueName, RetryPolicy.Default);

            await sender.SendAsync(new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject("WS Message :"+DateTime.Now))));
            Console.WriteLine($"Sent a single message to the queue: {queueName} using WebSockets");
            
        } 

        static async Task WSSendMessagesInBatchAsync(int numberOfMessagesToSend)
        {                
            var builder = new ServiceBusConnectionStringBuilder("Endpoint=sb://sb-demosk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=P8WDU/Q5D0kksAJP0U5U9XkkQeOe96JY+qKUxA/+OVM=");
            var sender = new MessageSender(connectionString, queueName, RetryPolicy.Default);
            IList<Message> messages = new List<Message>();

            //while(numberOfMessagesToSend-- > 0)
            for(var i=0;i<numberOfMessagesToSend;i++)
            {
                messages.Add(new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject("Batch:"+i+" Time: "+DateTime.Now))));
            }

            // send the messages
            await sender.SendAsync(messages);

            Console.WriteLine($"Sent a batch of {numberOfMessagesToSend} messages to the queue: {queueName} using WebSockets");
            
        } 


        static async Task WSReceiveMessageAsync()
        {                
            var builder = new ServiceBusConnectionStringBuilder("Endpoint=sb://sb-demosk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=P8WDU/Q5D0kksAJP0U5U9XkkQeOe96JY+qKUxA/+OVM=");
            var receiver = new MessageReceiver(connectionString, queueName, ReceiveMode.PeekLock, RetryPolicy.Default);

            Message message = await receiver.ReceiveAsync();
            if(message != null){
                string body = Encoding.UTF8.GetString(message.Body);

                Console.WriteLine($"WS Received: {body} ");
            }
            
            //close
            await receiver.CloseAsync();
        } 
        

        static async Task WSReceiveMessageListenerAsync()
        {            
            var ExitRequested= false;
            var builder = new ServiceBusConnectionStringBuilder("Endpoint=sb://sb-demosk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=P8WDU/Q5D0kksAJP0U5U9XkkQeOe96JY+qKUxA/+OVM=");
            var receiver = new MessageReceiver(connectionString, queueName, ReceiveMode.PeekLock, RetryPolicy.Default);
            Message message;
            //string body;
            while (!ExitRequested)
            {
                try
                {
                   message = await receiver.ReceiveAsync(TimeSpan.FromSeconds(1));
                    if (message != null){
                        Console.WriteLine($"WS Listner Received: {Encoding.UTF8.GetString(message.Body)} ");
                        
                        //close
                        await receiver.CompleteAsync(message.SystemProperties.LockToken);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error Occured: {ex} ");
                }
            }    
        }
        
        //multiple messages polling
        static async Task WSReceiveMessagesInBatchAsync(int numberOfMessagesToReceive)
        {
            while(numberOfMessagesToReceive-- > 0)
            {
                var builder = new ServiceBusConnectionStringBuilder("Endpoint=sb://sb-demosk.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=P8WDU/Q5D0kksAJP0U5U9XkkQeOe96JY+qKUxA/+OVM=");
                var receiver = new MessageReceiver(connectionString, queueName, ReceiveMode.PeekLock, RetryPolicy.Default);
            
                // Receive the message
                Message message = await receiver.ReceiveAsync();

                // Process the message
                Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");

                // Complete the message so that it is not received again.
                // This can be done only if the MessageReceiver is created in ReceiveMode.PeekLock mode (which is default).
                await receiver.CompleteAsync(message.SystemProperties.LockToken);
            }
        }
    }

}
