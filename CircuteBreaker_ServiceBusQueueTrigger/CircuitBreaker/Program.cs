using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Management;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TestConsoleForQueue
{
    /// <summary>
    /// Program
    /// </summary>
    internal class Program
    {
        /// <summary>
        /// Main
        /// </summary>
        /// <param name="args"></param>
        private static void Main(string[] args)
        {
            new MessageReceiverClient();

            Console.ReadKey();
        }
    }

    /// <summary>
    /// Represents MessageReceiverClient
    /// </summary>
    public class MessageReceiverClient
    {
        private const string ServiceBusConnectionString = "ConnectioString";
        private const string QueueName = "queuename";
        private static IMessageReceiver _messageReceiver;
        private static int retryCount = 5;
        private static ManagementClient _managementClient;
        private static IQueueClient _queueClient;

        /// <summary>
        /// Constructor.
        /// </summary>
        public MessageReceiverClient()
        {
            if (_managementClient == null)
            {
                _managementClient = new ManagementClient(ServiceBusConnectionString);
            }
            var res =  GetNamespaceInfo();
            //used MessageReceiver since queueclient doesn't have ReceiveMessagesAsBatch in .net core.
            _messageReceiver = new MessageReceiver(ServiceBusConnectionString, QueueName, ReceiveMode.PeekLock);

            //Initial registration with servicebus.
            RegisterOnMessageHandlerAndReceiveMessages();
        }

        /// <summary>
        /// GetNamespaceInfo
        /// </summary>
        /// <returns></returns>
        private static async Task GetNamespaceInfo()
        {
            var queueDescription = await _managementClient.GetQueueAsync(QueueName);
            var result = await _managementClient.GetNamespaceInfoAsync();
        }

        /// <summary>
        /// RegisterOnMessageHandlerAndReceiveMessages
        /// </summary>
        private static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 10,
                AutoComplete = false
            };
            _messageReceiver.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        /// <summary>
        /// ProcessMessagesAsync
        /// </summary>
        /// <param name="message"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        private static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            Console.WriteLine($"RegisterMessageHandler Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
            await _messageReceiver.CompleteAsync(message.SystemProperties.LockToken);
            IsQueueInitialized();
            if (!message.UserProperties.ContainsKey("Retry_Count"))
            {
                message.UserProperties["Retry_Count"] = 0;
                message.UserProperties["original-SequenceNumber"] = message.SystemProperties.SequenceNumber;
            }

            if (CosmosHealthChecker())
            {
                //API call and save goes normal.
            }
            else
            {
                await ExponentialRetryAsync(message);
            }
        }

        /// <summary>
        /// ExponentialRetryAsync
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        private static async Task ExponentialRetryAsync(Message message)
        {
            if ((int)message.UserProperties["Retry_Count"] == retryCount)
            {
                //reset
                await ScheduleMessage(message);
                await ReceiveDisabled(EntityStatus.ReceiveDisabled);
            }
            else
            {
                await ScheduleMessage(message);
            }
        }

        /// <summary>
        /// ScheduleMessage
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        private static async Task ScheduleMessage(Message message)
        {
            var retryMessage = message.Clone();

            var updatedRetryCount = (int)message.UserProperties["Retry_Count"] + 1;

            if (updatedRetryCount == retryCount)
            {
                await ReceiveDisabled(EntityStatus.ReceiveDisabled);
                return;
            }

            var interval = retryCount * updatedRetryCount;

            var scheduledTime = DateTimeOffset.Now.AddSeconds(interval);

            await _queueClient.ScheduleMessageAsync(retryMessage, scheduledTime);

            await _messageReceiver.CompleteAsync(message.SystemProperties.LockToken);

            if (updatedRetryCount == retryCount)
            {
                retryMessage.UserProperties["Retry_Count"] = 0;
                await ReceiveDisabled(EntityStatus.ReceiveDisabled);
            }

            Console.WriteLine($"Scheduling message retry {updatedRetryCount} to wait {interval} seconds and arrive at {scheduledTime.UtcDateTime}");
        }

        /// <summary>
        /// ExceptionReceivedHandler
        /// </summary>
        /// <param name="exceptionReceivedEventArgs"></param>
        /// <returns></returns>
        private static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }

        /// <summary>
        /// ReceiveDisabled
        /// </summary>
        /// <param name="entityStatus"></param>
        /// <returns></returns>
        public static async Task ReceiveDisabled(EntityStatus entityStatus)
        {
            var queueDescription = await GetQueueDescription();
            if (queueDescription.Status != entityStatus)
            {
                queueDescription.Status = entityStatus;
                await _managementClient.UpdateQueueAsync(queueDescription);
            }
        }

        /// <summary>
        /// GetQueueDescription
        /// </summary>
        /// <returns></returns>
        private static async Task<QueueDescription> GetQueueDescription()
        {
            if (_managementClient == null)
            {
                _managementClient = new ManagementClient(ServiceBusConnectionString);
            }
            var queueDescription = await _managementClient.GetQueueAsync("sbq-inputormstore");
            return queueDescription;
        }

        /// <summary>
        /// IsQueueInitialized
        /// </summary>
        private static void IsQueueInitialized()
        {
            if (_queueClient == null)
            {
                _queueClient = new QueueClient(ServiceBusConnectionString, "sbq-inputormstore", ReceiveMode.PeekLock);
            }
        }

        /// <summary>
        /// CosmosHealthChecker
        /// </summary>
        /// <returns></returns>
        private static bool CosmosHealthChecker()
        {
            // Do some Operation for Cosmos Health..
            return false;
        }
    }
}