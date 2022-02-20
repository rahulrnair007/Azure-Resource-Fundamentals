using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;

namespace SampleSessions.Functions
{
    public static class SampleProcessor
    {
        [FunctionName(nameof(SampleProcessor))]
        public async static Task RunAsync([ServiceBusTrigger("%queuename%", Connection = "ServiceBusStorageConnectionString", IsSessionsEnabled = true)]
        ServiceBusReceivedMessage[] messages, ServiceBusMessageActions messageActions,
            ILogger log)
        {
            foreach (var message in messages)
            {
                log.Log(LogLevel.Information, $"Session: {message.SessionId}, MessageId: {message.MessageId}");
            }

        }
    }
}