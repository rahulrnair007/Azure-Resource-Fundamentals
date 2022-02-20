using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;

namespace SampleSessions.Functions
{
    public static class SampleProcessor
    {
        [FunctionName(nameof(SampleProcessor))]
        public async static Task RunAsync([ServiceBusTrigger("%queuename%", Connection = "ServiceBusStorageConnectionString", IsSessionsEnabled = true)]
        Message message, IMessageSession messageSession,
            ILogger log)
        {

            log.Log(LogLevel.Information, $"Session: {message.SessionId}, Message Id: {message.MessageId}");

        }
    }
}