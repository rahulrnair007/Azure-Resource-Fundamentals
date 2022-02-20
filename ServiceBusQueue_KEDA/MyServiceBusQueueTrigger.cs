using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace Company.Function
{
    public static class MyServiceBusQueueTrigger
    {
        [FunctionName("MyServiceBusQueueTrigger")]
        public static void Run([ServiceBusTrigger("queuename", Connection = "ServiceBusStorageConnectionString", IsSessionsEnabled = true)]string myQueueItem, ILogger log)
        {
            // Prerequisite -- Need to install KEDA on your local/azure kubernates cluster
            // instances are scaled by using component called KEDA.
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");
        }
    }
}
