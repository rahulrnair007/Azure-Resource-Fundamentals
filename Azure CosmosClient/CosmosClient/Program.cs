using System;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace Cosmos.Samples.Shared
{
    /// <summary>
    /// Family
    /// </summary>
    public class Family
    {
        [JsonProperty("id")]
        public string Id { get; set; }
        public string LastName { get; set; }
        public Parent[] Parents { get; set; }
        public Child[] Children { get; set; }
        public Address Address { get; set; }
        public bool IsRegistered { get; set; }
    }
    /// <summary>
    /// Parent
    /// </summary>
    public class Parent
    {
        public string FamilyName { get; set; }
        public string FirstName { get; set; }
    }

    /// <summary>
    /// Child
    /// </summary>
    public class Child
    {
        public string FamilyName { get; set; }
        public string FirstName { get; set; }
        public string Gender { get; set; }
        public int Grade { get; set; }
        public Pet[] Pets { get; set; }
    }

    /// <summary>
    /// Pet
    /// </summary>
    public class Pet
    {
        public string GivenName { get; set; }
    }

    /// <summary>
    /// Address
    /// </summary>
    public class Address
    {
        public string State { get; set; }
        public string County { get; set; }
        public string City { get; set; }
    }

    /// <summary>
    /// Program
    /// </summary>
    class Program
    {
        //Read configuration
        private static readonly string databaseId = "FamilyDatabase";
        private static readonly string containerId = "FamilyContainer";
        private static readonly string autoscaleContainerId = "autoscale-container-samples";
        private static readonly string partitionKeyPath = "/activityId";
        // The Azure Cosmos DB endpoint for running this sample.
        private static readonly string EndpointUri = "https://localhost:8081";
        // The primary key for the Azure Cosmos account.
        private static readonly string PrimaryKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

        private static Database database = null;

        // Async main requires c# 7.1 which is set in the csproj with the LangVersion attribute
        // <Main>
        public static async Task Main(string[] args)
        {
            try
            {
                var options = new CosmosClientOptions()
                {
                    ConnectionMode = ConnectionMode.Gateway,
                    ConsistencyLevel = ConsistencyLevel.Session,
                    SerializerOptions = new CosmosSerializationOptions()
                };
                using (CosmosClient client = new CosmosClient(EndpointUri, PrimaryKey))
                {
                    await Program.RunContainerDemo(client);
                }
            }
            catch (CosmosException cre)
            {
                Console.WriteLine(cre.ToString());
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }
        // </Main>

        /// <summary>
        /// Run through basic container access methods as a console app demo.
        /// </summary>
        /// <returns></returns>
        // <RunContainerDemo>
        private static async Task RunContainerDemo(CosmosClient client)
        {
            // Create the database if necessary
            await Program.Setup(client);

            Container simpleContainer = await Program.CreateContainer();


            await upsertItemsAsync(simpleContainer);


            getItems(simpleContainer);
            await Program.CreateAndUpdateAutoscaleContainer();

            await Program.CreateContainerWithCustomIndexingPolicy();

            await Program.CreateContainerWithTtlExpiration();

            await Program.GetAndChangeContainerPerformance(simpleContainer);

            await Program.ReadContainerProperties();

            await Program.ListContainersInDatabase();

            //Uncomment to delete container!
            await Program.DeleteContainer();
        }

        private static async  void getItems(Container simpleContainer)
        {
            var PartitionKey = new PartitionKey(partitionKeyPath);
            var item = await simpleContainer.ReadItemStreamAsync("Andersen.1", PartitionKey);
        }

        private static async Task upsertItemsAsync(Container simpleContainer)
        {
            Family andersenFamily = new Family
            {
                Id = "Andersen.1",
                LastName = "Andersen",
                Parents = new Parent[]
        {
            new Parent { FirstName = "Thomas" },
            new Parent { FirstName = "Mary Kay" }
        },
                Children = new Child[]
        {
            new Child
            {
                FirstName = "Henriette Thaulow",
                Gender = "female",
                Grade = 5,
                Pets = new Pet[]
                {
                    new Pet { GivenName = "Fluffy" }
                }
            }
        },
                Address = new Address { State = "WA", County = "King", City = "Seattle" },
                IsRegistered = false
            };
            var PartitionKey = new PartitionKey(partitionKeyPath);
            await simpleContainer.UpsertItemAsync(andersenFamily, PartitionKey);

        }

        // </RunContainerDemo>

        private static async Task Setup(CosmosClient client)
        {
            database = await client.CreateDatabaseIfNotExistsAsync(databaseId);
        }

        // <CreateContainer>
        private static async Task<Container> CreateContainer()
        {
            // Set throughput to the minimum value of 400 RU/s
            ContainerResponse simpleContainer = await database.CreateContainerIfNotExistsAsync(
                id: containerId,
                partitionKeyPath: partitionKeyPath,
                throughput: 400);

            Console.WriteLine($"{Environment.NewLine}1.1. Created container :{simpleContainer.Container.Id}");
            return simpleContainer;
        }
        // </CreateContainer>

        // <CreateAndUpdateAutoscaleContainer>
        private static async Task CreateAndUpdateAutoscaleContainer()
        {
            // Set autoscale throughput to the maximum value of 10000 RU/s
            ContainerProperties containerProperties = new ContainerProperties(autoscaleContainerId, partitionKeyPath);

            Container autoscaleContainer = await database.CreateContainerIfNotExistsAsync(
                containerProperties: containerProperties,
                throughputProperties: ThroughputProperties.CreateAutoscaleThroughput(autoscaleMaxThroughput: 10000));

            Console.WriteLine($"{Environment.NewLine}1.2. Created autoscale container :{autoscaleContainer.Id}");

            ThroughputResponse throughputResponse = await autoscaleContainer.ReadThroughputAsync(requestOptions: null);

            Console.WriteLine($"{Environment.NewLine}1.2.1. Found autoscale throughput {Environment.NewLine}The current throughput: {throughputResponse.Resource.Throughput} Max throughput: {throughputResponse.Resource.AutoscaleMaxThroughput} " +
                $"using container's id: {autoscaleContainer.Id}");

            int? currentThroughput = await autoscaleContainer.ReadThroughputAsync();

            Console.WriteLine($"{Environment.NewLine}1.2.2. Found autoscale throughput {Environment.NewLine}The current throughput: {currentThroughput} using container's id: {autoscaleContainer.Id}");


            ThroughputResponse throughputUpdateResponse = await autoscaleContainer.ReplaceThroughputAsync(ThroughputProperties.CreateAutoscaleThroughput(15000));

            Console.WriteLine($"{Environment.NewLine}1.2.3. Replaced autoscale throughput. {Environment.NewLine}The current throughput: {throughputUpdateResponse.Resource.Throughput} Max throughput: {throughputUpdateResponse.Resource.AutoscaleMaxThroughput} " +
                $"using container's id: {autoscaleContainer.Id}");

            // Get the offer again after replace
            throughputResponse = await autoscaleContainer.ReadThroughputAsync(requestOptions: null);

            Console.WriteLine($"{Environment.NewLine}1.2.4. Found autoscale throughput {Environment.NewLine}The current throughput: {throughputResponse.Resource.Throughput} Max throughput: {throughputResponse.Resource.AutoscaleMaxThroughput} " +
                $"using container's id: {autoscaleContainer.Id}{Environment.NewLine}");

            // Delete the container
            await autoscaleContainer.DeleteContainerAsync();
        }
        // </CreateAndUpdateAutoscaleContainer>

        // <CreateContainerWithCustomIndexingPolicy>
        private static async Task CreateContainerWithCustomIndexingPolicy()
        {
            // Create a container with custom index policy (consistent indexing)
            // We cover index policies in detail in IndexManagement sample project
            ContainerProperties containerProperties = new ContainerProperties(
                id: "SampleContainerWithCustomIndexPolicy",
                partitionKeyPath: partitionKeyPath);
            containerProperties.IndexingPolicy.IndexingMode = IndexingMode.Consistent;

            Container containerWithConsistentIndexing = await database.CreateContainerIfNotExistsAsync(
                containerProperties,
                throughput: 400);

            Console.WriteLine($"1.3 Created Container {containerWithConsistentIndexing.Id}, with custom index policy {Environment.NewLine}");

            await containerWithConsistentIndexing.DeleteContainerAsync();
        }
        // </CreateContainerWithCustomIndexingPolicy>

        // <CreateContainerWithTtlExpiration>
        private static async Task CreateContainerWithTtlExpiration()
        {
            ContainerProperties properties = new ContainerProperties
                (id: "TtlExpiryContainer",
                partitionKeyPath: partitionKeyPath);
            properties.DefaultTimeToLive = (int)TimeSpan.FromDays(1).TotalSeconds; //expire in 1 day

            ContainerResponse ttlEnabledContainerResponse = await database.CreateContainerIfNotExistsAsync(
                containerProperties: properties);
            ContainerProperties returnedProperties = ttlEnabledContainerResponse;

            Console.WriteLine($"{Environment.NewLine}1.4 Created Container {Environment.NewLine}{returnedProperties.Id} with TTL expiration of {returnedProperties.DefaultTimeToLive}");

            await ttlEnabledContainerResponse.Container.DeleteContainerAsync();
        }
        // </CreateContainerWithTtlExpiration>

        // <GetAndChangeContainerPerformance>
        private static async Task GetAndChangeContainerPerformance(Container simpleContainer)
        {

            int? throughputResponse = await simpleContainer.ReadThroughputAsync();

            Console.WriteLine($"{Environment.NewLine}2. Found throughput {Environment.NewLine}{throughputResponse}{Environment.NewLine}using container's id {Environment.NewLine}{simpleContainer.Id}");

            await simpleContainer.ReplaceThroughputAsync(500);

            Console.WriteLine($"{Environment.NewLine}3. Replaced throughput. Throughput is now 500.{Environment.NewLine}");

            // Get the offer again after replace
            throughputResponse = await simpleContainer.ReadThroughputAsync();

            Console.WriteLine($"3. Found throughput {Environment.NewLine}{throughputResponse}{Environment.NewLine} using container's ResourceId {simpleContainer.Id}.{Environment.NewLine}");
        }
        // </GetAndChangeContainerPerformance>

        // <ReadContainerProperties>
        private static async Task ReadContainerProperties()
        {
            Container container = database.GetContainer(containerId);
            ContainerProperties containerProperties = await container.ReadContainerAsync();

            Console.WriteLine($"{Environment.NewLine}4. Found Container {Environment.NewLine}{containerProperties.Id}{Environment.NewLine}");
        }
        // </ReadContainerProperties>

        /// <summary>
        /// List the container within a database by calling the GetContainerIterator (scan) API.
        /// </summary>
        /// <returns></returns>
        // <ListContainersInDatabase>
        private static async Task ListContainersInDatabase()
        {
            Console.WriteLine($"{Environment.NewLine}5. Reading all CosmosContainer resources for a database");

            using (FeedIterator<ContainerProperties> resultSetIterator = database.GetContainerQueryIterator<ContainerProperties>())
            {
                while (resultSetIterator.HasMoreResults)
                {
                    foreach (ContainerProperties container in await resultSetIterator.ReadNextAsync())
                    {
                        Console.WriteLine(container.Id);
                    }
                }
            }
        }
        // </ListContainersInDatabase>

        /// <summary>
        /// Delete a container
        /// </summary>
        /// <param name="simpleContainer"></param>
        // <DeleteContainer>
        private static async Task DeleteContainer()
        {
            await database.GetContainer(containerId).DeleteContainerAsync();
            Console.WriteLine($"{Environment.NewLine}6. Deleted Container{Environment.NewLine}");
        }
        // </DeleteContainer>
    }

    public class ToDoActivity
    {
        public string id = null;
        public string activityId = null;
        public string status = null;
    }
}