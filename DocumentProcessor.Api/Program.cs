using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Storage.Blobs;
using DocumentProcessor.Core.Enums;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Identity.Web;

var builder = FunctionsApplication.CreateBuilder(args);

builder.Services.AddSingleton<BlobServiceClient>(sp =>
{
    string blobEndpoint = Environment.GetEnvironmentVariable("StorageAccount__blobServiceUri")
        ?? throw new InvalidOperationException("StorageAccountUri is not configured.");

    return new BlobServiceClient(serviceUri: new Uri(blobEndpoint), new DefaultAzureCredential());
});

builder.Services.AddKeyedSingleton<BlobContainerClient>(BlobContainerKey.docs ,(sp, key) =>
{
    var blobServiceClient = sp.GetRequiredService<BlobServiceClient>();
    return blobServiceClient.GetBlobContainerClient("docs");
});

builder.Services.AddSingleton<CosmosClient>(sp =>
{
    string cosmosEndpoint = Environment.GetEnvironmentVariable("CosmosAccount__accountEndpoint")
        ?? throw new InvalidOperationException("CosmosDbEndpoint is not configured.");

    var options = new CosmosClientOptions
    {
        SerializerOptions = new CosmosSerializationOptions
        {
            PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase
        }
    };

    return new CosmosClient(accountEndpoint: cosmosEndpoint, new DefaultAzureCredential(), options);
});

builder.Services.AddKeyedSingleton<Container>(CosmosContainerKey.Documents, (sp, key) =>
{
    var cosmosClient = sp.GetRequiredService<CosmosClient>();

    string databaseName = "AsyncDocProcessor";
    string containerName = "Documents";

    return cosmosClient.GetContainer(databaseName, containerName);
});

builder.Services.AddSingleton<ServiceBusClient>(sp =>
{
    string sbEndpoint = Environment.GetEnvironmentVariable("ServiceBusAccount__fullyQualifiedNamespace")
        ?? throw new InvalidOperationException("Service Bus endpoint is missing.");

    return new ServiceBusClient(fullyQualifiedNamespace: sbEndpoint, new DefaultAzureCredential());
});

builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddMicrosoftIdentityWebApi(builder.Configuration.GetSection("AzureAd"));

builder.Services.AddAuthorizationBuilder();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.ConfigureFunctionsWebApplication();

if (builder.Environment.IsDevelopment())
{
    builder.ConfigureAspNetCoreMvcIntegration()
    .UseAspNetCoreMiddleware(app =>
    {
        app.UseFunctionSwaggerUI(); 
    });
}

builder.Build().Run();
