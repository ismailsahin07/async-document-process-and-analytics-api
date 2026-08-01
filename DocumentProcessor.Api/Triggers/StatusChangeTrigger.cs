using Azure.Messaging.EventGrid;
using DocumentProcessor.Core.Enums;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DocumentProcessor.Api.Triggers;

public class StatusChangeTrigger
{
    private readonly ILogger<StatusChangeTrigger> _logger;
    private readonly Container _container;

    public StatusChangeTrigger(
        ILogger<StatusChangeTrigger> logger,
        CosmosClient cosmosClient,
        [FromKeyedServices(CosmosContainerKey.Documents)] Container container)
    {
        _logger = logger;
        _container = container;
    }

    [Function(nameof(StatusChangeTrigger))]
    public void Run([EventGridTrigger] EventGridEvent egEvent)
    {
        
    }
}