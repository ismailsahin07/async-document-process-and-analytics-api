using Azure.Messaging.EventGrid;
using Azure.Messaging.EventGrid.SystemEvents;
using Azure.Storage.Blobs;
using DocumentProcessor.Core.Enums;
using DocumentProcessor.Core.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Text.Json;

namespace DocumentProcessor.Api.Triggers;

public class StatusChangeTrigger
{
    private readonly ILogger<StatusChangeTrigger> _logger;
    private readonly Container _container;
    private readonly BlobContainerClient _blobContainerClient;

    public StatusChangeTrigger(
        ILogger<StatusChangeTrigger> logger,
        [FromKeyedServices(CosmosContainerKey.Documents)] Container container,
        [FromKeyedServices(BlobContainerKey.docs)] BlobContainerClient blobContainerClient)
    {
        _logger = logger;
        _container = container;
        _blobContainerClient = blobContainerClient;
    }

    [Function(nameof(StatusChangeTrigger))]
    public async Task Run([EventGridTrigger] EventGridEvent egEvent)
    {
        _logger.LogInformation($"Event Grid interceppted for event: {egEvent.EventType}");

        if(egEvent.EventType is not "Microsoft.Storage.BlobCreated")
        {
            _logger.LogError($"Incorrect event type detected. Skipping execution...");
            return;
        }

        var eventData = egEvent.Data.ToObjectFromJson<StorageBlobCreatedEventData>(new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        if (string.IsNullOrEmpty(eventData?.Url))
        {
            _logger.LogError("Value of'url' property metadata is missing.");
            return;
        }

        _logger.LogInformation($"New blob detected at url: {eventData.Url}");

        string blobName = GetBlobNameFromUrl(eventData.Url, _blobContainerClient.Name);
        _logger.LogInformation($"Extracted blob: {blobName}");

        int lastDashIndex = blobName.LastIndexOf('-');

        if (lastDashIndex is -1)
        {
            _logger.LogError("Blob name isn't in the expected format.");
            return;
        }

        string userId = blobName.Substring(0, lastDashIndex);

        try
        {
            List<PatchOperation> patchOperations = new()
            {
                PatchOperation.Replace("/status", StatusTypes.Processing)
            };

            PatchItemRequestOptions requestOptions = new()
            {
                FilterPredicate = "FROM c WHERE c.status = 'UploadPending'"
            };

            ItemResponse<DocumentModel> statusResponse = await _container.PatchItemAsync<DocumentModel>(
                id: blobName,
                partitionKey: new PartitionKey(userId),
                patchOperations,
                requestOptions);

            _logger.LogInformation($"Document {blobName} is successfully patched. Request Charge: {statusResponse.RequestCharge} RUs.");
        }
        catch(CosmosException ex) when (ex.StatusCode is HttpStatusCode.PreconditionFailed)
        {
            _logger.LogError($"Blob: {blobName} wasn't in the 'UploadPending' state.");
            return;
        }
        catch(CosmosException ex)
        {
            _logger.LogError($"CosmosDB patch operation failed: {ex.Message}");
            return;
        }
    }

    private string GetBlobNameFromUrl(string url, string containerName)
    {
        string lookFor = $"/{containerName}/";
        int index = url.IndexOf(lookFor, StringComparison.OrdinalIgnoreCase);
        if (index is -1) return null;

        return url.Substring(index + lookFor.Length);
    }
}