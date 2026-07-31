using Azure.Storage.Blobs;
using DocumentProcessor.Core.Enums;
using DocumentProcessor.Core.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Security.Claims;

namespace DocumentProcessor.Api.Endpoints;

public class DocumentOperations
{
    private readonly ILogger<DocumentOperations> _logger;
    private readonly Container _container;
    private readonly BlobContainerClient _blobContainerClient;

    public DocumentOperations(
        ILogger<DocumentOperations> logger,
        [FromKeyedServices(CosmosContainerKey.Documents)] Container container,
        [FromKeyedServices(BlobContainerKey.docs)] BlobContainerClient blobContainerClient)
    {
        _logger = logger;
        _container = container;
        _blobContainerClient = blobContainerClient;
    }

    [Function(nameof(PostDocument))]
    public async Task<IActionResult> PostDocument([HttpTrigger(AuthorizationLevel.Anonymous, "post", "documents")] HttpRequest req)
    {
        _logger.LogInformation("Processing a document saving request...");
        
        var user = req.HttpContext.User;
        string? userId = user.FindFirstValue(ClaimTypes.NameIdentifier) ?? user.FindFirstValue("oid");

        if (string.IsNullOrEmpty(userId))
            return new UnauthorizedResult();

        if (!req.HasFormContentType)
            return new BadRequestObjectResult("Request must be multipart/form-data");

        var form = await req.ReadFormAsync();
        var file = form.Files.GetFile("documentFile");

        if (file is null || file.Length is 0)
            return new BadRequestObjectResult("File is missing.");

        var document = new DocumentModel
        {
            Id = $"{userId}-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}",
            UserId = userId,
            Name = form.TryGetValue("documentName", out var nameValue) ? nameValue.ToString() : file.FileName,
            TimeStamp = DateTimeOffset.UtcNow,
            FileType = Path.GetExtension(file.ContentType),
            Status = StatusTypes.Pending
        };

        try
        {
            using var stream = file.OpenReadStream();
            string blobName = document.Id;

            BlobClient blobClient = _blobContainerClient.GetBlobClient(blobName);
            await blobClient.UploadAsync(stream);
            _logger.LogInformation($"blob {blobName} is successfully uploaded to {_blobContainerClient.Name} container.");

            ItemResponse<DocumentModel> response = await _container.CreateItemAsync(
                item: document,
                partitionKey: new PartitionKey(userId));
            
            _logger.LogInformation($"Document has successfully saved to the database. Request Charge: {response.RequestCharge} RUs.");
            return new AcceptedResult("/api/documents", new { DocumentId = document.Id });
        }
        catch(CosmosException ex)
        {
            _logger.LogError($"CosmosDB tracking write failure encountered: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
        catch(Exception ex)
        {
            _logger.LogError($"Processing failure encountered: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
    }

    [Function(nameof(GetDocument))]
    public async Task<IActionResult> GetDocument([HttpTrigger(AuthorizationLevel.Anonymous, "get", "documents")] HttpRequest req)
    {
        _logger.LogInformation("Processing a document query request...");

        var user = req.HttpContext.User;
        string? userId = user.FindFirstValue(ClaimTypes.NameIdentifier) ?? user.FindFirstValue("oid");

        if (string.IsNullOrEmpty(userId))
            return new UnauthorizedResult();

        var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.userId = @userId")
            .WithParameter("@userId", userId);

        var queryOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKey(userId),
            MaxItemCount = 100
        };

        var results = new List<DocumentModel>();

        try
        {
            FeedIterator<DocumentModel> feedIterator = _container.GetItemQueryIterator<DocumentModel>(
            queryDefinition: queryDefinition,
            requestOptions: queryOptions);

            while (feedIterator.HasMoreResults)
            {
                FeedResponse<DocumentModel> response = await feedIterator.ReadNextAsync();
                results.AddRange(response);
            }

            return new OkObjectResult(results);
        }
        catch(CosmosException ex)
        {
            _logger.LogError($"CosmosDB query processing failure encountered: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
    }
}