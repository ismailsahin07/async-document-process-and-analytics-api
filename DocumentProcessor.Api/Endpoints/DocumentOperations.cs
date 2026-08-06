using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using DocumentProcessor.Core.Enums;
using DocumentProcessor.Core.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Security.Claims;
using System.Text.Json;

namespace DocumentProcessor.Api.Endpoints;

public class DocumentOperations
{
    private readonly ILogger<DocumentOperations> _logger;
    private readonly Container _container;
    private readonly BlobContainerClient _blobContainerClient;
    private readonly BlobServiceClient _blobServiceClient;

    public DocumentOperations(
        ILogger<DocumentOperations> logger,
        [FromKeyedServices(CosmosContainerKey.Documents)] Container container,
        [FromKeyedServices(BlobContainerKey.docs)] BlobContainerClient blobContainerClient,
        BlobServiceClient blobServiceClient)
    {
        _logger = logger;
        _container = container;
        _blobContainerClient = blobContainerClient;
        _blobServiceClient = blobServiceClient;
    }

    [Function(nameof(PostDocument))]
    public async Task<IActionResult> PostDocument([HttpTrigger(AuthorizationLevel.Anonymous, "post", "documents")] HttpRequest req)
    {
        _logger.LogInformation("Processing a CosmosDB document saving request...");
        
        var user = req.HttpContext.User;
        string? userId = user.FindFirstValue(ClaimTypes.NameIdentifier) ?? user.FindFirstValue("oid");

        if (string.IsNullOrEmpty(userId))
            return new UnauthorizedResult();

        using JsonDocument doc = await JsonDocument.ParseAsync(req.Body);
        var root = doc.RootElement;

        if (!root.TryGetProperty("documentName", out JsonElement nameElement) || 
            !root.TryGetProperty("fileType", out JsonElement fileTypeElement))
            return new BadRequestObjectResult("One or more properties are missing from the incoming JSON");

        var document = new DocumentModel
        {
            Id = $"{userId}-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}",
            UserId = userId,
            Name = nameElement.GetString(),
            TimeStamp = DateTimeOffset.UtcNow,
            FileType = fileTypeElement.GetString(),
            Status = StatusTypes.UploadPending
        };

        try
        {
            var options = new BlobGetUserDelegationKeyOptions(DateTimeOffset.UtcNow.AddMinutes(5))
            { StartsOn =  DateTimeOffset.UtcNow.AddMinutes(-2) };
            var key = await _blobServiceClient.GetUserDelegationKeyAsync(options);

            var permissions = BlobSasPermissions.Create | BlobSasPermissions.Write;
            var expireTime = DateTimeOffset.UtcNow.AddMinutes(5);

            BlobClient blobClient = _blobContainerClient.GetBlobClient(document.Id);
            Uri sasUri = blobClient.GenerateUserDelegationSasUri(permissions, expireTime, key);

            _logger.LogInformation($"Successfully generated a blob SAS Uri for upload.");

            ItemResponse<DocumentModel> response = await _container.CreateItemAsync(
                item: document,
                partitionKey: new PartitionKey(userId));
            
            _logger.LogInformation($"Document has successfully saved to the database. Request Charge: {response.RequestCharge} RUs.");
            return new AcceptedResult("/api/documents", new 
            { 
                DocumentId = document.Id , 
                UploadUrl = sasUri.ToString()
            });
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
            using FeedIterator<DocumentModel> feedIterator = _container.GetItemQueryIterator<DocumentModel>(
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

    [Function(nameof(GetDocumentById))]
    public async Task<IActionResult> GetDocumentById(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "documents/{id}")] HttpRequest req, string id)
    {
        _logger.LogInformation($"Processing a document point-read retrieving request...");

        var user = req.HttpContext.User;
        string? userId = user.FindFirstValue(ClaimTypes.NameIdentifier) ?? user.FindFirstValue("oid");

        if (userId is null)
            return new UnauthorizedResult();

        try
        {
            ItemResponse<DocumentModel> response = await _container.ReadItemAsync<DocumentModel>(
                id: id,
                partitionKey: new PartitionKey(userId));

            return new OkObjectResult(response.Resource);
        }
        catch(CosmosException ex) when (ex.StatusCode is HttpStatusCode.NotFound)
        {
            _logger.LogError($"Particular document hasn't found in the database: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status404NotFound);
        }
        catch(Exception ex)
        {
            _logger.LogError($"Document point-read tracking failure encountered: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
    }

    [Function(nameof(DownloadDocument))]
    public async Task<IActionResult> DownloadDocument(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "documents/{id}/download")] HttpRequest req, string id)
    {
        _logger.LogInformation("Processing a document download request...");

        var user = req.HttpContext.User;
        string? userId = user.FindFirstValue(ClaimTypes.NameIdentifier) ?? user.FindFirstValue("oid");

        if(userId is null)
            return new UnauthorizedResult();

        try
        {
            BlobClient blobClient = _blobContainerClient.GetBlobClient(id);

            var options = new BlobGetUserDelegationKeyOptions(DateTimeOffset.UtcNow.AddMinutes(5)) 
            { StartsOn = DateTimeOffset.UtcNow.AddMinutes(-2) };
            UserDelegationKey key =  await _blobServiceClient.GetUserDelegationKeyAsync(options);

            var permission = BlobSasPermissions.Read;
            var expirationTime = DateTimeOffset.UtcNow.AddMinutes(5);
            Uri sasUri = blobClient.GenerateUserDelegationSasUri(permission, expirationTime, key);

            _logger.LogInformation($"Successfully generated the SAS URI for blob: {blobClient.Name}");
            return new OkObjectResult(new { DownloadUri = sasUri.ToString() });
        }
        catch(Exception ex)
        {
            _logger.LogError($"Cryptographic SAS generation pipeline failure: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
    }

    [Function(nameof(DeleteDocument))]
    public async Task<IActionResult> DeleteDocument(
        [HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "documents/{id}")] HttpRequest req, string id)
    {
        _logger.LogInformation("Processing a document record deletion request...");

        var user = req.HttpContext.User;
        string? userId = user.FindFirstValue(ClaimTypes.NameIdentifier) ?? user.FindFirstValue("oid");

        if (string.IsNullOrEmpty(userId))
            return new UnauthorizedResult();

        string? eTag = req.Headers["If-Match"];

        if (string.IsNullOrEmpty(eTag))
            return new BadRequestObjectResult("'If-Match' property is missing from the HTTP headers.");

        try
        {
            BlobClient blobClient = _blobContainerClient.GetBlobClient(id);
            var storageResponse = await blobClient.DeleteIfExistsAsync();

            if(storageResponse.Value is false)
                _logger.LogWarning($"Blob {id} does not exist in the storage. Proceeding the database cleanup.");

            ItemRequestOptions options = new()
            {
                IfMatchEtag = eTag
            };

            ItemResponse<DocumentModel> databaseResponse = await _container.DeleteItemAsync<DocumentModel>(
                id: id,
                partitionKey: new PartitionKey(userId),
                options);

            _logger.LogInformation($"Record for the document with ID: {id} has been successfully deleted.");
            return new NoContentResult();
        }
        catch(CosmosException ex) when (ex.StatusCode is HttpStatusCode.NotFound)
        {
            _logger.LogError($"Particular document hasn't found in the database: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status404NotFound);
        }
        catch(CosmosException ex) when (ex.StatusCode is HttpStatusCode.PreconditionFailed)
        {
            _logger.LogError($"Precondition failed. ETag mismatch: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status412PreconditionFailed);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Document record deletion tracking failure encountered: {ex.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        } 
    }
}