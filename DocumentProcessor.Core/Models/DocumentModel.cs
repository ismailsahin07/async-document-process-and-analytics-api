using DocumentProcessor.Core.Enums;
using System.Text.Json.Serialization;


namespace DocumentProcessor.Core.Models
{
    public class DocumentModel
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string UserId { get; set; }
        public string Name { get; set; }
        public string FileType { get; set; }
        public StatusTypes Status {  get; set; }
        public Dictionary<string, object> ProcessingResults { get; set; }
        public DateTimeOffset TimeStamp { get; set; } = DateTimeOffset.UtcNow;
        
        [JsonPropertyName("_etag")]
        public string ETag { get; set; }
    }
}
