using DocumentProcessor.Core.Enums;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;


namespace DocumentProcessor.Core.Models
{
    public class DocumentModel
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string UserId { get; set; }
        public string Name { get; set; }
        public string FileType { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
        public StatusTypes Status {  get; set; }
        public Dictionary<string, object> ProcessingResults { get; set; }
        public DateTimeOffset TimeStamp { get; set; } = DateTimeOffset.UtcNow;
        
        [JsonProperty("_etag")]
        public string ETag { get; set; }
    }
}
