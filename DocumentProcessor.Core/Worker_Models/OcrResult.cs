namespace DocumentProcessor.Core.Worker_Models;

public class OcrResult
{
    public int PageCount { get; set; }
    public double ConfidenceScore { get; set; }
    public List<string> ExtractedKeywords { get; set; } = new();
}
