//BatchResult.cs//
using System.Data;
namespace CSVDatabaseImporter.Models
{
    public class BatchResult
    {
        public DataTable Data { get; set; }
        public bool IsFirstBatch { get; set; }
        public bool IsLastBatch { get; set; }
    }
}
