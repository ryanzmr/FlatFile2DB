//ConfigModels.cs//
using System;
using System.IO;
using Newtonsoft.Json;

namespace CSVDatabaseImporter.Configuration
{
    public class DatabaseConfig
    {
        public string Server { get; set; }
        public string Database { get; set; }
        public bool IntegratedSecurity { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
    }

    public class ProcessConfig
    {
        public string CsvFolderPath { get; set; }
        public string TempTableName { get; set; }
        public string DestinationTableName { get; set; }
        public string ErrorTableName { get; set; }
        public string SuccessLogTableName { get; set; }
        public int BatchSize { get; set; }
    }

    public class AppConfig
    {
        public DatabaseConfig DatabaseConfig { get; set; }
        public ProcessConfig ProcessConfig { get; set; }
    }

    public static class ConfigurationLoader
    {
        public static T LoadConfiguration<T>(string configPath)
        {
            if (!File.Exists(configPath))
            {
                throw new FileNotFoundException($"Configuration file not found: {configPath}");
            }

            string jsonContent = File.ReadAllText(configPath);
            T result = JsonConvert.DeserializeObject<T>(jsonContent);
            return result != null ? result : throw new Exception($"Failed to load configuration from {configPath}");
        }
    }
}
