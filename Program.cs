//Program.cs//
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CSVDatabaseImporter.Configuration;
using CSVDatabaseImporter.Services;
using Microsoft.Data.SqlClient;

namespace CSVDatabaseImporter
{
    internal static class Program
    {
        private static readonly CsvProcessor csvProcessor = new CsvProcessor();
        private static readonly DatabaseOperations dbOps = new DatabaseOperations();

        private static async Task Main()
        {
            try
            {
                // Get the directory where executable is running
                string exePath = AppDomain.CurrentDomain.BaseDirectory;

                // First try to find config.json in the same directory as executable
                string configPath = Path.Combine(exePath, "config.json");

                // If not found in root, check in Configuration folder
                if (!File.Exists(configPath))
                {
                    configPath = Path.Combine(exePath, "Configuration", "config.json");
                }

                if (!File.Exists(configPath))
                {
                    throw new FileNotFoundException("Configuration file not found. Please ensure config.json exists in the application directory or Configuration folder. Tried paths:\n" +
                        $"1. {Path.Combine(exePath, "config.json")}\n" +
                        $"2. {Path.Combine(exePath, "Configuration", "config.json")}");
                }

                Console.WriteLine($"Using configuration file: {configPath}");
                Console.WriteLine("Loading configuration...");
                AppConfig appConfig = ConfigurationLoader.LoadConfiguration<AppConfig>(configPath);
                DatabaseConfig dbConfig = appConfig.DatabaseConfig;
                ProcessConfig processConfig = appConfig.ProcessConfig;

                // Print loaded configuration for verification
                Console.WriteLine($"Server: {dbConfig.Server}");
                Console.WriteLine($"Database: {dbConfig.Database}");
                Console.WriteLine($"Windows Auth: {dbConfig.IntegratedSecurity}");
                Console.WriteLine($"CSV Folder: {processConfig.CsvFolderPath}");

                // Build the connection string
                string connectionString = BuildConnectionString(dbConfig);
                Console.WriteLine($"Connection String: {connectionString}");

                // Test the database connection
                TestDatabaseConnection(connectionString);

                // Ensure the CSV folder exists
                if (!Directory.Exists(processConfig.CsvFolderPath))
                {
                    _ = Directory.CreateDirectory(processConfig.CsvFolderPath);
                    Console.WriteLine($"Created CSV folder: {processConfig.CsvFolderPath}");
                }

                // Process the CSV files
                await ProcessFiles(connectionString, processConfig);

                Console.WriteLine("Process completed successfully!");
            }
            catch (SqlException sqlEx)
            {
                Console.WriteLine("\nSQL Server Error:");
                Console.WriteLine($"Error Number: {sqlEx.Number}");
                Console.WriteLine($"Error Message: {sqlEx.Message}");
                Console.WriteLine($"Error State: {sqlEx.State}");
                Console.WriteLine($"Server: {sqlEx.Server}");
            }
            catch (Exception ex)
            {
                Console.WriteLine("\nGeneral Error:");
                Console.WriteLine($"Error Type: {ex.GetType().Name}");
                Console.WriteLine($"Message: {ex.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
            }
            finally
            {
                Console.WriteLine("\nPress any key to exit...");
                _ = Console.ReadKey();
            }
        }

        private static string BuildConnectionString(DatabaseConfig config)
        {
            SqlConnectionStringBuilder sqlConnectionStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = config.Server,
                InitialCatalog = config.Database,
                IntegratedSecurity = config.IntegratedSecurity,
                TrustServerCertificate = true,
                ConnectTimeout = 30,
                MultipleActiveResultSets = true,
                MaxPoolSize = 100
            };
            SqlConnectionStringBuilder builder = sqlConnectionStringBuilder;

            if (!config.IntegratedSecurity)
            {
                builder.UserID = config.Username;
                builder.Password = config.Password;
            }

            return builder.ConnectionString;
        }

        private static void TestDatabaseConnection(string connectionString)
        {
            Console.WriteLine("\nTesting database connection...");
            using (SqlConnection testConn = new SqlConnection(connectionString))
            {
                try
                {
                    testConn.Open();
                    Console.WriteLine("Database connection successful!");

                    using (SqlCommand cmd = new SqlCommand("SELECT @@VERSION", testConn))
                    {
                        string version = cmd.ExecuteScalar()?.ToString();
                        Console.WriteLine($"SQL Server Version: {version}");
                    }
                }
                catch (SqlException)
                {
                    Console.WriteLine("Failed to connect to database!");
                    throw;
                }
            }
        }

        private static async Task ProcessFiles(string connectionString, ProcessConfig config)
        {
            if (!Directory.Exists(config.CsvFolderPath))
            {
                throw new DirectoryNotFoundException($"CSV folder not found: {config.CsvFolderPath}");
            }

            string[] csvFiles = Directory.GetFiles(config.CsvFolderPath, "*.csv").OrderBy(f => f).ToArray();

            Console.WriteLine($"\nFound {csvFiles.Length} CSV files to process.");

            if (!csvFiles.Any())
            {
                throw new Exception("No CSV files found in the specified folder.");
            }

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                DatabaseOperations.CreateLogTables(connection, config.ErrorTableName, config.SuccessLogTableName);

                for (int fileIndex = 0; fileIndex < csvFiles.Length; fileIndex++)
                {
                    string csvFile = csvFiles[fileIndex];
                    bool isFirstFile = fileIndex == 0;

                    Console.WriteLine($"\nProcessing file {fileIndex + 1} of {csvFiles.Length}: {Path.GetFileName(csvFile)}");

                    try
                    {
                        int totalRows = 0;
                        int columnCount = 0;

                        // Add stopwatch here, before the batch processing begins
                        Stopwatch stopwatch = new Stopwatch();

                        foreach (Models.BatchResult dataBatch in csvProcessor.LoadCSVInBatches(csvFile, connection, config.ErrorTableName, config.BatchSize))
                        {
                            if (dataBatch.IsFirstBatch)
                            {
                                columnCount = dataBatch.Data.Columns.Count;
                                DatabaseOperations.DropAndCreateTempTable(connection, config.TempTableName, dataBatch.Data);
                            }

                            // Start timing CSV to Temp transfer
                            stopwatch.Restart(); // Reset and start timing
                            Console.WriteLine($"Starting CSV to Temp transfer at: {DateTime.Now}");

                            await dbOps.LoadDataIntoTempTableAsync(connection, config.TempTableName, dataBatch.Data);

                            Console.WriteLine($"CSV to Temp completed. Time taken: {stopwatch.ElapsedMilliseconds}ms");
                            totalRows += dataBatch.Data.Rows.Count;
                            Console.WriteLine($"Processed {totalRows} rows...");

                            if (dataBatch.IsLastBatch)
                            {
                                // Start timing Temp to Destination transfer
                                stopwatch.Restart();
                                Console.WriteLine($"Starting Temp to Destination transfer at: {DateTime.Now}");

                                await dbOps.TransferToDestinationAsync(connection, config.TempTableName,
                                    config.DestinationTableName, config.ErrorTableName,
                                    config.SuccessLogTableName, isFirstFile, config.BatchSize,
                                    Path.GetFileName(csvFile));

                                Console.WriteLine($"Temp to Destination completed. Time taken: {stopwatch.ElapsedMilliseconds}ms");
                            }

                            dataBatch.Data.Dispose();
                        }

                        DatabaseOperations.LogSuccess(connection, config.SuccessLogTableName,
                         $"Successfully processed file: {Path.GetFileName(csvFile)}", totalRows, columnCount);
                        Console.WriteLine($"Completed file: {Path.GetFileName(csvFile)} - Total Rows: {totalRows}, Total Columns: {columnCount}");
                    }
                    catch (Exception ex)
                    {
                        DatabaseOperations.LogError(
                            connection,
                            config.ErrorTableName,
                            Path.GetFileName(csvFile),
                            "File Processing",
                            "ProcessError",
                            ex.Message,
                            null
                        );
                        Console.WriteLine($"Error processing file: {ex.Message}");
                    }
                }
            }
        }
    }
}