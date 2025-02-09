//DatabaseOperation.cs//
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace CSVDatabaseImporter.Services
{
    public class DatabaseOperations
    {
        private static readonly Stopwatch _stopwatch = new Stopwatch();

        public static void CreateLogTables(SqlConnection connection, string errorTableName, string successLogTableName)
        {
            string createErrorTableQuery = $@"
                IF OBJECT_ID('{errorTableName}', 'U') IS NULL
                CREATE TABLE {errorTableName} (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    FileName NVARCHAR(MAX),
                    ColumnName NVARCHAR(MAX),
                    ErrorType NVARCHAR(100),
                    Reason NVARCHAR(MAX),
                    Timestamp DATETIME DEFAULT GETDATE()
                )";

            string createSuccessTableQuery = $@"
                IF OBJECT_ID('{successLogTableName}', 'U') IS NULL
                CREATE TABLE {successLogTableName} (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    Message NVARCHAR(MAX),
                    Timestamp DATETIME DEFAULT GETDATE()
                )";

            using (SqlCommand cmd = new SqlCommand(createErrorTableQuery, connection))
            {
                _ = cmd.ExecuteNonQuery();
            }

            using (SqlCommand successCmd = new SqlCommand(createSuccessTableQuery, connection))
            {
                _ = successCmd.ExecuteNonQuery();
            }
        }

        public static void DropAndCreateTempTable(SqlConnection connection, string tempTableName, DataTable dataTable)
        {
            string dropTableQuery = $"IF OBJECT_ID('{tempTableName}', 'U') IS NOT NULL DROP TABLE {tempTableName}";
            using (SqlCommand dropCommand = new SqlCommand(dropTableQuery, connection))
            {
                _ = dropCommand.ExecuteNonQuery();
            }

            StringBuilder createTableQuery = new StringBuilder($"CREATE TABLE {tempTableName} (");
            foreach (DataColumn column in dataTable.Columns)
            {
                _ = createTableQuery.Append($"[{column.ColumnName}] NVARCHAR(MAX),");
            }
            createTableQuery.Length--; // Remove last comma
            _ = createTableQuery.Append(')');

            using (SqlCommand createCommand = new SqlCommand(createTableQuery.ToString(), connection))
            {
                _ = createCommand.ExecuteNonQuery();
            }
        }

        public async Task LoadDataIntoTempTableAsync(SqlConnection connection, string tempTableName, DataTable dataTable)
        {
            _stopwatch.Restart();
            Console.WriteLine($"Starting bulk load into temp table. Rows to process: {dataTable.Rows.Count}");

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.UseInternalTransaction, null))
            {
                bulkCopy.DestinationTableName = tempTableName;
                bulkCopy.BatchSize = 100000;
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.EnableStreaming = true;
                //bulkCopy.NotifyAfter = 50000;//commented out to avoid the error

                bulkCopy.SqlRowsCopied += (sender, e) =>
                    Console.WriteLine($"Copied {e.RowsCopied:N0} rows to temp table...");

                foreach (DataColumn column in dataTable.Columns)
                {
                    _ = bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(dataTable);
                Console.WriteLine($"Temp table load completed in {_stopwatch.Elapsed.TotalSeconds:N2} seconds");
            }

            dataTable.Dispose();
        }

        public async Task TransferToDestinationAsync(SqlConnection connection, string tempTableName,
            string destinationTableName, string errorTableName, string successLogTableName,
            bool isFirstFile, int batchSize, string csvFileName)
        {
            Console.WriteLine("Starting batch transfer to destination...");
            _stopwatch.Restart();

            using (SqlTransaction transaction = connection.BeginTransaction())
            {
                try
                {
                    // Set XACT_ABORT ON for the transaction
                    using (SqlCommand setXactAbortOn = new SqlCommand("SET XACT_ABORT ON;", connection, transaction))
                    {
                        _ = setXactAbortOn.ExecuteNonQuery();
                    }

                    // Get initial count in destination table
                    long initialDestinationCount = 0;
                    using (SqlCommand initialCountCmd = new SqlCommand(
                        $"SELECT CAST(COUNT_BIG(*) AS BIGINT) FROM {destinationTableName} WITH (NOLOCK)",
                        connection, transaction))
                    {
                        initialDestinationCount = Convert.ToInt64(initialCountCmd.ExecuteScalar());
                    }

                    // Rest of the existing validation code...
                    if (isFirstFile)
                    {
                        using (SqlCommand truncateCommand = new SqlCommand($"TRUNCATE TABLE {destinationTableName}", connection, transaction))
                        {
                            _ = truncateCommand.ExecuteNonQuery();
                            initialDestinationCount = 0; // Reset count after truncate
                        }
                    }

                    // Column validation and mapping code remains the same...
                    Dictionary<string, string> columnMapping = GetColumnMapping(connection, tempTableName, destinationTableName, transaction);
                    if (columnMapping.Count == 0)
                    {
                        throw new Exception("No matching columns found between temp and destination tables.");
                    }

                    // Get temp table count
                    long tempTableCount = 0;
                    using (SqlCommand tempCountCmd = new SqlCommand(
                        $"SELECT CAST(COUNT_BIG(*) AS BIGINT) FROM {tempTableName} WITH (NOLOCK)",
                        connection, transaction))
                    {
                        tempTableCount = Convert.ToInt64(tempCountCmd.ExecuteScalar());
                    }

                    // Perform the bulk copy operation
                    string selectQuery = $"SELECT {string.Join(", ", columnMapping.Keys)} FROM {tempTableName}";
                    using (SqlCommand selectCommand = new SqlCommand(selectQuery, connection, transaction))
                    using (SqlDataReader reader = selectCommand.ExecuteReader())
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                        {
                            bulkCopy.DestinationTableName = destinationTableName;
                            bulkCopy.BatchSize = batchSize;
                            bulkCopy.BulkCopyTimeout = 0;
                            bulkCopy.EnableStreaming = true;
                            bulkCopy.NotifyAfter = 50000;

                            foreach (KeyValuePair<string, string> mapping in columnMapping)
                            {
                                _ = bulkCopy.ColumnMappings.Add(mapping.Key, mapping.Value);
                            }

                            bulkCopy.SqlRowsCopied += (sender, e) =>
                            {
                                Console.WriteLine($"Transferred {e.RowsCopied:N0} rows to destination...");
                            };

                            await bulkCopy.WriteToServerAsync(reader);
                        }
                    }

                    // Get final count in destination table
                    long finalDestinationCount = 0;
                    using (SqlCommand finalCountCmd = new SqlCommand(
                        $"SELECT CAST(COUNT_BIG(*) AS BIGINT) FROM {destinationTableName} WITH (NOLOCK)",
                        connection, transaction))
                    {
                        finalDestinationCount = Convert.ToInt64(finalCountCmd.ExecuteScalar());
                    }

                    // Calculate actual rows transferred
                    long actualRowsTransferred = finalDestinationCount - initialDestinationCount;

                    // Verify the transfer
                    if (actualRowsTransferred != tempTableCount)
                    {
                        throw new Exception(
                            $"Data transfer mismatch! Temp table had {tempTableCount:N0} rows, " +
                            $"but only {actualRowsTransferred:N0} rows were transferred to destination.");
                    }

                    TimeSpan elapsed = _stopwatch.Elapsed;
                    double rowsPerSecond = actualRowsTransferred / elapsed.TotalSeconds;

                    transaction.Commit();
                    Console.WriteLine($"Transfer completed in {elapsed.TotalSeconds:N2} seconds. Rate: {rowsPerSecond:N0} rows/sec");

                    // Log success with verified row count
                    LogSuccess(connection, successLogTableName,
                        $"Successfully transferred data from {csvFileName}",
                        actualRowsTransferred, columnMapping.Count, null);
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    LogError(connection, errorTableName, csvFileName, "Process", "ProcessError",
                        ex.Message, null);
                    Console.WriteLine($"Error: {ex.Message}");
                    throw;
                }
            }
        }
        private static List<string> GetTableColumns(SqlConnection connection, string tableName, SqlTransaction transaction)
        {
            List<string> columns = new List<string>();
            string query = @"
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = @TableName 
                ORDER BY ORDINAL_POSITION";

            using (SqlCommand cmd = new SqlCommand(query, connection, transaction))
            {
                _ = cmd.Parameters.AddWithValue("@TableName", tableName);
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columns.Add(reader["COLUMN_NAME"].ToString());
                    }
                }
            }
            return columns;
        }

        private static Dictionary<string, string> GetColumnMapping(SqlConnection connection, string sourceTable, string destTable, SqlTransaction transaction)
        {
            Dictionary<string, string> mapping = new Dictionary<string, string>();
            string query = @"
                SELECT s.COLUMN_NAME as SourceColumn
                FROM INFORMATION_SCHEMA.COLUMNS s
                INNER JOIN INFORMATION_SCHEMA.COLUMNS d 
                    ON s.COLUMN_NAME = d.COLUMN_NAME
                WHERE s.TABLE_NAME = @SourceTable 
                    AND d.TABLE_NAME = @DestTable";

            using (SqlCommand cmd = new SqlCommand(query, connection, transaction))
            {
                _ = cmd.Parameters.AddWithValue("@SourceTable", sourceTable);
                _ = cmd.Parameters.AddWithValue("@DestTable", destTable);
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader["SourceColumn"].ToString();
                        mapping.Add(columnName, columnName);
                    }
                }
            }
            return mapping;
        }

        public static void LogSuccess(SqlConnection connection, string successLogTableName, string message, long totalRows, int totalColumns, SqlTransaction transaction = null)
        {
            EnsureConnectionOpen(connection);
            TimeSpan elapsed = _stopwatch.Elapsed;
            double rowsPerSecond = totalRows / elapsed.TotalSeconds;

            string fullMessage = $"{message} - Rows: {totalRows:N0}, Columns: {totalColumns}, " +
                                 $"Time: {elapsed.TotalSeconds:N2} s, Rate: {rowsPerSecond:N0} rows/sec";

            string insertLogQuery = $@"
                   INSERT INTO {successLogTableName} (Message, Timestamp) 
                       VALUES (@Message, GETDATE())";

            using (SqlCommand logCommand = new SqlCommand(insertLogQuery, connection, transaction))
            {
                _ = logCommand.Parameters.AddWithValue("@Message", fullMessage);
                _ = logCommand.ExecuteNonQuery();
            }
        }

        public static void LogError(SqlConnection connection, string errorTableName,
            string fileName, string columnName, string errorType, string reason,
            SqlTransaction transaction = null)
        {
            EnsureConnectionOpen(connection);

            string insertErrorQuery = $@"
                INSERT INTO {errorTableName} (FileName, ColumnName, ErrorType, Reason, Timestamp) 
                VALUES (@FileName, @ColumnName, @ErrorType, @Reason, GETDATE())";

            using (SqlCommand errorCommand = new SqlCommand(insertErrorQuery, connection, transaction))
            {
                _ = errorCommand.Parameters.AddWithValue("@FileName", fileName ?? (object)DBNull.Value);
                _ = errorCommand.Parameters.AddWithValue("@ColumnName", columnName ?? (object)DBNull.Value);
                _ = errorCommand.Parameters.AddWithValue("@ErrorType", errorType ?? (object)DBNull.Value);
                _ = errorCommand.Parameters.AddWithValue("@Reason", reason ?? (object)DBNull.Value);
                _ = errorCommand.ExecuteNonQuery();
            }
        }

        private static void EnsureConnectionOpen(SqlConnection connection)
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
        }
    }
}