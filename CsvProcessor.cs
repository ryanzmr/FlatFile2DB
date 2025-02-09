//CsvProcessor.cs//
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using CSVDatabaseImporter.Models;
using Microsoft.Data.SqlClient;

namespace CSVDatabaseImporter.Services
{
    public class CsvProcessor
    {
        public IEnumerable<BatchResult> LoadCSVInBatches(string csvFilePath, SqlConnection connection, string errorTableName, int batchSize)
        {
            using (StreamReader sr = new StreamReader(csvFilePath))
            {
                string headerLine = sr.ReadLine();
                if (string.IsNullOrEmpty(headerLine))
                {
                    throw new ArgumentException("CSV file is empty.");
                }

                string[] headers = ParseCSVLine(headerLine);
                DataTable currentBatch = CreateDataTable(headers);
                bool isFirstBatch = true;

                while (!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }

                    bool hasError = false;
                    try
                    {
                        string[] values = ParseCSVLine(line);
                        if (values.Length == headers.Length)
                        {
                            ProcessRow(values, currentBatch);
                        }
                        else
                        {
                            // Updated LogError call with new parameters
                            LogError(
                                connection,
                                errorTableName,
                                Path.GetFileName(csvFilePath),     // fileName
                                "Row Structure",                   // columnName
                                "CSV Parsing Error",              // errorType
                                $"Row length mismatch in file {Path.GetFileName(csvFilePath)}" // reason
                            );
                            hasError = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        // Updated LogError call with new parameters
                        LogError(
                            connection,
                            errorTableName,
                            Path.GetFileName(csvFilePath),    // fileName
                            "Data Processing",                // columnName
                            "ProcessError",                   // errorType
                            ex.Message                        // reason
                        );
                        hasError = true;
                    }

                    if (!hasError && currentBatch.Rows.Count >= batchSize)
                    {
                        yield return new BatchResult
                        {
                            Data = currentBatch,
                            IsFirstBatch = isFirstBatch,
                            IsLastBatch = false
                        };
                        currentBatch = CreateDataTable(headers);
                        isFirstBatch = false;
                    }
                }

                if (currentBatch.Rows.Count > 0)
                {
                    yield return new BatchResult
                    {
                        Data = currentBatch,
                        IsFirstBatch = isFirstBatch,
                        IsLastBatch = true
                    };
                }
            }
        }

        private static DataTable CreateDataTable(string[] headers)
        {
            DataTable dt = new DataTable();
            foreach (string header in headers)
            {
                _ = dt.Columns.Add(header.Trim());
            }
            return dt;
        }

        private static void ProcessRow(string[] values, DataTable dt)
        {
            DataRow row = dt.NewRow();
            for (int i = 0; i < values.Length; i++)
            {
                string value = values[i].Trim();

                if (string.IsNullOrWhiteSpace(value))
                {
                    row[i] = DBNull.Value;
                    continue;
                }

                // Try to parse as date
                if (DateTime.TryParseExact(value, new[] { "dd-MM-yyyy", "MM/dd/yyyy", "yyyy-MM-dd" }, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateValue))
                {
                    value = dateValue.ToString("yyyy-MM-dd");
                }
                // Try to parse as number
                else if (decimal.TryParse(value, out decimal numericValue))
                {
                    value = numericValue.ToString(CultureInfo.InvariantCulture);
                }

                row[i] = value;
            }
            dt.Rows.Add(row);
        }

        private static string[] ParseCSVLine(string line)
        {
            List<string> values = new List<string>();
            StringBuilder value = new StringBuilder();
            bool inQuotes = false;

            foreach (char c in line)
            {
                if (c == '\"')
                {
                    inQuotes = !inQuotes;
                }
                else if (c == ',' && !inQuotes)
                {
                    values.Add(value.ToString());
                    _ = value.Clear();
                }
                else
                {
                    _ = value.Append(c);
                }
            }
            values.Add(value.ToString());
            return values.ToArray();
        }

        // Updated LogError method to match the new error logging structure
        private static void LogError(SqlConnection connection, string errorTableName, string fileName, string columnName, string errorType, string reason)
        {
            string insertErrorQuery = $@"
                    INSERT INTO {errorTableName} (FileName, ColumnName, ErrorType, Reason, Timestamp) 
                    VALUES (@FileName, @ColumnName, @ErrorType, @Reason, GETDATE())";

            using (SqlCommand errorCommand = new SqlCommand(insertErrorQuery, connection))
            {
                _ = errorCommand.Parameters.AddWithValue("@FileName", fileName ?? (object)DBNull.Value);
                _ = errorCommand.Parameters.AddWithValue("@ColumnName", columnName ?? (object)DBNull.Value);
                _ = errorCommand.Parameters.AddWithValue("@ErrorType", errorType ?? (object)DBNull.Value);
                _ = errorCommand.Parameters.AddWithValue("@Reason", reason ?? (object)DBNull.Value);
                _ = errorCommand.ExecuteNonQuery();
            }
        }
    }
}