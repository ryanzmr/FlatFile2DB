# CSV to Database Importer

## Overview

This project is designed to import large CSV files into a SQL Server database with error handling, batch processing, and logging capabilities to ensure smooth data transfer. This README will guide you through the requirements, setup, and execution process in detail.

## Table of Contents

1. [Requirements](#requirements)
    - [Software](#software)
    - [NuGet Packages](#nuget-packages)
2. [Configuration](#configuration)
    - [Configuration File](#configuration-file-configjson)
3. [Setup Instructions](#setup-instructions)
    - [Clone the Repository](#1-clone-the-repository)
    - [Install .NET SDK](#2-install-net-sdk)
    - [Open the Project](#3-open-the-project)
    - [Install NuGet Packages](#4-install-nuget-packages)
    - [Update Configuration](#5-update-configuration)
4. [Execution Instructions](#execution-instructions)
    - [Running the Project](#running-the-project)
    - [Expected Output](#expected-output)
    - [Log Tables](#log-tables)
5. [Detailed Process Steps](#detailed-process-steps)
    - [Loading Configuration](#loading-configuration)
    - [Testing Database Connection](#testing-database-connection)
    - [Ensuring CSV Folder Exists](#ensuring-csv-folder-exists)
    - [Processing CSV Files](#processing-csv-files)
    - [Error Handling](#error-handling)
    - [Example Output Logs](#example-output-logs)
6. [Code Explanation](#code-explanation)
    - [Program.cs](#programcs)
    - [DatabaseOperations.cs](#databaseoperationscs)
    - [CsvProcessor.cs](#csvprocessorcs)
    - [Models](#models)
7. [Permissions](#permissions)
    - [Database Permissions](#database-permissions)
    - [File System Permissions](#file-system-permissions)
8. [Troubleshooting](#troubleshooting)
    - [Common Errors and Solutions](#common-errors-and-solutions)
9. [Contribution](#contribution)
10. [License](#license)

## Requirements

### Software
- **Microsoft SQL Server** (version 2019 or later)
- **.NET SDK** (version 6.0 or later)
- **Visual Studio 2019/2022** or **Visual Studio Code**

### NuGet Packages
- **Microsoft.Data.SqlClient** (latest version)
- **Newtonsoft.Json** (latest version)

## Configuration

### Configuration File (`config.json`)

Ensure you have a `config.json` file in the root directory of your project with the following structure:

```json
{
  "DatabaseConfig": {
    "Server": "YourServerName",
    "Database": "YourDatabaseName",
    "IntegratedSecurity": true,
    "Username": "YourUsername",  // Uncomment if IntegratedSecurity is false
    "Password": "YourPassword"   // Uncomment if IntegratedSecurity is false
  },
  "ProcessConfig": {
    "CsvFolderPath": "D:\\PathToYourCSVFiles",
    "TempTableName": "TempTable",
    "DestinationTableName": "DestinationTable",
    "ErrorTableName": "ErrorLog",
    "SuccessLogTableName": "SuccessLog",
    "BatchSize": 1000
  }
}
```

## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/CSVDatabaseImporter.git
cd CSVDatabaseImporter
```

### 2. Install .NET SDK
Download and install the .NET SDK from the [official website](https://dotnet.microsoft.com/download/dotnet/6.0).

### 3. Open the Project
- **Visual Studio**:
  - Open the solution file (`.sln`) in Visual Studio.
- **Visual Studio Code**:
  - Open the project folder in Visual Studio Code.

### 4. Install NuGet Packages
```bash
dotnet add package Microsoft.Data.SqlClient
dotnet add package Newtonsoft.Json
```

### 5. Update Configuration
Update the `config.json` file with your database server details and CSV folder path.

## Execution Instructions

### Running the Project
- **Visual Studio**:
  - Press `F5` to run the project.
- **Visual Studio Code**:
  - Open the terminal and run:
    ```bash
    dotnet build
    dotnet run
    ```

### Expected Output
1. The application will load the configuration from `config.json`.
2. It will test the database connection and print the SQL Server version.
3. It will ensure the CSV folder exists.
4. It will process each CSV file in the specified folder in batches.
5. It will log success messages, including the number of rows and columns transferred to the destination table.

### Log Tables
The following tables will be created in the database for logging purposes:
- **ErrorLog**: Stores information about any errors encountered during processing.
- **SuccessLog**: Stores information about successfully processed files and transferred rows and columns.

### Detailed Process Steps

### Loading Configuration
- Load the configuration from `config.json` to get database and processing details.

### Testing Database Connection
- Test the database connection to ensure it's established successfully.

### Ensuring CSV Folder Exists
- Check if the CSV folder exists. If not, create it.

### Processing CSV Files
- Process each CSV file in the specified folder.
- Read CSV data in batches.
- Load data into a temporary table in the database.
- Transfer data from the temporary table to the destination table.
- Log success and error information.

### Error Handling
- Log any errors encountered during the CSV file processing and data transfer.

### Example Output Logs
```
Found 3 CSV files to process.

Processing file 1 of 3: IMP-MAR23-01-10.csv
Processed 100000 rows...
Processed 200000 rows...
...
Completed file: IMP-MAR23-01-10.csv - Total Rows: 800000, Total Columns: 39

Processing file 2 of 3: IMP-MAR23-11-20.csv
Processed 100000 rows...
Processed 200000 rows...
...
Error: Must declare the scalar variable "@IDTR_A_INV".
Error processing file: Error transferring data to destination: Must declare the scalar variable "@IDTR_A_INV".

Processing file 3 of 3: IMP-MAR23-11-30.csv
Processed 100 rows...
Error: Must declare the scalar variable "@IDTR_A_INV".
Error processing file: Error transferring data to destination: Must declare the scalar variable "@IDTR_A_INV".

Process completed successfully!
```

### Code Explanation

### Program.cs
**Purpose**: This is the main entry point of the application. It handles configuration loading, database connection testing, and orchestrating the CSV file processing.

**Key Methods**:
1. **Main()**: Loads configuration, tests the database connection, and processes CSV files.
2. **BuildConnectionString()**: Constructs the SQL Server connection string.
3. **TestDatabaseConnection()**: Tests the connection to the SQL Server.
4. **ProcessFiles()**: Orchestrates the CSV file processing.

**Example:**
```csharp
static async Task Main()
{
    try
    {
        // Load configuration and process CSV files
        ...
    }
    catch (SqlException sqlEx)
    {
        // Handle SQL exceptions
        ...
    }
    catch (Exception ex)
    {
        // Handle general exceptions
        ...
    }
    finally
    {
        Console.WriteLine("\nPress any key to exit...");
        Console.ReadKey();
    }
}
```

### DatabaseOperations.cs
**Purpose**: Handles database-related operations such as creating log tables, loading data into temporary tables, and transferring data to the destination table.

**Key Methods**:
1. **CreateLogTables()**: Creates the error log and success log tables.
2. **DropAndCreateTempTable()**: Drops and recreates the temporary table based on the CSV file's schema.
3. **LoadDataIntoTempTableAsync()**: Loads data from a DataTable into the temporary table using bulk copy.
4. **TransferToDestinationAsync()**: Transfers data from the temporary table to the destination table in batches.
5. **LogSuccess()**: Logs success messages to the success log table.
6. **LogError()**: Logs error messages to the error log table.

**Example:**
```csharp
public void CreateLogTables(SqlConnection connection, string errorTableName, string successLogTableName)
{
    // Create error and success log tables
    ...
}

public void DropAndCreateTempTable(SqlConnection connection, string tempTableName, DataTable dataTable)
{
    // Drop and recreate temp table
    ...
}

public async Task LoadDataIntoTempTableAsync(SqlConnection connection, string tempTableName, DataTable dataTable)
{
    // Load data into temp table
    ...
}

public async Task TransferToDestinationAsync(SqlConnection connection, string tempTableName, string destinationTableName, string errorTableName, string successLogTableName, bool isFirstFile, int batchSize)
{
    // Transfer data to destination table
    ...
}

public void LogSuccess(SqlConnection connection, string successLogTableName, string message, int totalRows, int totalColumns, SqlTransaction transaction = null)
{
    // Log success messages
    ...
}

public void LogError(SqlConnection connection, string errorTableName, string columnName, string reason, SqlTransaction transaction = null)
{
    // Log error messages
    ...
}
```

### CsvProcessor.cs
**Purpose**: Handles CSV file reading and processing in batches.

**Key Methods**:
1. **LoadCSVInBatches()**: Reads a CSV file in batches and returns a DataTable for each batch.

**Example:**
```csharp
public IEnumerable<BatchResult> LoadCSVInBatches(string filePath, SqlConnection connection, string errorTableName, int batchSize)
{
    // Read CSV in batches
    ...
}
```

### Models

#### ConfigModel.cs
**Purpose**: Models the configuration settings loaded from the `config.json` file.

**Example:**
```csharp
public class ConfigModel
{
    public DatabaseConfig DatabaseConfig { get; set; }
    public ProcessConfig ProcessConfig { get; set; }
}

public class DatabaseConfig
{
    public string Server { get; set; }
    public string Database { get; set; }
    public bool IntegratedSecurity { get; set; }
    public string Username { get; set; } // Optional
    public string Password { get; set; } // Optional
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
```

#### BatchResult.cs
**Purpose**: Represents the result of processing a batch of CSV data.

**Example:**
```csharp
public class BatchResult
{
    public DataTable Data { get; set; }
    public bool IsFirstBatch { get; set; }
    public bool IsLastBatch { get; set; }
}
```

## Permissions

### Database Permissions
Ensure the following permissions are set for the database user:
- **Read and Write Access**: To access and modify tables.
- **Create Table Permission**: To create temporary tables for data processing.
- **Execute Permission**: To execute SQL commands and stored procedures.

### File System Permissions
Ensure the following permissions are set for the application:
- **Read Access**: To read CSV files from the specified folder.
- **Write Access**: To create and write to log files in the specified folder.

## Troubleshooting

### Common Errors and Solutions

1. **FileNotFoundException**:
   - **Solution**: Ensure the `config.json` file is in the correct location and the path in `Program.cs` is accurate.

2. **Database Connection Errors**:
   - **Solution**: Verify the database server is running and accessible. Ensure the connection string details in `config.json` are correct.

3. **CSV Processing Errors**:
   - **Solution**: Check the error log table (`ErrorLog`) for detailed error messages. Ensure the CSV files are properly formatted and accessible.

### Checking Error Logs

1. **ErrorLog Table**:
   - **Location**: Your database.
   - **Query**:
     ```sql
     SELECT * FROM ErrorLog;
     ```

2. **Review Error Details**:
   - **Columns**:
     - `ColumnName`: The column where the error occurred.
     - `Reason`: The error message.
     - `Timestamp`: The time the error occurred.

3. **Example**:
   ```plaintext
   ID  ColumnName         Reason                          Timestamp
   1   TransferToDestination Must declare the scalar variable "@IDTR_A_INV" 2025-01-14 00:40:39
   2   LoadDataIntoTempTable Timeout expired              2025-01-14 00:42:12
   ```

## Contribution

Feel free to fork this repository and submit pull requests. For significant changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the YZ License - see the [LICENSE](LICENSE) file for details.

---https://dotnet.microsoft.com/en-us/download/dotnet/6.0


dotnet add package Microsoft.Data.SqlClient
dotnet add package Newtonsoft.Json

publish command
dotnet publish CSVDatabaseImporter_Test.csproj -c Release -r win-x64 --self-contained true /p:PublishSingleFile=true -o "D:\Deployment\CSVImporter"