using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Data.SqlClient;
using System.Globalization;
using System.Threading;

class LogParser
{
    static void Main()
    {
        string logFilePath = "C:\\Prima Power\\ncexpress\\bin\\Cam3D.log";
        string connectionString = "Server=USC-3581-ANDYM\\SQLEXPRESS;Database=NCXDB;User Id=sa;Password=SUNSET;";
        string debugLogFilePath = "C:\\Prima Power\\ncexpress\\bin\\logparser_debug.log"; // Additional log file

        DateTime? importDate = null;
        string fileLocation = "";
        string partName = "";
        string materialXmlPath = "";
        string convertTime = "";
        TimeSpan importTime = TimeSpan.Zero;
        DateTime? startTime = null;
        DateTime? endTime = null;
        DateTime? lastProcessedTimestamp = null;
        long lastPosition = 0; // Track position of last read in file

        // Initialize and start logging
        LogToFile(debugLogFilePath, "LogParser started.");

        // FileSystemWatcher to monitor the log file for updates
        FileSystemWatcher fileWatcher = new FileSystemWatcher();
        fileWatcher.Path = Path.GetDirectoryName(logFilePath);
        fileWatcher.Filter = Path.GetFileName(logFilePath);
        fileWatcher.NotifyFilter = NotifyFilters.LastWrite;
        fileWatcher.Changed += (sender, e) =>
        {
            string message = $"Log file updated. Processing new entries at {DateTime.Now}.";
            LogToFile(debugLogFilePath, message);
            ProcessLogFileWithRetry(logFilePath, ref importDate, ref fileLocation, ref partName, ref materialXmlPath, ref convertTime, ref importTime, ref startTime, ref endTime, ref lastProcessedTimestamp, ref lastPosition, connectionString, debugLogFilePath);
        };

        // Begin watching the log file
        fileWatcher.EnableRaisingEvents = true;

        // Start initial processing of the log file
        ProcessLogFileWithRetry(logFilePath, ref importDate, ref fileLocation, ref partName, ref materialXmlPath, ref convertTime, ref importTime, ref startTime, ref endTime, ref lastProcessedTimestamp, ref lastPosition, connectionString, debugLogFilePath);

        Console.WriteLine("Monitoring log file for new updates. Press [Enter] to exit.");
        Console.ReadLine();
    }

    static void ProcessLogFileWithRetry(string logFilePath, ref DateTime? importDate, ref string fileLocation, ref string partName, ref string materialXmlPath, ref string convertTime, ref TimeSpan importTime, ref DateTime? startTime, ref DateTime? endTime, ref DateTime? lastProcessedTimestamp, ref long lastPosition, string connectionString, string debugLogFilePath)
    {
        bool success = false;

        // Retry logic to handle file being locked by another process (unlimited retries)
        while (!success)
        {
            try
            {
                using (FileStream fileStream = new FileStream(logFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (StreamReader reader = new StreamReader(fileStream))
                {
                    // Set the position to the last read position
                    reader.BaseStream.Seek(lastPosition, SeekOrigin.Begin);

                    while (!reader.EndOfStream)
                    {
                        string line = reader.ReadLine();
                        DateTime? currentLogTimestamp = null;  // To store the timestamp of the current log entry

                        // Handle "Import model - START -" lines
                        if (line.Contains("Import model - START -"))
                        {
                            Match match = Regex.Match(line, @"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ .* Import model - START - (.+)");
                            if (match.Success)
                            {
                                currentLogTimestamp = DateTime.ParseExact(match.Groups[1].Value, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                                // Skip if this entry is older than the last processed timestamp
                                if (lastProcessedTimestamp.HasValue && currentLogTimestamp <= lastProcessedTimestamp)
                                {
                                    continue;  // Skip processing this line as it's older than the last processed one
                                }

                                importDate = currentLogTimestamp;
                                fileLocation = match.Groups[2].Value;
                                partName = Path.GetFileName(fileLocation);
                                startTime = importDate;

                                // Output parsed info to the console for debugging
                                Console.WriteLine($"Import Start: {importDate.Value}, File Location: {fileLocation}, Part Name: {partName}");
                                LogToFile(debugLogFilePath, $"Import Start: {importDate.Value}, File Location: {fileLocation}, Part Name: {partName}");
                            }
                        }
                        // Handle "Import model - FINISHED" lines
                        else if (line.Contains("Import model - FINISHED"))
                        {
                            Match match = Regex.Match(line, @"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+.+ in ([0-9\.]+) s");
                            if (match.Success)
                            {
                                endTime = DateTime.ParseExact(match.Groups[1].Value, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                                importTime = endTime.HasValue && startTime.HasValue ? endTime.Value - startTime.Value : TimeSpan.Zero;

                                // Output parsed info to the console for debugging
                                Console.WriteLine($"Import Finished: {endTime.Value}, Import Time: {importTime.TotalSeconds} seconds");
                                LogToFile(debugLogFilePath, $"Import Finished: {endTime.Value}, Import Time: {importTime.TotalSeconds} seconds");
                            }
                        }
                        // Handle "Convert to sheet metal -" lines
                        else if (line.Contains("Convert to sheet metal -"))
                        {
                            convertTime = line;
                            // Output parsed info to the console for debugging
                            Console.WriteLine($"Convert Time: {convertTime}");
                            LogToFile(debugLogFilePath, $"Convert Time: {convertTime}");
                        }
                        // Handle "MaterialSerializer::LoadMaterialsFromXML - From path:" lines
                        else if (line.Contains("MaterialSerializer::LoadMaterialsFromXML - From path:"))
                        {
                            materialXmlPath = line.Split(new string[] { "From path:" }, StringSplitOptions.None)[1].Trim();
                            // Output parsed info to the console for debugging
                            Console.WriteLine($"Material XML Path: {materialXmlPath}");
                            LogToFile(debugLogFilePath, $"Material XML Path: {materialXmlPath}");
                        }

                        // After processing the part, insert into SQL if all relevant info is captured
                        if (importDate.HasValue && !string.IsNullOrEmpty(fileLocation) && startTime.HasValue && endTime.HasValue)
                        {
                            InsertIntoDatabase(importDate.Value, fileLocation, importTime.TotalSeconds, convertTime, materialXmlPath, partName, connectionString);

                            // Reset variables for next part entry
                            importDate = null;
                            fileLocation = "";
                            partName = "";
                            materialXmlPath = "";
                            convertTime = "";
                            importTime = TimeSpan.Zero;
                            startTime = null;
                            endTime = null;

                            // Update the last processed timestamp after inserting into the database
                            lastProcessedTimestamp = currentLogTimestamp;
                        }
                    }

                    // Update the position after reading all lines
                    lastPosition = reader.BaseStream.Position;
                }
                success = true; // Success if we exit the try block without errors
            }
            catch (IOException ex)
            {
                // Log the error and retry indefinitely
                Console.WriteLine($"Error accessing log file: {ex.Message}. Retrying...");
                LogToFile(debugLogFilePath, $"Error accessing log file: {ex.Message}. Retrying...");
                // Wait a moment before retrying
                Thread.Sleep(1000);
            }
        }
    }

    static void InsertIntoDatabase(DateTime importDate, string fileLocation, double importTime, string convertTime, string materialXmlPath, string partName, string connectionString)
    {
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            conn.Open();
            string query = "INSERT INTO Cam3DLog (ImportDate, FileLocation, ImportTime, ConvertTime, MaterialXMLPath, PartName) " +
                           "VALUES (@importDate, @fileLocation, @importTime, @convertTime, @materialXmlPath, @partName)";

            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@partName", partName);
                cmd.Parameters.AddWithValue("@importDate", importDate);
                cmd.Parameters.AddWithValue("@fileLocation", fileLocation);
                cmd.Parameters.AddWithValue("@importTime", importTime);
                cmd.Parameters.AddWithValue("@convertTime", convertTime);
                cmd.Parameters.AddWithValue("@materialXmlPath", materialXmlPath);
                cmd.ExecuteNonQuery();
            }
        }
    }

    static void LogToFile(string logFilePath, string message)
    {
        try
        {
            using (StreamWriter writer = new StreamWriter(logFilePath, true))
            {
                writer.WriteLine($"{DateTime.Now}: {message}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error writing to log file: {ex.Message}");
        }
    }
}
