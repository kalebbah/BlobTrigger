// using System;
// using System.IO;
// using System.Linq;
// using System.Text.Json;
// using System.Threading.Tasks;
// using Azure.Messaging.ServiceBus;
// using ClosedXML.Excel;
// using Microsoft.Azure.Functions.Worker;
// using Microsoft.Azure.Functions.Worker.Extensions.Storage.Blobs;
// using Microsoft.Extensions.Logging;
// using Microsoft.Data.SqlClient;

// namespace FunctionTrigger.Functions
// {
//     public enum Roles
//     {
//         admin,
//         registered,
//         guest
//     }
//     public class ProcessBulkEmployee
//     {
//         private readonly ILogger<ProcessBulkEmployee> _logger;
//         private readonly string _defaultPassword = "Password123!";
//         private const string emailQueueName = "certs-queue";

//         public ProcessBulkEmployee(ILogger<ProcessBulkEmployee> logger)
//         {
//             _logger = logger;
//         }

//         [Function("ProcessEmployeeFeed")]
//         [BlobOutput("processed/{name}-processed.txt", Connection = "ExternalBlobStorageConnection")]
//         public async Task<string> Run(
//             [BlobTrigger("uploads/{name}", Connection = "ExternalBlobStorageConnection")] ReadOnlyMemory<byte> blobData,
//             string name,
//             FunctionContext context)
//         {
//             var sqlConnectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
//             var serviceBusConnection = Environment.GetEnvironmentVariable("ServiceBusConnection");

//             _logger.LogInformation($"Processing blob: {name}");

//             string GetValue(IXLRow row, int index) => row.Cell(index).GetValue<string>()?.Trim();

//             try
//             {
//                 using var stream = new MemoryStream(blobData.ToArray());
//                 using var workbook = new XLWorkbook(stream);
//                 var worksheet = workbook.Worksheet(1);
//                 if (!IsValidEmployeeSheet(worksheet))
//                 {
//                     _logger.LogWarning($"Skipping blob {name}: does not contain required employee columns.");
//                     return $"Skipped {name}: invalid employee sheet format.";
//                 }

//                 using var sqlConnection = new SqlConnection(sqlConnectionString);
//                 await sqlConnection.OpenAsync();

//                 var serviceBusClient = new ServiceBusClient(serviceBusConnection);
//                 var emailSender = serviceBusClient.CreateSender(emailQueueName);

//                 int processedCount = 0;
//                 int skippedCount = 0;
//                 int errorCount = 0;

//                 foreach (var row in worksheet.RowsUsed().Skip(1))
//                 {
//                     try
//                     {
//                         var employeeId = GetValue(row, 1);
//                         var firstName = GetValue(row, 2);
//                         var lastName = GetValue(row, 3);
//                         var email = GetValue(row, 4);
//                         var phone = GetValue(row, 5);
//                         var role = GetValue(row, 7);
//                         var username = GetValue(row, 8);
//                         var fullname = $"{firstName} {lastName}".Trim();

//                         if (string.IsNullOrEmpty(employeeId) || string.IsNullOrEmpty(email))
//                         {
//                             _logger.LogWarning($"Skipping row due to missing required data. EmployeeId: {employeeId}, Email: {email}");
//                             skippedCount++;
//                             continue;
//                         }

//                         var existsCmd = new SqlCommand("SELECT COUNT(*) FROM [dbo.users] WHERE employeeid = @employeeId", sqlConnection);
//                         existsCmd.Parameters.AddWithValue("@employeeId", employeeId ?? (object)DBNull.Value);
//                         var exists = (int)await existsCmd.ExecuteScalarAsync();

//                         if (exists > 0)
//                         {
//                             _logger.LogInformation($"User {employeeId} already exists. Skipping.");
//                             skippedCount++;
//                             continue;
//                         }

//                         byte employee = 1; // or 0, depending on logic
//                         var insertUserCmd = new SqlCommand(@"
//                             INSERT INTO dbo.users (firstname, lastname, username, email, role, hashedpassword, employeeid, fullname, employee)
//                             OUTPUT INSERTED.id
//                             VALUES (@firstname, @lastname, @username, @email, @role, @hashedpassword, @employeeid, @fullname, @employee)", sqlConnection);

//                         insertUserCmd.Parameters.AddWithValue("@firstname", (object?)firstName ?? DBNull.Value);
//                         insertUserCmd.Parameters.AddWithValue("@lastname", (object?)lastName ?? DBNull.Value);
//                         insertUserCmd.Parameters.AddWithValue("@username", (object?)username ?? DBNull.Value);
//                         insertUserCmd.Parameters.AddWithValue("@email", (object?)email ?? DBNull.Value);
//                         insertUserCmd.Parameters.AddWithValue("@role", Roles.registered.ToString());
//                         insertUserCmd.Parameters.AddWithValue("@hashedpassword", BCrypt.Net.BCrypt.HashPassword(_defaultPassword));
//                         insertUserCmd.Parameters.AddWithValue("@employeeid", (object?)employeeId ?? DBNull.Value);
//                         insertUserCmd.Parameters.AddWithValue("@fullname", (object?)fullname ?? DBNull.Value);
//                         insertUserCmd.Parameters.AddWithValue("@employee", employee);

//                         var newUserId = (int)await insertUserCmd.ExecuteScalarAsync();

//                         var insertUserProfileCmd = new SqlCommand(@"
//                             INSERT INTO dbo.userprofile (phone, email, employeeid, userid)
//                             VALUES (@phone, @email, @employeeid, @userid)", sqlConnection);

//                         insertUserProfileCmd.Parameters.AddWithValue("@phone", (object?)phone ?? DBNull.Value);
//                         insertUserProfileCmd.Parameters.AddWithValue("@email", (object?)email ?? DBNull.Value);
//                         insertUserProfileCmd.Parameters.AddWithValue("@employeeid", (object?)employeeId ?? DBNull.Value);
//                         insertUserProfileCmd.Parameters.AddWithValue("@userid", newUserId);

//                         await insertUserProfileCmd.ExecuteNonQueryAsync();
                        

//                         var insertEmployeeCmd = new SqlCommand(@"
//                             INSERT INTO dbo.employees (employeeid, fullname, employeeEmail, userid, employeetenure, employeeidasint)
//                             VALUES (@employeeid, @fullname, @employeeemail, @userid, @employeetenure, @employeeidasint)", sqlConnection);

//                         var employeeIdAsInt = 0;
//                         if (int.TryParse(employeeId, out int parsedId))
//                         {
//                             employeeIdAsInt = parsedId;
//                         }
//                         insertEmployeeCmd.Parameters.AddWithValue("@employeeid", (object?)employeeIdAsInt ?? DBNull.Value);
//                         insertEmployeeCmd.Parameters.AddWithValue("@fullname", (object?)fullname ?? DBNull.Value);
//                         insertEmployeeCmd.Parameters.AddWithValue("@employeeemail", (object?)email ?? DBNull.Value);
//                         insertEmployeeCmd.Parameters.AddWithValue("@userid", newUserId);
//                         insertEmployeeCmd.Parameters.AddWithValue("@employeetenure", (object?)role ?? DBNull.Value);
//                         insertEmployeeCmd.Parameters.AddWithValue("@employeeidasint", (object?)employeeIdAsInt ?? DBNull.Value);


//                         await insertEmployeeCmd.ExecuteNonQueryAsync();


//                         var emailPayload = new
//                         {
//                             To = email,
//                             Subject = "Welcome to the CertAthon Certification Portal",
//                             Body = $"Hello {firstName},\n\nYour temporary password is: {_defaultPassword}"
//                         };

//                         var message = new ServiceBusMessage(JsonSerializer.Serialize(emailPayload));
//                         await emailSender.SendMessageAsync(message);

//                         processedCount++;
//                     }
//                     catch (Exception ex)
//                     {
//                         _logger.LogError($"Error processing row for employee {GetValue(row, 1)}: {ex.Message}");
//                         errorCount++;
//                     }
//                 }

//                 await emailSender.DisposeAsync();
//                 await serviceBusClient.DisposeAsync();

//                 var summary = $"Processed: {processedCount}, Skipped: {skippedCount}, Errors: {errorCount}";
//                 _logger.LogInformation($"Processing complete for {name}. {summary}");
//                 return summary;
//             }
//             catch (Exception ex)
//             {
//                 _logger.LogError($"Error processing blob {name}: {ex.Message}");
//                 throw;
//             }
//         }
//         private bool IsValidEmployeeSheet(IXLWorksheet worksheet)
//         {
//             var expectedColumns = new[]
//             {
//                 "id",
//                 "first_name",
//                 "last_name",
//                 "email",
//                 "phone",
//                 "role",
//                 "username",
//                 "grade"
//             };

//             var headerRow = worksheet.Row(1).CellsUsed().Select(c => c.GetValue<string>().Trim().ToLower()).ToList();

//             return expectedColumns.All(col => headerRow.Contains(col));
//         }

//     }
// }
