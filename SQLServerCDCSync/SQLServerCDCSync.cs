using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using System.Globalization;
using System.Data.SqlClient;
using Attunity.SqlServer.CDCControlTask;
using Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask;
using System.Data.OracleClient;

// Links : EzAPI
// http://social.msdn.microsoft.com/Forums/sqlserver/en-US/eb8b10bc-963c-4d36-8ea2-6c3ebbc20411/copying-600-tables-in-ssis-how-to?forum=sqlintegrationservices
// http://www.experts-exchange.com/Database/MS-SQL-Server/Q_23972361.html
// Fix permissions:
// http://www.mssqltips.com/sqlservertip/3086/how-to-resolve-ssis-access-denied-error-in-sql-server-management-studio/

// Problem with having the CDC Controls in a Sequence container:
// http://social.technet.microsoft.com/Forums/systemcenter/en-US/421b126b-1729-464f-aaa3-1a2175c09e28/cdc-control-task-timeout-expired-when-transaction-is-required 

// SSIS transaction might not be the best option
// http://www.mattmasson.com/2011/12/design-pattern-avoiding-transactions/

namespace SQLServerCDCSync
{
    class SQLServerCDCSync
    {
        public static void InitializeEnvironment()
        {
            // Make sure Oracle PATH and environment variables are set else OracleClient won't work
            string oraclehome;
            string path = Environment.GetEnvironmentVariable("PATH", EnvironmentVariableTarget.Process);
            if (Environment.GetEnvironmentVariable("ORACLE_HOME") == null)
            {
                oraclehome = OracleUtils.GetOracleHome();
                Environment.SetEnvironmentVariable("ORACLE_HOME", oraclehome, EnvironmentVariableTarget.Process);
            }
            else
            {
                oraclehome = Environment.GetEnvironmentVariable("ORACLE_HOME");
            }
            if (!path.Contains(oraclehome))
            {

                Environment.SetEnvironmentVariable("PATH", path + ";" + oraclehome, EnvironmentVariableTarget.Process);
            }
        }

        public static void GenerateMergeLoadSSISPackage(string filename, string destinationconn, string cdcdatabase, string[] tables)
        {
            Application app = new Application();
            Package package = new Package();

            package.Name = "CDC Merge Load Package for tables " + String.Join(", ", tables);
            
            // Add connection managers
            ConnectionManager destinationManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
            destinationManager.ConnectionString = destinationconn;
            destinationManager.Name = "Destination Connection";
            
            // Build the CDC connection string
            var cdcconn = (new System.Data.SqlClient.SqlConnectionStringBuilder(destinationconn));
            cdcconn.InitialCatalog = cdcdatabase;
            ConnectionManager cdcManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
            cdcManager.ConnectionString = cdcconn.ConnectionString;
            cdcManager.Name = "CDC database Connection";

            package.Variables.Add("CDC_State", false, "User", "");

            // Add CDC Get CDC Processing Range
            TaskHost cdcControlTaskGetRange = package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
            cdcControlTaskGetRange.Name = "Get CDC Processing Range";
            cdcControlTaskGetRange.Properties["Connection"].SetValue(cdcControlTaskGetRange, cdcManager.ID);
            cdcControlTaskGetRange.Properties["TaskOperation"].SetValue(cdcControlTaskGetRange, CdcControlTaskOperation.GetProcessingRange);
            cdcControlTaskGetRange.Properties["StateConnection"].SetValue(cdcControlTaskGetRange, destinationManager.ID);
            cdcControlTaskGetRange.Properties["StateVariable"].SetValue(cdcControlTaskGetRange, "User::CDC_State");
            cdcControlTaskGetRange.Properties["AutomaticStatePersistence"].SetValue(cdcControlTaskGetRange, true);
            cdcControlTaskGetRange.Properties["StateName"].SetValue(cdcControlTaskGetRange, "CDC_State");
            cdcControlTaskGetRange.Properties["StateTable"].SetValue(cdcControlTaskGetRange, "[dbo].[cdc_states]");
            cdcControlTaskGetRange.DelayValidation = true;
            //cdcControlTaskGetRange.TransactionOption = DTSTransactionOption.Required;

            // Add Mark CDC Processed Range
            TaskHost cdcControlTaskMarkRange = package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
            cdcControlTaskMarkRange.Name = "Mark CDC Processed Range";
            cdcControlTaskMarkRange.Properties["Connection"].SetValue(cdcControlTaskMarkRange, cdcManager.ID);
            cdcControlTaskMarkRange.Properties["TaskOperation"].SetValue(cdcControlTaskMarkRange, CdcControlTaskOperation.MarkProcessedRange);
            cdcControlTaskMarkRange.Properties["StateConnection"].SetValue(cdcControlTaskMarkRange, destinationManager.ID);
            cdcControlTaskMarkRange.Properties["StateVariable"].SetValue(cdcControlTaskMarkRange, "User::CDC_State");
            cdcControlTaskMarkRange.Properties["AutomaticStatePersistence"].SetValue(cdcControlTaskMarkRange, true);
            cdcControlTaskMarkRange.Properties["StateName"].SetValue(cdcControlTaskMarkRange, "CDC_State");
            cdcControlTaskMarkRange.Properties["StateTable"].SetValue(cdcControlTaskMarkRange, "[dbo].[cdc_states]");
            cdcControlTaskMarkRange.DelayValidation = true;
            //cdcControlTaskMarkRange.TransactionOption = DTSTransactionOption.Required;

            // Create a Sequence Container to make sure we have a transaction for all the merge operations
            Sequence sequenceContainer = (Sequence)package.Executables.Add("STOCK:SEQUENCE");
            sequenceContainer.FailPackageOnFailure = true;
            sequenceContainer.FailParentOnFailure = true;
            sequenceContainer.Name = "Sequence Container";
            sequenceContainer.TransactionOption = DTSTransactionOption.Required;

            SqlConnection cdcConnnection = new SqlConnection(cdcconn.ConnectionString);
            cdcConnnection.Open();

            // Get list of CDC tables
            var cdctables = new Dictionary<String, String>();
            SqlCommand sqlcmdtables = new System.Data.SqlClient.SqlCommand(
                "SELECT s.name, t.name " +
                "FROM sys.tables t INNER JOIN sys.schemas s ON (t.schema_id = s.schema_id) " +
                "WHERE s.name != 'cdc' and t.is_ms_shipped = 0 and t.is_tracked_by_cdc = 1;", cdcConnnection);
            using (SqlDataReader sqlRd = sqlcmdtables.ExecuteReader())
            {
                while (sqlRd.Read())
                {
                    cdctables[sqlRd.GetString(1)] = sqlRd.GetString(0) + "_" + sqlRd.GetString(1) + "";
                }
            }

            foreach (var table in tables)
            {
                var uniqueColums = new HashSet<String>();
                var colums = new List<string>();
                var identityColumsUsed = false;
                
                // Get Colums from database
                SqlCommand sqlcmd = new System.Data.SqlClient.SqlCommand(
                    "SELECT c.name, is_identity, ISNULL(i.is_unique, 0)\n" +
                    "FROM sys.tables t\n" +
                    "LEFT JOIN sys.columns c ON (t.object_id = c.object_id)\n" +
                    "LEFT JOIN sys.index_columns ic ON (c.object_id = ic.object_id AND c.column_id = ic.column_id)\n" +
                    "LEFT JOIN sys.indexes i ON (i.object_id = ic.object_id AND i.index_id = ic.index_id)\n" +
                    "WHERE t.name = '" + table + "';\n",
                cdcConnnection);
                using (SqlDataReader sqlRd = sqlcmd.ExecuteReader())
                {
                    while (sqlRd.Read())
                    {
                        colums.Add(sqlRd.GetString(0));
                        if (sqlRd.GetBoolean(1) || sqlRd.GetBoolean(2))
                        {
                            uniqueColums.Add(sqlRd.GetString(0));
                        }

                        if (sqlRd.GetBoolean(1))
                        {
                            identityColumsUsed = true;
                        }
                    }
                }

                if (uniqueColums.Count == 0)
                {
                    throw new Exception("Could not find any unique colums on the table");
                }

                // Create the merge SQL statement
                var merge_sql =
                    // Get the start lns and the lsn from the CDC mark operation
                    "declare @start_lsn binary(10);\n" +
                    "declare @end_lsn binary(10);\n" + 
                    "set @start_lsn = sys.fn_cdc_increment_lsn(CONVERT(binary(10), SUBSTRING(@cdcstate, CHARINDEX('/CS/', @cdcstate) + 4, CHARINDEX('/', @cdcstate, CHARINDEX('/CS/', @cdcstate) + 4) - CHARINDEX('/CS/', @cdcstate) - 4), 1));\n" +
                    "set @end_lsn = convert(binary(10), SUBSTRING(@cdcstate, CHARINDEX('/CE/', @cdcstate) + 4, CHARINDEX('/', @cdcstate, CHARINDEX('/CE/', @cdcstate) + 4) - CHARINDEX('/CE/', @cdcstate) - 4), 1);\n" +
                    
                    // Get the start lns and the lsn from the CDC mark operation
                    "declare @tablecdcstate nvarchar(256);\n" +
                    "set @tablecdcstate = (SELECT state FROM cdc_states WHERE name = 'CDC_State_" + table + "')\n" +
                    "if @tablecdcstate IS NOT NULL BEGIN\n" +
                        "declare @table_start_lsn binary(10);\n" +
                        "set @table_start_lsn = sys.fn_cdc_increment_lsn(CONVERT(binary(10), SUBSTRING(@tablecdcstate, CHARINDEX('/IR/', @tablecdcstate) + 4, CHARINDEX('/', @tablecdcstate, CHARINDEX('/IR/', @tablecdcstate) + 4) - CHARINDEX('/IR',@tablecdcstate) - 4), 1));\n" +
                        "if @table_start_lsn < @start_lsn BEGIN\n" +
                            "set @start_lsn = @table_start_lsn;\n" +
                        "END\n" +
                    "END;\n" +
                    
                    // Create Merge statement
                    "declare @rowcount int;\n" +
                    "set @rowcount = 0;\n" +
                    "IF @end_lsn > @start_lsn BEGIN\n" +
                        (identityColumsUsed ? "SET IDENTITY_INSERT [dbo].[" + table + "] ON;\n": "") +
                        "MERGE [dbo].[" + table + "] AS D\n" +
                        "USING " + cdcdatabase + ".cdc.fn_cdc_get_net_changes_" + cdctables[table] + "(@start_lsn, @end_lsn, 'all with merge') AS S\n" +
                        "ON (" + String.Join(" AND ", uniqueColums.Select(s => "D." + s + " = " + "S." + s)) + ")\n" +
                        // Insert
                        "WHEN NOT MATCHED BY TARGET AND __$operation = 5\n" +
                            "THEN INSERT (" + String.Join(", ", colums) + ") VALUES(" + String.Join(", ", colums.Select(s => "S." + s)) + ")\n" +
                        // Update
                        "WHEN MATCHED AND __$operation = 5\n" +
                            "THEN UPDATE SET " + String.Join(", ", colums.Where(s => !uniqueColums.Contains(s)).Select(s => "D." + s + " = S." + s)) + "\n" +
                        // Delete
                        "WHEN MATCHED AND __$operation = 1\n" +
                            "THEN DELETE\n" +
                        ";\n" +
                        "set @rowcount = @@ROWCOUNT;\n" +
                        (identityColumsUsed ? "SET IDENTITY_INSERT [dbo].[" + table + "] OFF;\n" : "") +
                        "DELETE FROM cdc_states WHERE name = 'CDC_State_" + table + "';\n" +
                    "END\n" +
                    "INSERT INTO cdc_status ([name], [rowcount]) VALUES('" + table + "',@rowcount)\n" +
                    "SELECT @rowcount as RowsUpdated;\n";

                // Add variables
                package.Variables.Add("RowsUpdated_" + table, false, "User", "");

                // Add Execute Merge Command
                TaskHost executeMergeSQL = sequenceContainer.Executables.Add("STOCK:SQLTask") as TaskHost;
                executeMergeSQL.Name = "Execute Merge Command for " + table;
                executeMergeSQL.TransactionOption = DTSTransactionOption.Required;
                executeMergeSQL.Properties["Connection"].SetValue(executeMergeSQL, destinationManager.ID);
                executeMergeSQL.Properties["SqlStatementSource"].SetValue(executeMergeSQL, merge_sql);
                var executeMergeSQLTask = executeMergeSQL.InnerObject as ExecuteSQLTask;
                executeMergeSQLTask.ResultSetType = ResultSetType.ResultSetType_SingleRow;
                // Add input parameter binding
                executeMergeSQLTask.ParameterBindings.Add();
                IDTSParameterBinding parameterBinding = executeMergeSQLTask.ParameterBindings.GetBinding(0);
                parameterBinding.DtsVariableName = "CDC_State";
                parameterBinding.ParameterDirection = ParameterDirections.Input;
                parameterBinding.DataType = 16;
                parameterBinding.ParameterName = "@cdcstate";
                parameterBinding.ParameterSize = -1;
                // Add result set binding, map the id column to variable
                executeMergeSQLTask.ResultSetBindings.Add();
                IDTSResultBinding resultBinding = executeMergeSQLTask.ResultSetBindings.GetBinding(0);
                resultBinding.ResultName = "0";
                resultBinding.DtsVariableName = "User::RowsUpdated_" + table;
            }

            // Configure precedence
            (package.PrecedenceConstraints.Add((Executable)cdcControlTaskGetRange, (Executable)sequenceContainer)).Value = DTSExecResult.Success;
            (package.PrecedenceConstraints.Add((Executable)sequenceContainer, (Executable)cdcControlTaskMarkRange)).Value = DTSExecResult.Success;

            package.ProtectionLevel = DTSProtectionLevel.EncryptAllWithPassword;
            package.PackagePassword = "Qwerty1234";
            app.SaveToXml(filename, package, null);
        }

        public static void GenerateInitialLoadSSISPackage(string filename, string sourceprovider, string sourceconn, string destinationconn, string cdcdatabase, string[] tables)
        {
            Application app = new Application();
            Package package = new Package();

            package.Name = "CDC Initial Load Package for tables " + String.Join(", ", tables);

            ConnectionManager sourceManager;
            ConnectionManager destinationManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
            destinationManager.ConnectionString = destinationconn;
            destinationManager.Name = "Destination Connection";

            // Build the CDC connection string
            var cdcconn = (new System.Data.SqlClient.SqlConnectionStringBuilder(destinationconn));
            cdcconn.InitialCatalog = cdcdatabase;
            ConnectionManager cdcManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
            cdcManager.ConnectionString = cdcconn.ConnectionString;
            cdcManager.Name = "CDC database Connection";

            // Get CDC tables from database
            SqlConnection cdcConnnection = new SqlConnection(cdcconn.ConnectionString);
            cdcConnnection.Open();
            var cdctables = new Dictionary<String,String>();
            SqlCommand sqlcmd = new System.Data.SqlClient.SqlCommand(
                "SELECT s.name, t.name " +
                "FROM sys.tables t INNER JOIN sys.schemas s ON (t.schema_id = s.schema_id) " +
                "WHERE s.name != 'cdc' and t.is_ms_shipped = 0 and t.is_tracked_by_cdc = 1;", cdcConnnection);
            using (SqlDataReader sqlRd = sqlcmd.ExecuteReader())
            {
                while (sqlRd.Read())
                {
                    cdctables[sqlRd.GetString(1)] =  sqlRd.GetString(0) + "." + sqlRd.GetString(1) + "";
                }
            }

            // Source connection managers
            if (sourceprovider == "System.Data.SqlClient")
            {                
                sourceManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
            }
            else if (sourceprovider == "System.Data.OracleClient")
            {
                sourceManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(OracleConnection).AssemblyQualifiedName));
            }
            else
            {
                throw new Exception("Unknown connection provider");
            }

            sourceManager.ConnectionString = sourceconn;
            sourceManager.Name = "Source Connection";
           
            // Add CDC State Table
            TaskHost createCDCStateTable = package.Executables.Add("STOCK:SQLTask") as TaskHost;
            createCDCStateTable.Name = "Create CDC state table if it does not exist";
            createCDCStateTable.Properties["Connection"].SetValue(createCDCStateTable, destinationManager.ID);
            createCDCStateTable.Properties["SqlStatementSource"].SetValue(createCDCStateTable,
                "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='cdc_states' and xtype='U') BEGIN\n" +
                    "CREATE TABLE [dbo].[cdc_states] ([name] [nvarchar](256) NOT NULL, [state] [nvarchar](256) NOT NULL) ON [PRIMARY];\n" +
                    "CREATE UNIQUE NONCLUSTERED INDEX [cdc_states_name] ON [dbo].[cdc_states] ( [name] ASC ) WITH (PAD_INDEX  = OFF) ON [PRIMARY];\n" +
                "END;"
            );

            // Add CDC status Table
            TaskHost createCDCMergeStatusTable = package.Executables.Add("STOCK:SQLTask") as TaskHost;
            createCDCMergeStatusTable.Name = "Create CDC status table if it does not exist";
            createCDCMergeStatusTable.Properties["Connection"].SetValue(createCDCMergeStatusTable, destinationManager.ID);
            createCDCMergeStatusTable.Properties["SqlStatementSource"].SetValue(createCDCMergeStatusTable,
                "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='cdc_status' and xtype='U') BEGIN\n" +
                    "CREATE TABLE [dbo].[cdc_status] (" + 
                        "[id] [int] IDENTITY(1,1) NOT NULL, " + 
                        "[name] [nvarchar](256) NOT NULL, " + 
                        "[rowcount] [int] NOT NULL, " + 
                        "[created] [datetime] default CURRENT_TIMESTAMP, " +
                    "CONSTRAINT [PK_cdc_status] PRIMARY KEY CLUSTERED ([id] ASC) ON [PRIMARY]) ON [PRIMARY];\n" +
                "END;"
            );

            // Copy CDC State variable from the first table if it does not exists
            TaskHost copyCDCState = package.Executables.Add("STOCK:SQLTask") as TaskHost;
            copyCDCState.Name = "Copy the CDC State of the first table to start if the CDC_State does not exist";
            copyCDCState.Properties["Connection"].SetValue(copyCDCState, destinationManager.ID);
            copyCDCState.Properties["SqlStatementSource"].SetValue(copyCDCState,
                "IF NOT EXISTS (SELECT TOP 1 state FROM cdc_states WHERE name = 'CDC_State') BEGIN\n" +
                   "INSERT INTO cdc_states(name, state)\n" +
                   "SELECT TOP 1 'CDC_State' name, state FROM cdc_states ORDER BY state ASC\n" +
                "END;\n"
            );

            foreach (var table in tables)
            {
                // Add variables
                package.Variables.Add("CDC_State_" + table, false, "User", "");

                // Create destination Table from source table definiation
                TaskHost createDestinationTable = package.Executables.Add("STOCK:SQLTask") as TaskHost;
                createDestinationTable.Name = "Create destination " + table + " from source table definiation";
                createDestinationTable.Properties["Connection"].SetValue(createDestinationTable, destinationManager.ID);
                createDestinationTable.Properties["SqlStatementSource"].SetValue(createDestinationTable, String.Format("SELECT * INTO [dbo].{0} FROM [{1}].{2} WHERE 1 = 2;", table, cdcdatabase, cdctables[table]));

				// TODO: Create a index from the primary key
				
                // Add CDC Initial load start
                TaskHost cdcMarkStartInitialLoad = package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
                cdcMarkStartInitialLoad.Name = "Mark initial load start for table " + table;
                cdcMarkStartInitialLoad.Properties["Connection"].SetValue(cdcMarkStartInitialLoad, cdcManager.ID);
                cdcMarkStartInitialLoad.Properties["TaskOperation"].SetValue(cdcMarkStartInitialLoad, CdcControlTaskOperation.MarkInitialLoadStart);
                cdcMarkStartInitialLoad.Properties["StateConnection"].SetValue(cdcMarkStartInitialLoad, destinationManager.ID);
                cdcMarkStartInitialLoad.Properties["StateVariable"].SetValue(cdcMarkStartInitialLoad, "User::CDC_State_" + table);
                cdcMarkStartInitialLoad.Properties["AutomaticStatePersistence"].SetValue(cdcMarkStartInitialLoad, true);
                cdcMarkStartInitialLoad.Properties["StateName"].SetValue(cdcMarkStartInitialLoad, "CDC_State_" + table );
                cdcMarkStartInitialLoad.Properties["StateTable"].SetValue(cdcMarkStartInitialLoad, "[dbo].[cdc_states]");
                cdcMarkStartInitialLoad.DelayValidation = true;

                // Add copy table task
                TaskHost dataFlowTask = package.Executables.Add("STOCK:PipelineTask") as TaskHost;
                dataFlowTask.Name = "Copy source to destination for " + table;
                dataFlowTask.DelayValidation = true;
                MainPipe dataFlowTaskPipe = (MainPipe)dataFlowTask.InnerObject;

                // Add CDC Initial load end
                TaskHost cdcMarkEndInitialLoad = package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
                cdcMarkEndInitialLoad.Name = "Mark initial load end for table " + table;
                cdcMarkEndInitialLoad.Properties["Connection"].SetValue(cdcMarkEndInitialLoad, cdcManager.ID);
                cdcMarkEndInitialLoad.Properties["TaskOperation"].SetValue(cdcMarkEndInitialLoad, CdcControlTaskOperation.MarkInitialLoadEnd);
                cdcMarkEndInitialLoad.Properties["StateConnection"].SetValue(cdcMarkEndInitialLoad, destinationManager.ID);
                cdcMarkEndInitialLoad.Properties["StateVariable"].SetValue(cdcMarkEndInitialLoad, "User::CDC_State_" + table);
                cdcMarkEndInitialLoad.Properties["AutomaticStatePersistence"].SetValue(cdcMarkEndInitialLoad, true);
                cdcMarkEndInitialLoad.Properties["StateName"].SetValue(cdcMarkEndInitialLoad, "CDC_State_" + table);
                cdcMarkEndInitialLoad.Properties["StateTable"].SetValue(cdcMarkEndInitialLoad, "[dbo].[cdc_states]");
                cdcMarkEndInitialLoad.DelayValidation = true;

                // Configure precedence
                (package.PrecedenceConstraints.Add((Executable)createCDCStateTable, (Executable)createDestinationTable)).Value = DTSExecResult.Success;
                (package.PrecedenceConstraints.Add((Executable)createCDCMergeStatusTable, (Executable)createDestinationTable)).Value = DTSExecResult.Success;
                (package.PrecedenceConstraints.Add((Executable)createDestinationTable, (Executable)cdcMarkStartInitialLoad)).Value = DTSExecResult.Success;
                (package.PrecedenceConstraints.Add((Executable)cdcMarkStartInitialLoad, (Executable)dataFlowTask)).Value = DTSExecResult.Success;
                (package.PrecedenceConstraints.Add((Executable)dataFlowTask, (Executable)cdcMarkEndInitialLoad)).Value = DTSExecResult.Success;
                (package.PrecedenceConstraints.Add((Executable)cdcMarkEndInitialLoad, (Executable)copyCDCState)).Value = DTSExecResult.Success;

                // Configure the source
                IDTSComponentMetaData100 adonetsrc = dataFlowTaskPipe.ComponentMetaDataCollection.New();
                adonetsrc.Name = "ADO NET Source";
                adonetsrc.ValidateExternalMetadata = true;
                adonetsrc.ComponentClassID = app.PipelineComponentInfos["ADO NET Source"].CreationName;
                IDTSDesigntimeComponent100 adonetsrcinstance = adonetsrc.Instantiate();
                adonetsrcinstance.ProvideComponentProperties();
                adonetsrcinstance.SetComponentProperty("AccessMode", 0);
                adonetsrcinstance.SetComponentProperty("TableOrViewName", table);
                adonetsrc.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(sourceManager);
                adonetsrc.RuntimeConnectionCollection[0].ConnectionManagerID = sourceManager.ID;
                adonetsrcinstance.AcquireConnections(null);
                adonetsrcinstance.ReinitializeMetaData();
                adonetsrcinstance.ReleaseConnections();

                // Configure the destination
                IDTSComponentMetaData100 adonetdst = dataFlowTaskPipe.ComponentMetaDataCollection.New();
                adonetdst.Name = "ADO NET Destination";
                adonetdst.ValidateExternalMetadata = true;
                adonetdst.ComponentClassID = app.PipelineComponentInfos["ADO NET Destination"].CreationName;
                IDTSDesigntimeComponent100 adonetdstinstance = adonetdst.Instantiate();
                adonetdstinstance.ProvideComponentProperties();
                adonetdstinstance.SetComponentProperty("TableOrViewName", cdctables[table]); // TODO
                // Point to the CDC tables when generating the metadata
                adonetdst.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(cdcManager);
                adonetdst.RuntimeConnectionCollection[0].ConnectionManagerID = cdcManager.ID;
                adonetdstinstance.AcquireConnections(null);
                adonetdstinstance.ReinitializeMetaData();
                adonetdstinstance.ReleaseConnections();

                // Attach the path from data flow source to destination
                IDTSPath100 path = dataFlowTaskPipe.PathCollection.New();
                path.AttachPathAndPropagateNotifications(adonetsrc.OutputCollection[0], adonetdst.InputCollection[0]);

                // Do coloum mapping on the destination
                IDTSInput100 destInput = adonetdst.InputCollection[0];
                IDTSExternalMetadataColumnCollection100 externalColumnCollection = destInput.ExternalMetadataColumnCollection;
                IDTSVirtualInput100 destVirInput = destInput.GetVirtualInput();
                IDTSInputColumnCollection100 destInputCols = destInput.InputColumnCollection;
                IDTSExternalMetadataColumnCollection100 destExtCols = destInput.ExternalMetadataColumnCollection;
                IDTSOutputColumnCollection100 sourceColumns = adonetsrc.OutputCollection[0].OutputColumnCollection;

                // Hook up the external columns
                foreach (IDTSOutputColumn100 outputCol in sourceColumns)
                {
                    // Get the external column id
                    IDTSExternalMetadataColumn100 extCol = (IDTSExternalMetadataColumn100)destExtCols[outputCol.Name];
                    if (extCol != null)
                    {
                        // Create an input column from an output col of previous component.
                        destVirInput.SetUsageType(outputCol.ID, DTSUsageType.UT_READONLY);
                        IDTSInputColumn100 inputCol = destInputCols.GetInputColumnByLineageID(outputCol.ID);
                        if (inputCol != null)
                        {
                            // map the input column with an external metadata column
                            adonetdstinstance.MapInputColumn(destInput.ID, inputCol.ID, extCol.ID);
                        }
                    }
                }

                adonetdst.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(destinationManager);
                adonetdst.RuntimeConnectionCollection[0].ConnectionManagerID = destinationManager.ID;
                adonetdstinstance.SetComponentProperty("TableOrViewName",  table);
            }

            /*package.ProtectionLevel = DTSProtectionLevel.EncryptAllWithPassword;
            package.PackagePassword = "Qwerty1234"; */

            app.SaveToXml(filename, package, null);
        }
    }
}
