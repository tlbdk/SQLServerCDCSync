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
        public static void GenerateMergeLoadSSISPackage(string filename, string sourceconn, string destinationconn, string[] tables)
        {
            Application app = new Application();
            Package package = new Package();

            package.Name = "CDC Merge Load Package for tables " + String.Join(", ", tables);
            
            // Add connection managers
            ConnectionManager sourceManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
            sourceManager.ConnectionString = sourceconn;
            sourceManager.Name = "Source Connection";
            ConnectionManager destinationManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
            destinationManager.ConnectionString = destinationconn;
            destinationManager.Name = "Destination Connection";

            var sourcedbname = (new System.Data.SqlClient.SqlConnectionStringBuilder(sourceconn)).InitialCatalog;

            package.Variables.Add("CDC_State", false, "User", "");

            SqlConnection sourceConnnection = new SqlConnection(sourceconn);
            sourceConnnection.Open();

            // Add CDC Get CDC Processing Range
            TaskHost cdcControlTaskGetRange = package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
            cdcControlTaskGetRange.Name = "Get CDC Processing Range";
            cdcControlTaskGetRange.Properties["Connection"].SetValue(cdcControlTaskGetRange, sourceManager.ID);
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
            cdcControlTaskMarkRange.Properties["Connection"].SetValue(cdcControlTaskMarkRange, sourceManager.ID);
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

            foreach (var table in tables)
            {
                var identityColums = new HashSet<String>();
                var colums = new List<string>();
                // Get Colums from database
                SqlCommand sqlcmd = new System.Data.SqlClient.SqlCommand("SELECT name, is_identity FROM sys.columns WHERE object_id = OBJECT_ID('" + table + "')", sourceConnnection);
                using (SqlDataReader sqlRd = sqlcmd.ExecuteReader())
                {
                    while (sqlRd.Read())
                    {
                        colums.Add(sqlRd.GetString(0));
                        if (sqlRd.GetBoolean(1))
                        {
                            identityColums.Add(sqlRd.GetString(0));
                        }
                    }
                }

                // Create the merge SQL statement
                var merge_sql =
                    "declare @start_lsn binary(10);\n" +
                    "declare @end_lsn binary(10);\n" + 
                    "declare @rowcount int;\n" +
                    "set @rowcount = 0;\n" +
                    "set @start_lsn = sys.fn_cdc_increment_lsn(CONVERT(binary(10), SUBSTRING(@cdcstate, CHARINDEX('/CS/', @cdcstate) + 4, CHARINDEX('/', @cdcstate, CHARINDEX('/CS/', @cdcstate) + 4) - CHARINDEX('/CS/', @cdcstate) - 4), 1));\n" +
                    "set @end_lsn = convert(binary(10), SUBSTRING(@cdcstate, CHARINDEX('/CE/', @cdcstate) + 4, CHARINDEX('/', @cdcstate, CHARINDEX('/CE/', @cdcstate) + 4) - CHARINDEX('/CE/', @cdcstate) - 4), 1);\n" +
                    "IF @end_lsn > @start_lsn BEGIN\n" +
                        "SET IDENTITY_INSERT [dbo].[" + table + "] ON;\n" +
                        "MERGE [dbo].[" + table + "] AS D\n" +
                        "USING " + sourcedbname + ".cdc.fn_cdc_get_net_changes_Test1(@start_lsn, @end_lsn, 'all with merge') AS S\n" +
                        "ON (" + String.Join(" AND ", identityColums.Select(s => "D." + s + " = " + "S." + s)) + ")\n" +
                        // Insert
                        "WHEN NOT MATCHED BY TARGET AND __$operation = 5\n" +
                            "THEN INSERT (" + String.Join(", ", colums) + ") VALUES(" + String.Join(", ", colums.Select(s => "S." + s)) + ")\n" +
                        // Update
                        "WHEN MATCHED AND __$operation = 5\n" +
                            "THEN UPDATE SET " + String.Join(", ", colums.Where(s => !identityColums.Contains(s)).Select(s => "D." + s + " = S." + s)) + "\n" +
                        // Delete
                        "WHEN MATCHED AND __$operation = 1\n" +
                            "THEN DELETE\n" +
                        ";\n" +
                        "set @rowcount = @@ROWCOUNT;\n" +
                        "SET IDENTITY_INSERT [dbo].[" + table + "] OFF;\n" +
                    "END\n" +
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

            app.SaveToXml(filename, package, null);
        }

        public static void GenerateInitialLoadSSISPackage(string filename, string sourceprovider, string sourceconn, string destinationconn, string cdcdatabase, string[] tables)
        {
            Application app = new Application();
            Package package = new Package();

            package.Name = "CDC Initial Load Package for tables " + String.Join(", ", tables);

            ConnectionManager sourceManager;
            ConnectionManager destinationManager;

            if (sourceprovider == "System.Data.SqlClient")
            {
                // Add SQL Server connection managers
                sourceManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
                destinationManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
               

            }
            else if (sourceprovider == "System.Data.OracleClient")
            {
                // Add SQL Server connection managers
                sourceManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(OracleConnection).AssemblyQualifiedName));
                destinationManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(OracleConnection).AssemblyQualifiedName));
            }
            else
            {
                throw new Exception("Unknown connection provider");
            }

            sourceManager.ConnectionString = sourceconn;
            sourceManager.Name = "Source Connection";
            destinationManager.ConnectionString = destinationconn;
            destinationManager.Name = "Destination Connection";

            // Add variables
            package.Variables.Add("CDC_State", false, "User", "");

            // Add CDC State Table
            TaskHost createCDCStateTable = package.Executables.Add("STOCK:SQLTask") as TaskHost;
            createCDCStateTable.Name = "Create CDC state table if it does not exist";
            createCDCStateTable.Properties["Connection"].SetValue(createCDCStateTable, destinationManager.ID);
            createCDCStateTable.Properties["SqlStatementSource"].SetValue(createCDCStateTable,
                "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='cdc_states' and xtype='U') BEGIN " +
                    "CREATE TABLE [dbo].[cdc_states] ([name] [nvarchar](256) NOT NULL, [state] [nvarchar](256) NOT NULL) ON [PRIMARY];" +
                    "CREATE UNIQUE NONCLUSTERED INDEX [cdc_states_name] ON [dbo].[cdc_states] ( [name] ASC ) WITH (PAD_INDEX  = OFF) ON [PRIMARY];" +
                "END;"
            );

            // Add CDC Initial load start
            TaskHost cdcControlTaskStartLoad = package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
            cdcControlTaskStartLoad.Name = "Mark initial load start";
            cdcControlTaskStartLoad.Properties["Connection"].SetValue(cdcControlTaskStartLoad, sourceManager.ID);
            cdcControlTaskStartLoad.Properties["TaskOperation"].SetValue(cdcControlTaskStartLoad, CdcControlTaskOperation.MarkInitialLoadStart);
            cdcControlTaskStartLoad.Properties["StateConnection"].SetValue(cdcControlTaskStartLoad, destinationManager.ID);
            cdcControlTaskStartLoad.Properties["StateVariable"].SetValue(cdcControlTaskStartLoad, "User::CDC_State");
            cdcControlTaskStartLoad.Properties["AutomaticStatePersistence"].SetValue(cdcControlTaskStartLoad, true);
            cdcControlTaskStartLoad.Properties["StateName"].SetValue(cdcControlTaskStartLoad, "CDC_State");
            cdcControlTaskStartLoad.Properties["StateTable"].SetValue(cdcControlTaskStartLoad, "[dbo].[cdc_states]");
            cdcControlTaskStartLoad.DelayValidation = true;

            // Add CDC Initial load end
            TaskHost cdcControlTaskEndLoad = package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
            cdcControlTaskEndLoad.Name = "Mark initial load end";
            cdcControlTaskEndLoad.Properties["Connection"].SetValue(cdcControlTaskEndLoad, sourceManager.ID);
            cdcControlTaskEndLoad.Properties["TaskOperation"].SetValue(cdcControlTaskEndLoad, CdcControlTaskOperation.MarkInitialLoadEnd);
            cdcControlTaskEndLoad.Properties["StateConnection"].SetValue(cdcControlTaskEndLoad, destinationManager.ID);
            cdcControlTaskEndLoad.Properties["StateVariable"].SetValue(cdcControlTaskEndLoad, "User::CDC_State");
            cdcControlTaskEndLoad.Properties["AutomaticStatePersistence"].SetValue(cdcControlTaskEndLoad, true);
            cdcControlTaskEndLoad.Properties["StateName"].SetValue(cdcControlTaskEndLoad, "CDC_State");
            cdcControlTaskEndLoad.Properties["StateTable"].SetValue(cdcControlTaskEndLoad, "[dbo].[cdc_states]");
            cdcControlTaskEndLoad.DelayValidation = true;

            // Configure precedence
            (package.PrecedenceConstraints.Add((Executable)createCDCStateTable, (Executable)cdcControlTaskStartLoad)).Value = DTSExecResult.Success;

            foreach (var table in tables)
            {
                // Create destination Table from source table definiation
                TaskHost createDestinationTable = package.Executables.Add("STOCK:SQLTask") as TaskHost;
                createDestinationTable.Name = "Create destination " + table + " from source table definiation";
                createDestinationTable.Properties["Connection"].SetValue(createDestinationTable, destinationManager.ID);
                createDestinationTable.Properties["SqlStatementSource"].SetValue(createDestinationTable, String.Format("SELECT * INTO [dbo].[{1}] FROM [{0}].[dbo].[{1}] WHERE 1 = 2;", cdcdatabase, table));
                
                // Configure precedence
                (package.PrecedenceConstraints.Add((Executable)createDestinationTable, (Executable)cdcControlTaskStartLoad)).Value = DTSExecResult.Success;
            }

            foreach (var table in tables)
            {
                // Add copy table task
                TaskHost dataFlowTask = package.Executables.Add("STOCK:PipelineTask") as TaskHost;
                dataFlowTask.Name = "Copy source to destination for " + table;
                dataFlowTask.DelayValidation = true;
                MainPipe dataFlowTaskPipe = (MainPipe)dataFlowTask.InnerObject;

                // Configure precedence
                (package.PrecedenceConstraints.Add((Executable)cdcControlTaskStartLoad, (Executable)dataFlowTask)).Value = DTSExecResult.Success;
                (package.PrecedenceConstraints.Add((Executable)dataFlowTask, (Executable)cdcControlTaskEndLoad)).Value = DTSExecResult.Success;

                // Configure the source
                IDTSComponentMetaData100 adonetsrc = dataFlowTaskPipe.ComponentMetaDataCollection.New();
                adonetsrc.Name = "ADO NET Source";
                adonetsrc.ValidateExternalMetadata = true;
                adonetsrc.ComponentClassID = app.PipelineComponentInfos["ADO NET Source"].CreationName;
                IDTSDesigntimeComponent100 adonetsrcinstance = adonetsrc.Instantiate();
                adonetsrcinstance.ProvideComponentProperties();
                adonetsrcinstance.SetComponentProperty("AccessMode", 0);
                adonetsrcinstance.SetComponentProperty("TableOrViewName", "\"dbo\".\"" + table + "\"");
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
                adonetdstinstance.SetComponentProperty("TableOrViewName", "\"dbo\".\"" + table + "\"");
                adonetdst.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(sourceManager);
                adonetdst.RuntimeConnectionCollection[0].ConnectionManagerID = sourceManager.ID;

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
            }

            app.SaveToXml(filename, package, null);
        }

    }
}
