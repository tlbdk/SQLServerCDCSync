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

// Links : EzAPI
// http://social.msdn.microsoft.com/Forums/sqlserver/en-US/eb8b10bc-963c-4d36-8ea2-6c3ebbc20411/copying-600-tables-in-ssis-how-to?forum=sqlintegrationservices
// http://www.experts-exchange.com/Database/MS-SQL-Server/Q_23972361.html
// Fix permissions:
// http://www.mssqltips.com/sqlservertip/3086/how-to-resolve-ssis-access-denied-error-in-sql-server-management-studio/

namespace SQLServerCDCSync
{
    class SQLServerCDCSync
    {
        public static void GenerateMergeLoadSSISPackage(string filename, string sourceconn, string destinationconn, string[] tables)
        {
            Application app = new Application();
            Package package = new Package();

            // Add connection managers
            ConnectionManager destinationManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
            destinationManager.ConnectionString = destinationconn;
            destinationManager.Name = "Destination Connection";

            var sourcedbname = (new System.Data.SqlClient.SqlConnectionStringBuilder(sourceconn)).InitialCatalog;

            SqlConnection sourceConnnection = new SqlConnection(sourceconn);
            sourceConnnection.Open();

            foreach (var table in tables)
            {
                // Get Colums from database
                SqlCommand sqlcmd = new System.Data.SqlClient.SqlCommand("SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('"+ table +"')", sourceConnnection);
                var colums = new List<string>();
                using (SqlDataReader sqlRd = sqlcmd.ExecuteReader())
                {
                    while (sqlRd.Read())
                    {
                        colums.Add(sqlRd.GetString(0));
                    }
                }

                // Create the merge SQL statement
                var merge_sql =
                    "set @rowcount = 0;\n" +
                    "set @start_lsn = sys.fn_cdc_increment_lsn(CONVERT(binary(10), SUBSTRING(@cdcstate, CHARINDEX('/CS/', @cdcstate) + 4, CHARINDEX('/', @cdcstate, CHARINDEX('/CS/', @cdcstate) + 4) - CHARINDEX('/CS/', @cdcstate) - 4), 1));\n" +
                    "set @end_lsn = convert(binary(10), SUBSTRING(@cdcstate, CHARINDEX('/CE/', @cdcstate) + 4, CHARINDEX('/', @cdcstate, CHARINDEX('/CE/', @cdcstate) + 4) - CHARINDEX('/CE/', @cdcstate) - 4), 1);\n" +
                    "IF @end_lsn > @start_lsn BEGIN\n" +
                        "SET IDENTITY_INSERT [dbo].[" + table + "] ON;\n" +
                        "MERGE [dbo].[" + table + "] AS D\n" +
                        "USING " + sourcedbname + ".cdc.fn_cdc_get_net_changes_Test1(@start_lsn, @end_lsn, 'all with merge') AS S\n" +
                        "ON (D.Id = S.Id)\n" +
                        // Insert
                        "WHEN NOT MATCHED BY TARGET AND __$operation = 5\n" +
                            "THEN INSERT (" + String.Join(", ", colums) + ") VALUES(" + String.Join(", ", colums.Select(s => "S." + s)) + ")\n" +
                        // Update
                        "WHEN MATCHED AND __$operation = 5\n" +
                            "THEN UPDATE SET " + String.Join(", ", colums.Select(s => "D." + s + " = S." + s)) + "\n" +
                        // Delete
                        "WHEN MATCHED AND __$operation = 1\n" +
                            "THEN DELETE\n" +
                        ";\n" +
                        "set @rowcount = @@ROWCOUNT;\n" +
                        "SET IDENTITY_INSERT [dbo].[" + table + "] OFF;\n" +
                    "END\n" +
                    "SELECT @rowcount as NumberOfRecords;\n";
            }

            app.SaveToXml(filename, package, null);
        }

        public static void GenerateInitialLoadSSISPackage(string filename, string sourceconn, string destinationconn, string[] tables)
        {
            Application app = new Application();
            Package package = new Package();

            // Add connection managers
            ConnectionManager sourceManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
            sourceManager.ConnectionString = sourceconn;
            sourceManager.Name = "Source Connection";
            ConnectionManager destinationManager = package.Connections.Add(string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName));
            destinationManager.ConnectionString = destinationconn;
            destinationManager.Name = "Destination Connection";

            var sourcedbname = (new System.Data.SqlClient.SqlConnectionStringBuilder(sourceconn)).InitialCatalog;

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

            foreach (var table in tables)
            {
                // Add variables
                package.Variables.Add("CDC_State_" + table, false, "User", "");

                // Create destination Table from source table definiation
                TaskHost createDestinationTable = package.Executables.Add("STOCK:SQLTask") as TaskHost;
                createDestinationTable.Name = "Create destination " + table + " from source table definiation";
                createDestinationTable.Properties["Connection"].SetValue(createDestinationTable, destinationManager.ID);
                createDestinationTable.Properties["SqlStatementSource"].SetValue(createDestinationTable, String.Format("SELECT * INTO [dbo].[{1}] FROM [{0}].[dbo].[{1}] WHERE 1 = 2;", sourcedbname, table));

                // Add copy table task
                TaskHost dataFlowTask = package.Executables.Add("STOCK:PipelineTask") as TaskHost;
                dataFlowTask.Name = "Copy source to destination for " + table;
                dataFlowTask.DelayValidation = true;
                MainPipe dataFlowTaskPipe = (MainPipe)dataFlowTask.InnerObject;

                // Add CDC Initial load start
                TaskHost cdcControlTaskStartLoad = package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
                cdcControlTaskStartLoad.Name = "Mark initial load start for " + table;
                cdcControlTaskStartLoad.Properties["Connection"].SetValue(cdcControlTaskStartLoad, sourceManager.ID);
                cdcControlTaskStartLoad.Properties["TaskOperation"].SetValue(cdcControlTaskStartLoad, CdcControlTaskOperation.MarkInitialLoadStart);
                cdcControlTaskStartLoad.Properties["StateConnection"].SetValue(cdcControlTaskStartLoad, destinationManager.ID);
                cdcControlTaskStartLoad.Properties["StateVariable"].SetValue(cdcControlTaskStartLoad, "User::CDC_State_" + table);
                cdcControlTaskStartLoad.Properties["AutomaticStatePersistence"].SetValue(cdcControlTaskStartLoad, true);
                cdcControlTaskStartLoad.Properties["StateName"].SetValue(cdcControlTaskStartLoad, "CDC_State_" + table);
                cdcControlTaskStartLoad.Properties["StateTable"].SetValue(cdcControlTaskStartLoad, "[dbo].[cdc_states]");
                cdcControlTaskStartLoad.DelayValidation = true;

                // Add CDC Initial load end
                TaskHost cdcControlTaskEndLoad = package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
                cdcControlTaskEndLoad.Name = "Mark initial load end for " + table;
                cdcControlTaskEndLoad.Properties["Connection"].SetValue(cdcControlTaskEndLoad, sourceManager.ID);
                cdcControlTaskEndLoad.Properties["TaskOperation"].SetValue(cdcControlTaskEndLoad, CdcControlTaskOperation.MarkInitialLoadEnd);
                cdcControlTaskEndLoad.Properties["StateConnection"].SetValue(cdcControlTaskEndLoad, destinationManager.ID);
                cdcControlTaskEndLoad.Properties["StateVariable"].SetValue(cdcControlTaskEndLoad, "User::CDC_State_" + table);
                cdcControlTaskEndLoad.Properties["AutomaticStatePersistence"].SetValue(cdcControlTaskEndLoad, true);
                cdcControlTaskEndLoad.Properties["StateName"].SetValue(cdcControlTaskEndLoad, "CDC_State_" + table);
                cdcControlTaskEndLoad.Properties["StateTable"].SetValue(cdcControlTaskEndLoad, "[dbo].[cdc_states]");
                cdcControlTaskEndLoad.DelayValidation = true;

                // Configure precedence
                (package.PrecedenceConstraints.Add((Executable)createCDCStateTable, (Executable)createDestinationTable)).Value = DTSExecResult.Success;
                PrecedenceConstraint pcaddCDCStateTableStartLoad = package.PrecedenceConstraints.Add((Executable)createDestinationTable, (Executable)cdcControlTaskStartLoad);
                pcaddCDCStateTableStartLoad.Value = DTSExecResult.Success;
                PrecedenceConstraint pcStartLoadDataFlow = package.PrecedenceConstraints.Add((Executable)cdcControlTaskStartLoad, (Executable)dataFlowTask);
                pcStartLoadDataFlow.Value = DTSExecResult.Success;
                PrecedenceConstraint pcEndLoadDataFlow = package.PrecedenceConstraints.Add((Executable)dataFlowTask, (Executable)cdcControlTaskEndLoad);
                pcEndLoadDataFlow.Value = DTSExecResult.Success;

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
