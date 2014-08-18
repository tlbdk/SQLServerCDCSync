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
        public static string GenerateMergeSQL (string name, string sourcetable, string destinationtable, string[] colums)
        {
            return null;
        }

        public static bool GenerateInitialLoadSSISPackage(string filename, string sourceconn, string destinationconn, string table)
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

            // Add variables
            package.Variables.Add("CDC_State", false, "User", "");

            // Create destination Table from source table definiation
            TaskHost createDestinationTable = package.Executables.Add("STOCK:SQLTask") as TaskHost;
            createDestinationTable.Name = "Create destination table from source table definiation";
            createDestinationTable.Properties["Connection"].SetValue(createDestinationTable, destinationManager.ID);
            createDestinationTable.Properties["SqlStatementSource"].SetValue(createDestinationTable, String.Format("SELECT * INTO [dbo].[{1}] FROM [{0}].[dbo].[{1}] WHERE 1 = 2;", sourcedbname, table));

            // Add CDC State Table
            TaskHost addCDCStateTable = package.Executables.Add("STOCK:SQLTask") as TaskHost;
            addCDCStateTable.Name = "Add CDC State Table";
            addCDCStateTable.Properties["Connection"].SetValue(addCDCStateTable, destinationManager.ID);
            addCDCStateTable.Properties["SqlStatementSource"].SetValue(addCDCStateTable, 
                "CREATE TABLE [dbo].[cdc_states] ([name] [nvarchar](256) NOT NULL, [state] [nvarchar](256) NOT NULL) ON [PRIMARY];" + 
                "CREATE UNIQUE NONCLUSTERED INDEX [cdc_states_name] ON [dbo].[cdc_states] ( [name] ASC ) WITH (PAD_INDEX  = OFF) ON [PRIMARY];"
            );

            // Add copy table task
            TaskHost dataFlowTask = package.Executables.Add("STOCK:PipelineTask") as TaskHost;
            dataFlowTask.Name = "Copy source to destination";
            dataFlowTask.DelayValidation = true;
            MainPipe dataFlowTaskPipe = (MainPipe)dataFlowTask.InnerObject;

            // Add CDC Initial load start
            TaskHost cdcControlTaskStartLoad = package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
            cdcControlTaskStartLoad.Name = "Mark initial load start";
            cdcControlTaskStartLoad.Properties["Connection"].SetValue(cdcControlTaskStartLoad, sourceManager.ID);
            cdcControlTaskStartLoad.Properties["TaskOperation"].SetValue(cdcControlTaskStartLoad, CdcControlTaskOperation.MarkInitialLoadStart);
            cdcControlTaskStartLoad.Properties["StateConnection"].SetValue(cdcControlTaskStartLoad, destinationManager.ID);
            cdcControlTaskStartLoad.Properties["StateVariable"].SetValue(cdcControlTaskStartLoad, "User::CDC_State");
            cdcControlTaskStartLoad.Properties["AutomaticStatePersistence"].SetValue(cdcControlTaskStartLoad, true);
            cdcControlTaskStartLoad.Properties["StateName"].SetValue(cdcControlTaskStartLoad, table + "_State");
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

            // Make sure we truncate before we run the dataflow
            PrecedenceConstraint pcaddCDCStateTableStartLoad = package.PrecedenceConstraints.Add((Executable)createDestinationTable, (Executable)addCDCStateTable);
            pcaddCDCStateTableStartLoad.Value = DTSExecResult.Success;

            // Make sure we truncate before we run the dataflow
            PrecedenceConstraint pcTruncatecStartLoad = package.PrecedenceConstraints.Add((Executable)addCDCStateTable, (Executable)cdcControlTaskStartLoad);
            pcTruncatecStartLoad.Value = DTSExecResult.Success;

            // Make sure we truncate before we run the dataflow
            PrecedenceConstraint pcStartLoadDataFlow = package.PrecedenceConstraints.Add((Executable)cdcControlTaskStartLoad, (Executable)dataFlowTask);
            pcStartLoadDataFlow.Value = DTSExecResult.Success;

            // Make sure we truncate before we run the dataflow
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
            adonetsrcinstance.SetComponentProperty("TableOrViewName", "\"dbo\".\"Test1\"");
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
            adonetdstinstance.SetComponentProperty("TableOrViewName", "\"dbo\".\"Test1\"");
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

            app.SaveToXml(filename, package, null);

            return true;
        }

    }
}
