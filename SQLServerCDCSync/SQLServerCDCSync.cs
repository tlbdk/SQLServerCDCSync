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
using System.Data.Common;
using System.Data.OleDb;
using System.Configuration;
using System.IO;
using System.Reflection;
using System.Diagnostics;
using SQLServerCDCSync.SSISUtils;

// Links : EzAPI
// http://social.msdn.microsoft.com/Forums/sqlserver/en-US/eb8b10bc-963c-4d36-8ea2-6c3ebbc20411/copying-600-tables-in-ssis-how-to?forum=sqlintegrationservices
// http://www.experts-exchange.com/Database/MS-SQL-Server/Q_23972361.html
// Fix permissions:
// http://www.mssqltips.com/sqlservertip/3086/how-to-resolve-ssis-access-denied-error-in-sql-server-management-studio/

// Problem with having the CDC Controls in a Sequence container:
// http://social.technet.microsoft.com/Forums/systemcenter/en-US/421b126b-1729-464f-aaa3-1a2175c09e28/cdc-control-task-timeout-expired-when-transaction-is-required 

// SSIS transaction might not be the best option
// http://www.mattmasson.com/2011/12/design-pattern-avoiding-transactions/

// Error codes
// http://technet.microsoft.com/es-uy/library/ms345164(v=sql.90).aspx

namespace SQLServerCDCSync
{
    class SQLServerCDCSync
    {
        public String SourceConnection;
        public String SourceConnectionProvider;
        public String DestinationConnection;
        public String DestinationConnectionProvider;
        public String CDCDatabaseConnection = null;
        public String CDCStateConnection = null;
        public String ErrorLogPath;
        public String[] Tables;

        private String DestinationDatabaseName;

        public SQLServerCDCSync()
        {

        }

        public SQLServerCDCSync(ConnectionStringSettings sourceconn, ConnectionStringSettings destinationconn, ConnectionStringSettings cdcdbconn, ConnectionStringSettings cdcstateconn, string[] tables)
        {
            this.SourceConnection = sourceconn.ConnectionString;
            this.SourceConnectionProvider = sourceconn.ProviderName;
            this.DestinationConnection = destinationconn.ConnectionString;
            this.DestinationConnectionProvider = destinationconn.ProviderName;
            if (cdcdbconn != null) {
                this.CDCDatabaseConnection = cdcdbconn.ConnectionString;
                this.CDCStateConnection = cdcstateconn.ConnectionString;
            }
            this.Tables = tables;
            this.ErrorLogPath = Path.GetFullPath(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));

            // TODO: Verify that the CDCDatabaseConnection and the DestinationConnection is on the same server
            // TODO: Verify that source and destination provider is of same type

            if (this.SourceConnectionProvider.StartsWith("Oracle"))
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


            // Get name of destination database
            var destinationConnnection = CreateDBConnection(this.DestinationConnection, this.DestinationConnectionProvider);
            destinationConnnection.Open();
            this.DestinationDatabaseName = GetSQLString(destinationConnnection, "SELECT db_name()");
            destinationConnnection.Close();
        }

        public Package GenerateInitialLoadSSISPackageSimple()
        {
            var ssis = new SSISPackage("Initial Load Package for " + this.DestinationDatabaseName);

            var sourceManager = ssis.AddConnectionManager("Source Connection", this.SourceConnection, this.SourceConnectionProvider);
            var destinationManager = ssis.AddConnectionManager("Destination Connection", this.DestinationConnection, this.DestinationConnectionProvider);

            // Get table list if we have not defined it first
            var destinationConnnection = CreateDBConnection(this.DestinationConnection, this.DestinationConnectionProvider);
            destinationConnnection.Open();
            if (this.Tables.Length == 0)
            {
                this.Tables = GetSQLTableList(destinationConnnection, "WHERE s.name != 'cdc' and t.is_ms_shipped = 0").Keys.ToArray();
            }
            
            // Add CDC State Table
            var createStateTable = ssis.AddSQLTask("Create load_states table", destinationManager,
                "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='load_states' and xtype='U') BEGIN\n" +
                    "CREATE TABLE [dbo].[load_states] ([name] [nvarchar](256) NOT NULL, [start] [datetime2] NOT NULL, [end] [datetime2] NULL) ON [PRIMARY];\n" +
                    "CREATE UNIQUE NONCLUSTERED INDEX [load_states_name] ON [dbo].[load_states] ( [name] ASC ) WITH (PAD_INDEX  = OFF) ON [PRIMARY];\n" +
                "END;"
            );

            // Insert start time //TODO: Get the timestamp from the source db to be sure it's correct
            var insertStart = ssis.AddSQLTask("Insert start time", destinationManager, "INSERT INTO load_states ([name], [start], [end]) VALUES ('all', SYSUTCDATETIME(), NULL)\n");
            // Update end time
            var updateEnd = ssis.AddSQLTask("Update end time", destinationManager, "UPDATE load_states SET [end] = SYSUTCDATETIME() WHERE [name] = 'all'");

            // Configure precedence
            ssis.AddConstraint(createStateTable, insertStart, DTSExecResult.Success);

            foreach (var table in this.Tables)
            {
                var tinfo = new SQLTableInfo(destinationConnnection, table);
                // Skip table if it's load_states
                if (table == "load_states") continue;

                // Disable FastLoadKeepNulls if a colum is non nullable, is unsupported and has a default value set // TODO: Move this into AddDataFlow
                var DisableFastLoadKeepNulls = tinfo.Colums.Any(x => !tinfo.NullableColums.Contains(x) && tinfo.UnsupportedTypes.ContainsKey(x) && tinfo.DefaultValueColums.ContainsKey(x));

                Console.WriteLine("Generating initial load for table " + table);
                TaskHost dataFlowTask = AddDataFlow(ssis.app, ssis.package, table, table, sourceManager, destinationManager, null, new HashSet<String>(tinfo.UnsupportedTypes.Keys), DisableFastLoadKeepNulls);

                // Configure precedence
                ssis.AddConstraint(insertStart, dataFlowTask, DTSExecResult.Success);
                ssis.AddConstraint(dataFlowTask, updateEnd, DTSExecResult.Success);
            }

            destinationConnnection.Close();

            return ssis.package;
        }

        private TaskHost AddDataFlow(Application app, Package package, string sourcetable, string destinationtable, ConnectionManager sourceManager, ConnectionManager destinationManager, ConnectionManager fakeDestinationManager = null, HashSet<String> ignoreColums = null, bool disableFastLoadNulls = false)
        {
            TaskHost dataFlowTask = package.Executables.Add("STOCK:PipelineTask") as TaskHost;
            dataFlowTask.Name = "Copy source table "+ sourcetable + " to destination";
            dataFlowTask.DelayValidation = true;
            MainPipe dataFlowTaskPipe = (MainPipe)dataFlowTask.InnerObject;
            // TODO Tweak these by finding the avg row size: "DB2: select avgrowsize from syscat.tables where tabschema = 'YOURSCHEMA' and tabname = 'YOURTABLE'"
            //dataFlowTaskPipe.DefaultBufferMaxRows = 1000;
            dataFlowTaskPipe.DefaultBufferSize *= 8;

            // Configure the source
            IDTSComponentMetaData100 adonetsrc = dataFlowTaskPipe.ComponentMetaDataCollection.New();
            adonetsrc.ValidateExternalMetadata = true;
            if (sourceManager.CreationName.StartsWith("ADO.NET"))
            {
                adonetsrc.Name = "ADO NET Source";
                adonetsrc.ComponentClassID = app.PipelineComponentInfos["ADO NET Source"].CreationName;
            }
            else if (sourceManager.CreationName.StartsWith("OLEDB"))
            {
                adonetsrc.Name = "OLE DB Source";
                adonetsrc.ComponentClassID = app.PipelineComponentInfos["OLE DB Source"].CreationName;
            }
            IDTSDesigntimeComponent100 adonetsrcinstance = adonetsrc.Instantiate();
            adonetsrcinstance.ProvideComponentProperties();
            adonetsrcinstance.SetComponentProperty("CommandTimeout", 300);
            if (sourceManager.CreationName.StartsWith("ADO.NET"))
            {
                adonetsrcinstance.SetComponentProperty("AccessMode", 0);
                adonetsrcinstance.SetComponentProperty("TableOrViewName", sourcetable);
            }
            else if (sourceManager.CreationName.StartsWith("OLEDB"))
            {
                adonetsrcinstance.SetComponentProperty("AccessMode", 0);
                adonetsrcinstance.SetComponentProperty("OpenRowset", sourcetable);
                //adonetsrcinstance.SetComponentProperty("FastLoadOptions", "ROWS_PER_BATCH = 10000");
            }
            adonetsrc.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(sourceManager);
            adonetsrc.RuntimeConnectionCollection[0].ConnectionManagerID = sourceManager.ID;
            adonetsrcinstance.AcquireConnections(null);
            adonetsrcinstance.ReinitializeMetaData();
            adonetsrcinstance.ReleaseConnections();

            // Configure the destination
            IDTSComponentMetaData100 adonetdst = dataFlowTaskPipe.ComponentMetaDataCollection.New();
            adonetdst.ValidateExternalMetadata = true;
            if (destinationManager.CreationName.StartsWith("ADO.NET"))
            {
                adonetdst.Name = "ADO NET Destination";
                adonetdst.ComponentClassID = app.PipelineComponentInfos["ADO NET Destination"].CreationName;
            }
            else if (destinationManager.CreationName.StartsWith("OLEDB"))
            {
                adonetdst.Name = "OLE DB Destination";
                adonetdst.ComponentClassID = app.PipelineComponentInfos["OLE DB Destination"].CreationName;
            }
            IDTSDesigntimeComponent100 adonetdstinstance = adonetdst.Instantiate();
            adonetdstinstance.ProvideComponentProperties();
            adonetdstinstance.SetComponentProperty("CommandTimeout", 300);
            if (destinationManager.CreationName.StartsWith("ADO.NET"))
            {
                adonetdstinstance.SetComponentProperty("TableOrViewName", destinationtable);
            }
            else if (destinationManager.CreationName.StartsWith("OLEDB"))
            {
                adonetdstinstance.SetComponentProperty("FastLoadKeepNulls", disableFastLoadNulls ? false : true);
                adonetdstinstance.SetComponentProperty("FastLoadKeepIdentity", true);
                adonetdstinstance.SetComponentProperty("AccessMode", 3);
                adonetdstinstance.SetComponentProperty("OpenRowset", destinationtable);
            }

            // Point to the CDC tables when generating the metadata
            if (fakeDestinationManager != null) {
                adonetdst.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(fakeDestinationManager);
                adonetdst.RuntimeConnectionCollection[0].ConnectionManagerID = fakeDestinationManager.ID;
            }
            else
            {
                adonetdst.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(destinationManager);
                adonetdst.RuntimeConnectionCollection[0].ConnectionManagerID = destinationManager.ID;
            }
            adonetdstinstance.AcquireConnections(null);
            adonetdstinstance.ReinitializeMetaData();
            adonetdstinstance.ReleaseConnections();

            // Attach the path from data flow source to destination
            IDTSPath100 path = dataFlowTaskPipe.PathCollection.New();
            path.AttachPathAndPropagateNotifications(adonetsrc.OutputCollection[0], adonetdst.InputCollection[0]);

            // Do column mapping on the destination
            IDTSInput100 destInput = adonetdst.InputCollection[0];
            IDTSExternalMetadataColumnCollection100 externalColumnCollection = destInput.ExternalMetadataColumnCollection;
            IDTSVirtualInput100 destVirInput = destInput.GetVirtualInput();
            IDTSInputColumnCollection100 destInputCols = destInput.InputColumnCollection;
            IDTSExternalMetadataColumnCollection100 destExtCols = destInput.ExternalMetadataColumnCollection;
            IDTSOutputColumnCollection100 sourceColumns = adonetsrc.OutputCollection[0].OutputColumnCollection;

            destInput.ErrorRowDisposition = DTSRowDisposition.RD_RedirectRow;
            //destInput.TruncationRowDisposition = DTSRowDisposition.RD_RedirectRow; // Does not like an option in the GUI

            // Hook up the external columns
            var removeColumns = new List<int>();
            foreach (IDTSOutputColumn100 outputCol in sourceColumns)
            {
                // Add ignore colums to ignore list
                if (ignoreColums.Contains(outputCol.Name))
                {
                    Console.WriteLine("Ignoring column " + outputCol.Name);
                    removeColumns.Add(outputCol.ID);
                    continue;
                }

                // Ignore all errors
                outputCol.ErrorRowDisposition = DTSRowDisposition.RD_RedirectRow;
                outputCol.TruncationRowDisposition = DTSRowDisposition.RD_RedirectRow;

                // Build list for debugging, TODO: Move this some code before where we check that colums are as they should be
                var destExtColsNames = new HashSet<String>();
                foreach (IDTSExternalMetadataColumn100 test in destExtCols)
                {
                    destExtColsNames.Add(test.Name);
                }

                if (!destExtColsNames.Contains(outputCol.Name))
                {
                    throw new Exception("Table " + sourcetable + " has new colum " + outputCol.Name);
                }

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
                        //Debug.WriteLine(inputCol.Name + ": " + inputCol.LineageID);
                    }
                }
            }

            // Remove columns that are ignored
            foreach (var id in removeColumns)
            {
                sourceColumns.RemoveObjectByID(id);
            }

            // Point to the destination table
            adonetdst.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(destinationManager);
            adonetdst.RuntimeConnectionCollection[0].ConnectionManagerID = destinationManager.ID;
            if (destinationManager.CreationName.StartsWith("ADO.NET"))
            {
                adonetdstinstance.SetComponentProperty("TableOrViewName", sourcetable);
            }
            else if (destinationManager.CreationName.StartsWith("OLEDB"))
            {
                adonetdstinstance.SetComponentProperty("OpenRowset", sourcetable);
            }

            //
            // Create Error output for source
            // 
            ConnectionManager errorfilesourceManager = package.Connections.Add("FLATFILE");
            errorfilesourceManager.ConnectionString = Path.Combine(this.ErrorLogPath, "source-errors-" + sourcetable + ".txt");
            errorfilesourceManager.Name = "Error output for source table " + sourcetable;
            errorfilesourceManager.Properties["Format"].SetValue(errorfilesourceManager, "Delimited");
            //errorfilesourceManager.Properties["CodePage"].SetValue(errorfilesourceManager, "65001");
            errorfilesourceManager.Properties["Unicode"].SetValue(errorfilesourceManager, true);
            errorfilesourceManager.Properties["ColumnNamesInFirstDataRow"].SetValue(errorfilesourceManager, true);
            var errorfilesourceManagerInstance = errorfilesourceManager.InnerObject as Microsoft.SqlServer.Dts.Runtime.Wrapper.IDTSConnectionManagerFlatFile100;

            // Add Flat File destination
            IDTSComponentMetaData100 adonetsrcerror = dataFlowTaskPipe.ComponentMetaDataCollection.New();
            adonetsrcerror.Name = "Source Error File";
            adonetsrcerror.ComponentClassID = app.PipelineComponentInfos["Flat File Destination"].CreationName;
            IDTSDesigntimeComponent100 adonetsrcerrorinstance = adonetsrcerror.Instantiate();
            adonetsrcerrorinstance.ProvideComponentProperties();
            adonetsrcerror.RuntimeConnectionCollection[0].ConnectionManagerID = errorfilesourceManager.ID;
            adonetsrcerror.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(errorfilesourceManager);

            //TODO http://www.sqlis.com/sqlis/post/Creating-packages-in-code-OLE-DB-Source-to-Flat-File-File-Destination.aspx

            // Create flat file connection columns to match adonetsrc error columns
            IDTSOutputColumnCollection100 sourceErrorColumns = adonetsrc.OutputCollection[1].OutputColumnCollection;

            int indexMax = sourceErrorColumns.Count - 1;
            for (int index = 0; index <= indexMax; index++)
            {
                // Get input column to replicate in flat file
                var errorColumn = sourceErrorColumns[index];

                // Add column to Flat File connection manager
                var flatFileColumn = errorfilesourceManagerInstance.Columns.Add() as Microsoft.SqlServer.Dts.Runtime.Wrapper.IDTSConnectionManagerFlatFileColumn100;
                flatFileColumn.ColumnType = "Delimited";
                flatFileColumn.ColumnWidth = errorColumn.Length;
                flatFileColumn.DataPrecision = errorColumn.Precision;
                flatFileColumn.DataScale = errorColumn.Scale;
                flatFileColumn.DataType = errorColumn.DataType;
                var columnName = flatFileColumn as Microsoft.SqlServer.Dts.Runtime.Wrapper.IDTSName100;
                columnName.Name = errorColumn.Name;

                if (index < indexMax)
                {
                    flatFileColumn.ColumnDelimiter = ",";
                }
                else
                {
                    flatFileColumn.ColumnDelimiter = Environment.NewLine;
                }
            }

            // Reinitialize the metadata, generating external columns from flat file columns
            adonetsrcerrorinstance.AcquireConnections(null);
            adonetsrcerrorinstance.ReinitializeMetaData();
            adonetsrcerrorinstance.ReleaseConnections();

            // Attach the path from data flow source to destination
            IDTSPath100 srcerrorpath = dataFlowTaskPipe.PathCollection.New();
            srcerrorpath.AttachPathAndPropagateNotifications(adonetsrc.OutputCollection[1], adonetsrcerror.InputCollection[0]);

            // Hook up the external columns and map the error colums
            var removeErrorColumns = new List<int>();
            foreach (IDTSOutputColumn100 outputCol in sourceErrorColumns)
            {
                // Ignore colums we don't support
                if (ignoreColums.Contains(outputCol.Name))
                {
                    removeErrorColumns.Add(outputCol.ID);
                    continue;
                }

                // Get the external column id
                IDTSExternalMetadataColumn100 extCol = (IDTSExternalMetadataColumn100)adonetsrcerror.InputCollection[0].ExternalMetadataColumnCollection[outputCol.Name];
                if (extCol != null)
                {
                    // Create an input column from an output col of previous component.
                    adonetsrcerror.InputCollection[0].GetVirtualInput().SetUsageType(outputCol.ID, DTSUsageType.UT_READONLY);
                    IDTSInputColumn100 inputCol = adonetsrcerror.InputCollection[0].InputColumnCollection.GetInputColumnByLineageID(outputCol.ID);
                    if (inputCol != null)
                    {
                        // map the input column with an external metadata column
                        adonetsrcerrorinstance.MapInputColumn(adonetsrcerror.InputCollection[0].ID, inputCol.ID, extCol.ID);
                    }
                }
            }

            // Remove columns that are ignored
            foreach (var id in removeErrorColumns)
            {
                sourceErrorColumns.RemoveObjectByID(id);
            }

            //
            // Create Error output for destination
            // 
            ConnectionManager errorfiledestinationManager = package.Connections.Add("FLATFILE");
            errorfiledestinationManager.ConnectionString = Path.Combine(this.ErrorLogPath, "destination-errors-" + sourcetable + ".txt");
            errorfiledestinationManager.Name = "Error output for destination table " + sourcetable;
            errorfiledestinationManager.Properties["Format"].SetValue(errorfiledestinationManager, "Delimited");
            //errorfiledestinationManager.Properties["CodePage"].SetValue(errorfiledestinationManager, "65001");
            errorfiledestinationManager.Properties["Unicode"].SetValue(errorfiledestinationManager, true);
            errorfiledestinationManager.Properties["ColumnNamesInFirstDataRow"].SetValue(errorfiledestinationManager, true);
            var errorfiledestinationManagerInstance = errorfiledestinationManager.InnerObject as Microsoft.SqlServer.Dts.Runtime.Wrapper.IDTSConnectionManagerFlatFile100;

            // Add Flat File destination
            IDTSComponentMetaData100 adonetdsterror = dataFlowTaskPipe.ComponentMetaDataCollection.New();
            adonetdsterror.Name = "Destination Error File";
            adonetdsterror.ComponentClassID = app.PipelineComponentInfos["Flat File Destination"].CreationName;
            IDTSDesigntimeComponent100 adonetdsterrorinstance = adonetdsterror.Instantiate();
            adonetdsterrorinstance.ProvideComponentProperties();
            adonetdsterror.RuntimeConnectionCollection[0].ConnectionManagerID = errorfiledestinationManager.ID;
            adonetdsterror.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(errorfiledestinationManager);

            // Add all the input colums as they don't exists for a destination for some reason
            foreach (IDTSExternalMetadataColumn100 errorColumn in adonetdst.InputCollection[0].ExternalMetadataColumnCollection)
            {
                // Add column to Flat File connection manager
                var flatFileColumn = errorfiledestinationManagerInstance.Columns.Add() as Microsoft.SqlServer.Dts.Runtime.Wrapper.IDTSConnectionManagerFlatFileColumn100;
                flatFileColumn.ColumnType = "Delimited";
                flatFileColumn.ColumnWidth = errorColumn.Length;
                flatFileColumn.DataPrecision = errorColumn.Precision;
                flatFileColumn.DataScale = errorColumn.Scale;
                flatFileColumn.DataType = errorColumn.DataType;
                var columnName = flatFileColumn as Microsoft.SqlServer.Dts.Runtime.Wrapper.IDTSName100;
                columnName.Name = errorColumn.Name;
                flatFileColumn.ColumnDelimiter = ",";
            }

            // Create flat file connection columns to match adonetsrc error columns
            IDTSOutputColumnCollection100 destinationErrorColumns = adonetdst.OutputCollection[0].OutputColumnCollection;
            indexMax = destinationErrorColumns.Count - 1;
            for (int index = 0; index <= indexMax; index++)
            {
                // Get input column to replicate in flat file
                var errorColumn = destinationErrorColumns[index];

                // Add column to Flat File connection manager
                var flatFileColumn = errorfiledestinationManagerInstance.Columns.Add() as Microsoft.SqlServer.Dts.Runtime.Wrapper.IDTSConnectionManagerFlatFileColumn100;
                flatFileColumn.ColumnType = "Delimited";
                flatFileColumn.ColumnWidth = errorColumn.Length;
                flatFileColumn.DataPrecision = errorColumn.Precision;
                flatFileColumn.DataScale = errorColumn.Scale;
                flatFileColumn.DataType = errorColumn.DataType;
                var columnName = flatFileColumn as Microsoft.SqlServer.Dts.Runtime.Wrapper.IDTSName100;
                columnName.Name = errorColumn.Name;

                if (index < indexMax)
                {
                    flatFileColumn.ColumnDelimiter = ",";
                }
                else
                {
                    flatFileColumn.ColumnDelimiter = Environment.NewLine;
                }
            }

            // Reinitialize the metadata, generating external columns from flat file columns
            adonetdsterrorinstance.AcquireConnections(null);
            adonetdsterrorinstance.ReinitializeMetaData();
            adonetdsterrorinstance.ReleaseConnections();

            // Attach the path from data flow source to destination
            IDTSPath100 dsterrorpath = dataFlowTaskPipe.PathCollection.New();
            dsterrorpath.AttachPathAndPropagateNotifications(adonetdst.OutputCollection[0], adonetdsterror.InputCollection[0]);

            // Hook up the external columns and map the source colums to the error input
            foreach (IDTSOutputColumn100 outputCol in sourceColumns)
            {
                // Get the external column id
                IDTSExternalMetadataColumn100 extCol = (IDTSExternalMetadataColumn100)adonetdsterror.InputCollection[0].ExternalMetadataColumnCollection[outputCol.Name];
                if (extCol != null)
                {
                    // Create an input column from an output col of previous component.
                    adonetdsterror.InputCollection[0].GetVirtualInput().SetUsageType(outputCol.ID, DTSUsageType.UT_READONLY);
                    IDTSInputColumn100 inputCol = adonetdsterror.InputCollection[0].InputColumnCollection.GetInputColumnByLineageID(outputCol.ID);
                    if (inputCol != null)
                    {
                        // map the input column with an external metadata column
                        adonetdsterrorinstance.MapInputColumn(adonetdsterror.InputCollection[0].ID, inputCol.ID, extCol.ID);
                    }
                }
            }

            // Hook up the external columns and map the error colums to the error input
            foreach (IDTSOutputColumn100 outputCol in destinationErrorColumns)
            {
                // Get the external column id
                IDTSExternalMetadataColumn100 extCol = (IDTSExternalMetadataColumn100)adonetdsterror.InputCollection[0].ExternalMetadataColumnCollection[outputCol.Name];
                if (extCol != null)
                {
                    // Create an input column from an output col of previous component.
                    adonetdsterror.InputCollection[0].GetVirtualInput().SetUsageType(outputCol.ID, DTSUsageType.UT_READONLY);
                    IDTSInputColumn100 inputCol = adonetdsterror.InputCollection[0].InputColumnCollection.GetInputColumnByLineageID(outputCol.ID);
                    if (inputCol != null)
                    {
                        // map the input column with an external metadata column
                        adonetdsterrorinstance.MapInputColumn(adonetdsterror.InputCollection[0].ID, inputCol.ID, extCol.ID);
                    }
                }
            }

            return dataFlowTask;
        }


        public Package GenerateInitialLoadSSISPackageCDC()
        {
            var ssis = new SSISPackage("CDC Initial Load Package for " + this.DestinationDatabaseName);

            // Create connection managers and add them to the package
            var cdcConnManager = ssis.AddConnectionManager("ADO.NET CDC Database Connection", this.CDCDatabaseConnection, "System.Data.SqlClient");
            var cdcStateManager = ssis.AddConnectionManager("ADO.NET CDC State", this.CDCStateConnection, "System.Data.SqlClient");
            var sourceManager = ssis.AddConnectionManager("Source Connection", this.SourceConnection, this.SourceConnectionProvider);
            var destinationManager = ssis.AddConnectionManager("Destination Connection", this.DestinationConnection, this.DestinationConnectionProvider);

            // Create normal SQL connection to the CDC database so we can use it to get some table information
            var cdcConnnection = new SqlConnection(this.CDCDatabaseConnection);
            cdcConnnection.Open();

            // Get the cdc Database name
            var cdcdatabase = GetSQLString(cdcConnnection, "SELECT db_name()");

            // Create a manager that points to the CDC database so we can update the metadata even when we don't have any tables yet
            var fakeDestinationManager = ssis.AddConnectionManager("Fake Destination Connection", this.DestinationConnection, this.DestinationConnectionProvider, cdcdatabase);

            // Get CDC tables from database
            var cdctables = GetSQLTableList(cdcConnnection, "WHERE s.name != 'cdc' and t.is_ms_shipped = 0 and t.is_tracked_by_cdc = 1");
            if (cdctables.Count == 0)
            {
                throw new Exception("Did not find any tables in the CDC Database");
            }

            if (this.Tables.Length == 0)
            {
                this.Tables = cdctables.Keys.ToArray();
            }

            // Add CDC State Table
            var createCDCStateTable = ssis.AddSQLTask("Create CDC state table if it does not exist", destinationManager,
                "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='cdc_states' and xtype='U') BEGIN\n" +
                    "CREATE TABLE [dbo].[cdc_states] ([name] [nvarchar](256) NOT NULL, [state] [nvarchar](256) NOT NULL) ON [PRIMARY];\n" +
                    "CREATE UNIQUE NONCLUSTERED INDEX [cdc_states_name] ON [dbo].[cdc_states] ( [name] ASC ) WITH (PAD_INDEX  = OFF) ON [PRIMARY];\n" +
                "END;"
            );

            // Add CDC status Table
            TaskHost createCDCMergeStatusTable = ssis.AddSQLTask("Create CDC status table if it does not exist", destinationManager,
                "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='cdc_status' and xtype='U') BEGIN\n" +
                    "CREATE TABLE [dbo].[cdc_status] (" +
                        "[id] [int] IDENTITY(1,1) NOT NULL, " +
                        "[name] [nvarchar](256) NOT NULL, " +
                        "[rowcount] [int] NOT NULL, " +
                        "[elapsed] [int] NOT NULL, " +
                        "[created] [datetime] default CURRENT_TIMESTAMP, " +
                        "[cdcstate] [nvarchar](256), " +
                        "[error] [nvarchar](256), " +
                    "CONSTRAINT [PK_cdc_status] PRIMARY KEY CLUSTERED ([id] ASC) ON [PRIMARY]) ON [PRIMARY];\n" +
                "END\n"
            );

            // Copy CDC State variable from the first table if it does not exists
            TaskHost copyCDCState = ssis.AddSQLTask("Copy the CDC State of the first table to start if the CDC_State does not exist", destinationManager,
                "IF NOT EXISTS (SELECT TOP 1 state FROM cdc_states WHERE name = 'CDC_State') BEGIN\n" +
                   "INSERT INTO cdc_states(name, state)\n" +
                   "SELECT TOP 1 'CDC_State' name, state FROM cdc_states ORDER BY state ASC\n" + // Make sure we don't need to use subselect to get the list ordered
                "END;\n"
            );

            foreach (var table in this.Tables)
            {
                Console.WriteLine("Generating initial load for table " + table);
                var tinfo = new SQLTableInfo(cdcConnnection, table);

                // Add variables
                ssis.package.Variables.Add("CDC_State_" + table, false, "User", "");
                ssis.package.Variables.Add("Create" + table + "_Result", false, "User", "");

                // Create destination Table from source table definiation
                TaskHost createDestinationTable = ssis.AddSQLTask("Create destination " + table + " from source table definiation", destinationManager,
                    "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='" + table + "' and xtype='U') BEGIN\n" +
                        "SELECT * INTO [dbo].[" + table + "] FROM [" + cdcdatabase + "]." + cdctables[table] + " WHERE 1 = 2;\n" +
                        "SELECT 'NOTEXISTS' AS Result;\n" +
                    "END\n" +
                    "ELSE BEGIN\n" +
                        "SELECT 'EXISTS' AS Result;\n" +
                    "END\n"
                );

                // Add result set binding for createDestinationTable
                var createDestinationTableTask = createDestinationTable.InnerObject as ExecuteSQLTask;
                createDestinationTableTask.ResultSetBindings.Add();
                createDestinationTableTask.ResultSetType = ResultSetType.ResultSetType_SingleRow;
                IDTSResultBinding resultBinding = createDestinationTableTask.ResultSetBindings.GetBinding(0);
                resultBinding.ResultName = "0";
                resultBinding.DtsVariableName = "User::Create" + table + "_Result";

                // TODO: Implement support for tables without UNID
                if (tinfo.CreatePrimaryKeySQL == null)
                {
                    throw new Exception("Table " + table + " does not have any unique keys");
                }

                // Create a index from the primary key
                TaskHost createIndexes = ssis.AddSQLTask("Create indexes on destination table " + table, destinationManager,
                    tinfo.CreatePrimaryKeySQL
                );

                // Add CDC Initial load start
                TaskHost cdcMarkStartInitialLoad = ssis.package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
                cdcMarkStartInitialLoad.Name = "Mark initial load start for table " + table;
                cdcMarkStartInitialLoad.Properties["Connection"].SetValue(cdcMarkStartInitialLoad, cdcConnManager.ID);
                cdcMarkStartInitialLoad.Properties["StateConnection"].SetValue(cdcMarkStartInitialLoad, cdcStateManager.ID);
                cdcMarkStartInitialLoad.Properties["TaskOperation"].SetValue(cdcMarkStartInitialLoad, CdcControlTaskOperation.MarkInitialLoadStart);
                cdcMarkStartInitialLoad.Properties["StateVariable"].SetValue(cdcMarkStartInitialLoad, "User::CDC_State_" + table);
                cdcMarkStartInitialLoad.Properties["AutomaticStatePersistence"].SetValue(cdcMarkStartInitialLoad, true);
                cdcMarkStartInitialLoad.Properties["StateName"].SetValue(cdcMarkStartInitialLoad, "CDC_State_" + table);
                cdcMarkStartInitialLoad.Properties["StateTable"].SetValue(cdcMarkStartInitialLoad, "[dbo].[cdc_states]");
                cdcMarkStartInitialLoad.DelayValidation = true;

                // Add CDC Initial load end
                TaskHost cdcMarkEndInitialLoad = ssis.package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
                cdcMarkEndInitialLoad.Name = "Mark initial load end for table " + table;
                cdcMarkEndInitialLoad.Properties["Connection"].SetValue(cdcMarkEndInitialLoad, cdcConnManager.ID);
                cdcMarkEndInitialLoad.Properties["StateConnection"].SetValue(cdcMarkEndInitialLoad, cdcStateManager.ID);
                cdcMarkEndInitialLoad.Properties["TaskOperation"].SetValue(cdcMarkEndInitialLoad, CdcControlTaskOperation.MarkInitialLoadEnd);
                cdcMarkEndInitialLoad.Properties["StateVariable"].SetValue(cdcMarkEndInitialLoad, "User::CDC_State_" + table);
                cdcMarkEndInitialLoad.Properties["AutomaticStatePersistence"].SetValue(cdcMarkEndInitialLoad, true);
                cdcMarkEndInitialLoad.Properties["StateName"].SetValue(cdcMarkEndInitialLoad, "CDC_State_" + table);
                cdcMarkEndInitialLoad.Properties["StateTable"].SetValue(cdcMarkEndInitialLoad, "[dbo].[cdc_states]");
                cdcMarkEndInitialLoad.DelayValidation = true;

                // Add copy table task
                // Remove database types we don't support: http://msdn.microsoft.com/en-us/library/dn175470.aspx
                TaskHost dataFlowTask = this.AddDataFlow(ssis.app, ssis.package, table, cdctables[table], sourceManager, destinationManager, fakeDestinationManager, new HashSet<String>(tinfo.UnsupportedTypes.Keys));

                // Configure precedence
                ssis.AddConstraint(createCDCStateTable, createDestinationTable, DTSExecResult.Success);
                ssis.AddConstraint(createCDCMergeStatusTable, createDestinationTable, DTSExecResult.Success);
                var precedence = ssis.AddConstraint(createDestinationTable, cdcMarkStartInitialLoad, DTSExecResult.Success);
                precedence.EvalOp = DTSPrecedenceEvalOp.ExpressionAndConstraint;
                precedence.Expression = "@[User::Create" + table + "_Result] == \"NOTEXISTS\"";

                ssis.AddConstraint(cdcMarkStartInitialLoad,dataFlowTask,DTSExecResult.Success);
                ssis.AddConstraint(dataFlowTask,createIndexes, DTSExecResult.Success);
                ssis.AddConstraint(createIndexes,cdcMarkEndInitialLoad, DTSExecResult.Success);
                ssis.AddConstraint(cdcMarkEndInitialLoad, copyCDCState, DTSExecResult.Success);
            }

            return ssis.package;
        }

        public Package GenerateMergeLoadSSISPackageCDC()
        {
            var ssis = new SSISPackage("CDC Merge Load Package for " + this.DestinationDatabaseName);

            // Create connection managers and add them to the package
            var cdcConnManager = ssis.AddConnectionManager("ADO.NET CDC Database Connection", this.CDCDatabaseConnection, "System.Data.SqlClient");
            var cdcStateManager = ssis.AddConnectionManager("ADO.NET CDC State", this.CDCStateConnection, "System.Data.SqlClient");
            var destinationManager = ssis.AddConnectionManager("Destination Connection", this.DestinationConnection, this.DestinationConnectionProvider);

            // Create normal SQL connection to the CDC database so we can use it to get some table information
            var cdcConnnection = new SqlConnection(this.CDCDatabaseConnection);
            cdcConnnection.Open();

            // Get the cdc Database name
            var cdcdatabase = GetSQLString(cdcConnnection, "SELECT db_name()");

            // Add variables
            ssis.package.Variables.Add("CDC_State", false, "User", "");

            // Add CDC Get CDC Processing Range
            TaskHost cdcControlTaskGetRange = ssis.package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
            cdcControlTaskGetRange.Name = "Get CDC Processing Range";
            cdcControlTaskGetRange.Properties["Connection"].SetValue(cdcControlTaskGetRange, cdcConnManager.ID);
            cdcControlTaskGetRange.Properties["TaskOperation"].SetValue(cdcControlTaskGetRange, CdcControlTaskOperation.GetProcessingRange);
            cdcControlTaskGetRange.Properties["StateConnection"].SetValue(cdcControlTaskGetRange, cdcStateManager.ID);
            cdcControlTaskGetRange.Properties["StateVariable"].SetValue(cdcControlTaskGetRange, "User::CDC_State");
            cdcControlTaskGetRange.Properties["AutomaticStatePersistence"].SetValue(cdcControlTaskGetRange, true);
            cdcControlTaskGetRange.Properties["StateName"].SetValue(cdcControlTaskGetRange, "CDC_State");
            cdcControlTaskGetRange.Properties["StateTable"].SetValue(cdcControlTaskGetRange, "[dbo].[cdc_states]");
            cdcControlTaskGetRange.DelayValidation = true;

            // Add Mark CDC Processed Range
            TaskHost cdcControlTaskMarkRange = ssis.package.Executables.Add("Attunity.CdcControlTask") as TaskHost;
            cdcControlTaskMarkRange.Name = "Mark CDC Processed Range";
            cdcControlTaskMarkRange.Properties["Connection"].SetValue(cdcControlTaskMarkRange, cdcConnManager.ID);
            cdcControlTaskMarkRange.Properties["TaskOperation"].SetValue(cdcControlTaskMarkRange, CdcControlTaskOperation.MarkProcessedRange);
            cdcControlTaskMarkRange.Properties["StateConnection"].SetValue(cdcControlTaskMarkRange, cdcStateManager.ID);
            cdcControlTaskMarkRange.Properties["StateVariable"].SetValue(cdcControlTaskMarkRange, "User::CDC_State");
            cdcControlTaskMarkRange.Properties["AutomaticStatePersistence"].SetValue(cdcControlTaskMarkRange, true);
            cdcControlTaskMarkRange.Properties["StateName"].SetValue(cdcControlTaskMarkRange, "CDC_State");
            cdcControlTaskMarkRange.Properties["StateTable"].SetValue(cdcControlTaskMarkRange, "[dbo].[cdc_states]");
            cdcControlTaskMarkRange.DelayValidation = true;

            // Get list of CDC tables
            var cdctables = GetSQLTableList(cdcConnnection, "WHERE s.name != 'cdc' and t.is_ms_shipped = 0 and t.is_tracked_by_cdc = 1");
            if (this.Tables.Length == 0)
            {
                this.Tables = cdctables.Keys.ToArray();
            }

            // TODO: implement check so we don't fail if the initial load is not done yet, fx skip tables not done, if ILUPDATE or TFSTART or TFREDO
            // CDC States: http://msdn.microsoft.com/en-us/library/hh758667.aspx
            var merge_sql =
                "IF(@cdcstate IS NULL OR @cdcstate = '') BEGIN\n" +
                    "THROW 51001, 'cdcstate variable not set', 1;\n" +
                "END\n" +
                "declare @cdcstart_lsn binary(10);\n" +
                "declare @end_lsn binary(10);\n" +
                "declare @start_lsn binary(10);\n" +
                "declare @min_lsn binary(10);\n" +
                "declare @tablecdcstate nvarchar(256);\n" +
                "declare @rowcount int;\n" +
                "declare @t1 datetime;\n" +
                // Get the start lns and the lsn from the CDC mark operation(Get CDC Processing Range)
                "set @cdcstart_lsn = sys.fn_cdc_increment_lsn(CONVERT(binary(10), SUBSTRING(@cdcstate, CHARINDEX('/CS/', @cdcstate) + 4, CHARINDEX('/', @cdcstate, CHARINDEX('/CS/', @cdcstate) + 4) - CHARINDEX('/CS/', @cdcstate) - 4), 1));\n" +
                "set @end_lsn = CONVERT(binary(10), SUBSTRING(@cdcstate, CHARINDEX('/CE/', @cdcstate) + 4, CHARINDEX('/', @cdcstate, CHARINDEX('/CE/', @cdcstate) + 4) - CHARINDEX('/CE/', @cdcstate) - 4), 1);\n"
            ;

            // Declare @cdcstate as when OLEDB is used ? is used instead of parameter names 
            if(destinationManager.CreationName.StartsWith("OLEDB")) {
                merge_sql =
                    "declare @cdcstate nvarchar(256);\n" +
                    "set @cdcstate = ?;\n" +
                    merge_sql;
            }

            foreach (var table in this.Tables)
            {
                Console.WriteLine("Generating merge code for table " + table);
                var tinfo = new SQLTableInfo(cdcConnnection, table);

                if (tinfo.UniqueColums.Count == 0)
                {
                    throw new Exception("Could not find any unique colums on the table");
                }

                // CDC Debug : http://msdn.microsoft.com/en-us/library/hh758686.aspx   
                // TODO: Do per table skip as first TODO suggested, if ILEND
                // Create the merge SQL statement
                merge_sql +=
                    // Reset start LSN
                    "set @start_lsn = @cdcstart_lsn;\n" +
                    // Get table specific start_lsn if it has been set
                    "set @tablecdcstate = (SELECT state FROM cdc_states WHERE name = 'CDC_State_" + table + "');\n" +
                    "if @tablecdcstate IS NOT NULL AND @tablecdcstate LIKE 'ILEND%' BEGIN\n" +
                        "set @start_lsn = sys.fn_cdc_increment_lsn(CONVERT(binary(10), SUBSTRING(@tablecdcstate, CHARINDEX('/IR/', @tablecdcstate) + 4, CHARINDEX('/', @tablecdcstate, CHARINDEX('/IR/', @tablecdcstate) + 4) - CHARINDEX('/IR',@tablecdcstate) - 4), 1));\n" +
                    "END\n" +
                    // Overwrite start with min if the initial sync has been started before any changes has been done to the table and min lsn is larger than the IL start 
                    "set @min_lsn = " + cdcdatabase + ".sys.fn_cdc_get_min_lsn('" + ReplaceFirst(cdctables[table], "dbo.", "").Replace('.', '_') + "');\n" +
                    "if @min_lsn > @start_lsn BEGIN\n" +
                        "set @start_lsn = @min_lsn;\n" +
                    "END\n" +
                    // Create Merge statement
                    "set @rowcount = 0;\n" +
                    "set @t1 = GETDATE();\n" +
                    "IF @end_lsn >= @start_lsn BEGIN\n" +
                        (tinfo.IdentityColumsUsed ? "SET IDENTITY_INSERT [dbo].[" + table + "] ON;\n" : "") +
                        "MERGE [dbo].[" + table + "] AS D\n" +
                        "USING " + cdcdatabase + ".cdc.fn_cdc_get_net_changes_" + ReplaceFirst(cdctables[table], "dbo.", "").Replace('.', '_') + "(@start_lsn, @end_lsn, 'all with merge') AS S\n" +
                        "ON (" + String.Join(" AND ", tinfo.UniqueColums.Select(s => "D." + s + " = " + "S." + s)) + ")\n" +
                    // Insert
                        "WHEN NOT MATCHED BY TARGET AND __$operation = 5\n" +
                            "THEN INSERT (" + String.Join(", ", tinfo.Colums) + ") VALUES(" + String.Join(", ", tinfo.Colums.Select(s => "S." + s)) + ")\n" +
                    // Update
                        "WHEN MATCHED AND __$operation = 5\n" +
                            "THEN UPDATE SET " + String.Join(", ", tinfo.Colums.Where(s => !tinfo.UniqueColums.Contains(s)).Select(s => "D." + s + " = S." + s)) + "\n" +
                    // Delete
                        "WHEN MATCHED AND __$operation = 1\n" +
                            "THEN DELETE\n" +
                        ";\n" +
                        "set @rowcount = @@ROWCOUNT;\n" +
                        (tinfo.IdentityColumsUsed ? "SET IDENTITY_INSERT [dbo].[" + table + "] OFF;\n" : "") +
                        "DELETE FROM cdc_states WHERE name = 'CDC_State_" + table + "';\n" +
                    "END\n" +
                    "INSERT INTO cdc_status ([name], [rowcount], [elapsed], [cdcstate]) VALUES('" + table + "',@rowcount, DATEDIFF(millisecond,@t1,GETDATE()), @cdcstate);\n";
            }

            var error_msg = "CAST(ERROR_NUMBER() AS VARCHAR) + ',' + CAST(ERROR_SEVERITY() AS VARCHAR) + ',' + CAST(ERROR_STATE() AS VARCHAR) + ',' + CAST(ERROR_LINE() AS VARCHAR) + ': ' + ERROR_MESSAGE()";
            var merge_sql_transwrap =
                "SET XACT_ABORT ON;\n" + // Missing object errors are not cought by try/catch in SQL server so here we tell it to rollback the transaction if something fails
                "BEGIN TRY\n" +
                    "BEGIN TRANSACTION\n" +
                        merge_sql +
                    "COMMIT TRANSACTION\n" +
                "END TRY\n" +
                "BEGIN CATCH\n" +
                    "ROLLBACK TRANSACTION;\n" +
                    "INSERT INTO cdc_status ([name], [rowcount], [elapsed], [cdcstate], [error]) VALUES('error', -1, 0, @cdcstate, " + error_msg + ");\n" +
                    "THROW;\n" +
                "END CATCH\n";

            // Add Execute Merge Command
            var executeMergeSQL = ssis.AddSQLTask("Execute Merge Command", destinationManager, merge_sql_transwrap);
            var executeMergeSQLTask = executeMergeSQL.InnerObject as ExecuteSQLTask;
            executeMergeSQLTask.TimeOut = 30 * 60 ; // 30 minutes

            // Add input parameter binding
            executeMergeSQLTask.ParameterBindings.Add();
            IDTSParameterBinding parameterBinding = executeMergeSQLTask.ParameterBindings.GetBinding(0);
            parameterBinding.ParameterDirection = ParameterDirections.Input;
            parameterBinding.ParameterSize = 256;
            parameterBinding.DtsVariableName = "User::CDC_State";

            if (destinationManager.CreationName.StartsWith("ADO.NET"))
            {
                parameterBinding.DataType = 16; // NVARCHAR in ADO.NET
                parameterBinding.ParameterName = "@cdcstate";
                
            }
            else if (destinationManager.CreationName.StartsWith("OLEDB"))
            {
                parameterBinding.DataType = 130; // NVARCHAR in OLEDB
                parameterBinding.ParameterName = "0";
            }

            // Configure precedence
            ssis.AddConstraint(cdcControlTaskGetRange, executeMergeSQL, DTSExecResult.Success);
            ssis.AddConstraint(executeMergeSQL, cdcControlTaskMarkRange, DTSExecResult.Success);

            return ssis.package;
        }


        public static void SavePackageToXML(Package package, string filename, string password = null)
        {
            Application app = new Application();
            if (password != null)
            {
                package.ProtectionLevel = DTSProtectionLevel.EncryptAllWithPassword;
                package.PackagePassword = password;
            }

            app.SaveToXml(filename, package, null);
        }

        public static void SavePackageToSQLServer(Package package, string connectionString, bool overwrite = false)
        {
            var conn = (new System.Data.SqlClient.SqlConnectionStringBuilder(connectionString));
            Application app = new Application();
            package.ProtectionLevel = DTSProtectionLevel.ServerStorage;

            if (!app.ExistsOnSqlServer(package.Name, conn.DataSource, conn.UserID, conn.Password) || overwrite)
            {
                app.SaveToSqlServer(package, null, conn.DataSource, conn.UserID, conn.Password);
            }
        }

        private static String GetSQLString(DbConnection connnection, String sql)
        {
            DbCommand sqlcmd;
            if (connnection is SqlConnection)
            {
                sqlcmd = new System.Data.SqlClient.SqlCommand(sql, (SqlConnection)connnection);
            }
            else if (connnection is OleDbConnection)
            {
                sqlcmd = new System.Data.OleDb.OleDbCommand(sql, (OleDbConnection)connnection);
            }
            else
            {
                throw new Exception("Unknown connection type");
            }

            using (var sqlRd = sqlcmd.ExecuteReader())
            {
                while (sqlRd.Read())
                {
                    return sqlRd.GetString(0);
                }
            }

            return null;
        }

        private static DbConnection CreateDBConnection(String connstr, String provider)
        {
            if (provider == "OLEDB")
            {
                provider = "System.Data.OleDb";
            }

            var factory = DbProviderFactories.GetFactory(provider);
            var dbconnection = factory.CreateConnection();
            dbconnection.ConnectionString = connstr;
            return dbconnection;
        }

        private static Dictionary<String, String> GetSQLTableList(DbConnection connnection, string whereclause = "")
        {
            var tables = new Dictionary<String, String>();
            var sql =
                "SELECT s.name, t.name" + ", t.is_ms_shipped, t.is_tracked_by_cdc " +
                "FROM sys.tables t INNER JOIN sys.schemas s ON (t.schema_id = s.schema_id) " +
                whereclause;

            DbCommand sqlcmd;
            if (connnection is SqlConnection)
            {
                sqlcmd = new System.Data.SqlClient.SqlCommand(sql, (SqlConnection)connnection);
            }
            else if (connnection is OleDbConnection)
            {
                sqlcmd = new System.Data.OleDb.OleDbCommand(sql, (OleDbConnection)connnection);
            }
            else
            {
                throw new Exception("Unknown connection type");
            }

            using (var sqlRd = sqlcmd.ExecuteReader())
            {
                while (sqlRd.Read())
                {
                    var test = sqlRd.GetString(0) + ", " + sqlRd.GetString(1) + ", " + sqlRd.GetBoolean(2) + ", " + sqlRd.GetBoolean(3);
                    tables[sqlRd.GetString(1)] = sqlRd.GetString(0) + "." + sqlRd.GetString(1) + "";
                }
            }
            return tables;
        }

        private static string ReplaceFirst(string text, string search, string replace)
        {
            int pos = text.IndexOf(search);
            if (pos < 0)
            {
                return text;
            }
            return text.Substring(0, pos) + replace + text.Substring(pos + search.Length);
        }

        // Utility Classes
        private class SQLTableInfo
        {
            public HashSet<String> UniqueColums = new HashSet<String>();
            public HashSet<String> NullableColums = new HashSet<String>();
            public Dictionary<String, String> DefaultValueColums = new Dictionary<String, String>();
            public String CreatePrimaryKeySQL = null;
            public List<string> Colums = new List<string>();
            public Dictionary<string,string> UnsupportedTypes = new Dictionary<string,string>();
            public bool IdentityColumsUsed = false;

            public SQLTableInfo(DbConnection connnection, string table)
            {
                // Get Colums from database
                string sql =
                    "SELECT c.name AS column_name, is_identity, ISNULL(i.is_unique, 0) as is_unique, ISNULL(i.is_primary_key, 0) as is_primary_key, ISNULL(ic.is_descending_key, 0) AS is_descending_key\n" +
                    ",ty.name, c.max_length, d.name AS column_default_name, d.definition AS column_default_definition, ISNULL(c.is_nullable, 0) AS is_nullable\n" +
                    "FROM sys.tables t\n" +
                    "LEFT JOIN sys.columns c ON (t.object_id = c.object_id)\n" +
                    "LEFT JOIN sys.types ty ON (c.user_type_id = ty.user_type_id)\n" +
                    "LEFT JOIN sys.index_columns ic ON (c.object_id = ic.object_id AND c.column_id = ic.column_id)\n" +
                    "LEFT JOIN sys.indexes i ON (i.object_id = ic.object_id AND i.index_id = ic.index_id)\n" +
                    "LEFT JOIN sys.default_constraints d on (c.default_object_id = d.object_id)\n" +
                    "WHERE t.name = '" + table + "'\n" +
                    "ORDER BY ic.index_id, ic.index_column_id";

                DbCommand sqlcmd;
                if (connnection is SqlConnection)
                {
                    sqlcmd = new System.Data.SqlClient.SqlCommand(sql, (SqlConnection)connnection);
                }
                else if (connnection is OleDbConnection)
                {
                    sqlcmd = new System.Data.OleDb.OleDbCommand(sql, (OleDbConnection)connnection);
                }
                else
                {
                    throw new Exception("Unknown connection type");
                }

                using (var sqlRd = sqlcmd.ExecuteReader())
                {
                    while (sqlRd.Read())
                    {
                        Colums.Add(sqlRd.GetString(0));
                        if(sqlRd.GetString(5) == "varchar" && sqlRd.GetInt16(6) == -1) {
                            UnsupportedTypes.Add(sqlRd.GetString(0), "varchar(max)");
                        }
                        
                        if (sqlRd.GetBoolean(1) || sqlRd.GetBoolean(2))
                        {
                            UniqueColums.Add(sqlRd.GetString(0));
                        }

                        if (sqlRd.GetBoolean(1))
                        {
                            IdentityColumsUsed = true;
                        }

                        if(sqlRd.GetBoolean(3))
                        {
                            var key = sqlRd.GetBoolean(4) ? sqlRd.GetString(0) + " DESC" : sqlRd.GetString(0);
        
                            // Init string if null
                            CreatePrimaryKeySQL += CreatePrimaryKeySQL == null 
                              ? "ALTER TABLE " + table + " ADD CONSTRAINT PK_" + table + " " + "PRIMARY KEY CLUSTERED (" + key 
                              : "," + key;
                        }

                        if (!sqlRd.IsDBNull(7))
                        {
                            DefaultValueColums[sqlRd.GetString(0)] = sqlRd.GetString(8);
                        }

                        if (sqlRd.GetBoolean(9))
                        {
                            NullableColums.Add(sqlRd.GetString(0));
                        }
                    }
                }

                if (CreatePrimaryKeySQL != null)
                {
                    CreatePrimaryKeySQL += ")";
                }
            }
        }
    }
}

