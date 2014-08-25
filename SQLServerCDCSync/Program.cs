using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLServerCDCSync
{
    class Program
    {
        static void Main(string[] args)
        {
            // http://www.codeproject.com/Articles/547311/ProgrammaticallyplusCreateplusandplusDeployplusSSI

            var sourceconn = ConfigurationManager.ConnectionStrings["SourceConnection"];
            var destinationconn = ConfigurationManager.ConnectionStrings["DestinationConnection"];
            var pgkuploadconn = ConfigurationManager.ConnectionStrings["PackageUploadConnection"];
            string cdcdatabase = ConfigurationManager.AppSettings["CDCDatabase"];
            string[] tables = ConfigurationManager.AppSettings["Tables"].Split(',');
            string password = ConfigurationManager.AppSettings["Password"];


            /* TODO: Move to OO interface as we are now starting to reuse some of the information in both queries
            var generator = new SQLServerCDCSync();
            generator.SourceConnection = "";
            generator.SourceConnectionProvider = "System.Data.OracleClient";
            generator.DestinationConnection = "";
            generator.CDCDatabase = "";
            generator.Tables = new string[] { };

            generator.RefreshMetaData();
            
            var p1 = generator.GenerateInitialLoadSSISPackage();
            var p2 = generator.GenerateMergeSSISPackage();
            generator.SaveToSQLServer(); */

            // Make sure external dependencies such as environment variables are initialized 
            SQLServerCDCSync.InitializeEnvironment();

            // Generate initial sync package
            var initpackage = SQLServerCDCSync.GenerateInitialLoadSSISPackage(
                sourceconn.ProviderName, sourceconn.ConnectionString, destinationconn.ConnectionString, cdcdatabase, tables
            );
            
             // Generate merge package
            var mergepackage = SQLServerCDCSync.GenerateMergeLoadSSISPackage(
                destinationconn.ConnectionString, cdcdatabase, tables
            );

            /* SQLServerCDCSync.SavePackageToXML(initpackage, @"C:\repos\SQLServerCDCSync\SQLServerCDCSync.SSISSample\InitialLoadTest1.dtsx", password);
            SQLServerCDCSync.SavePackageToXML(mergepackage, @"C:\repos\SQLServerCDCSync\SQLServerCDCSync.SSISSample\MergeLoadTest1.dtsx", password); */
            
            if(pgkuploadconn != null)
            {
                SQLServerCDCSync.SavePackageToSQLServer(initpackage, pgkuploadconn.ConnectionString);
                SQLServerCDCSync.SavePackageToSQLServer(mergepackage, pgkuploadconn.ConnectionString);
            }
            
        }
    }
}
