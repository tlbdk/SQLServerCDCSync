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
            
            //var pgkuploadconn = ConfigurationManager.ConnectionStrings["PackageUploadConnection"];
            string password = ConfigurationManager.AppSettings["Password"];


            var generator = new SQLServerCDCSync(
                ConfigurationManager.ConnectionStrings["SourceConnection"], 
                ConfigurationManager.ConnectionStrings["DestinationConnection"], 
                ConfigurationManager.ConnectionStrings["CDCDatabaseConnection"],
                ConfigurationManager.ConnectionStrings["CDCStateConnection"],
                (ConfigurationManager.AppSettings["Tables"] ?? "").Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
            );

            // Generate initial sync package
            var initpackage = generator.GenerateInitialLoadSSISPackage();
            var mergepackage = generator.GenerateMergeLoadSSISPackage();
            // Generate merge package

            //var test = initpackage.Execute();

            SQLServerCDCSync.SavePackageToXML(initpackage, @"C:\repos\SQLServerCDCSync\SQLServerCDCSync.SSISSample\InitialLoadTest1.dtsx", password);
            SQLServerCDCSync.SavePackageToXML(mergepackage, @"C:\repos\SQLServerCDCSync\SQLServerCDCSync.SSISSample\MergeLoadTest1.dtsx", password); 

            //var mergepackage = generator.GenerateMergeSSISPackage();


            /* 
            var initpackage = SQLServerCDCSync.GenerateInitialLoadSSISPackage(
                sourceconn.ProviderName, sourceconn.ConnectionString, destinationconn.ConnectionString, cdcdatabase, tables
            );
            
             
            var mergepackage = SQLServerCDCSync.GenerateMergeLoadSSISPackage(
                destinationconn.ProviderName, destinationconn.ConnectionString, cdcdatabase, tables
            );

            
            
            
            if(pgkuploadconn != null)
            {
                SQLServerCDCSync.SavePackageToSQLServer(initpackage, pgkuploadconn.ConnectionString, true);
                SQLServerCDCSync.SavePackageToSQLServer(mergepackage, pgkuploadconn.ConnectionString, true);
            } */
            
        }
    }
}
