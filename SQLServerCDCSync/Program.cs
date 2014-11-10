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
            
            string password = ConfigurationManager.AppSettings["Password"];

            var generator = new SQLServerCDCSync(
                ConfigurationManager.ConnectionStrings["SourceConnection"], 
                ConfigurationManager.ConnectionStrings["DestinationConnection"], 
                ConfigurationManager.ConnectionStrings["CDCDatabaseConnection"],
                ConfigurationManager.ConnectionStrings["CDCStateConnection"],
                (ConfigurationManager.AppSettings["Tables"] ?? "").Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
            );

            generator.ErrorLogPath = @"c:\logs";

            // Generate initial load and merge packages if we have a CDC connection
            if (ConfigurationManager.ConnectionStrings["CDCDatabaseConnection"] != null)
            {
                var initpackage = generator.GenerateInitialLoadSSISPackageCDC();
                var mergepackage = generator.GenerateMergeLoadSSISPackageCDC();

                // Limit the package to max 4 tables;
                initpackage.MaxConcurrentExecutables = 4;

                // Save local version of the packages
                SQLServerCDCSync.SavePackageToXML(initpackage, @"C:\repos\SQLServerCDCSync\SQLServerCDCSync.SSISSample\InitialLoadTest1.dtsx", password);
                SQLServerCDCSync.SavePackageToXML(mergepackage, @"C:\repos\SQLServerCDCSync\SQLServerCDCSync.SSISSample\MergeLoadTest1.dtsx", password);

                // Upload packages to server
                if (ConfigurationManager.ConnectionStrings["PackageUploadConnection"] != null)
                {
                    SQLServerCDCSync.SavePackageToSQLServer(initpackage, ConfigurationManager.ConnectionStrings["PackageUploadConnection"].ConnectionString, true);
                    SQLServerCDCSync.SavePackageToSQLServer(mergepackage, ConfigurationManager.ConnectionStrings["PackageUploadConnection"].ConnectionString, true);
                }
            }
            else
            {
                var initpackage = generator.GenerateInitialLoadSSISPackageSimple();
                // Limit the package to max 4 tables;
                initpackage.MaxConcurrentExecutables = 4;
                SQLServerCDCSync.SavePackageToXML(initpackage, @"C:\repos\SQLServerCDCSync\SQLServerCDCSync.SSISSample\InitialLoadTest2.dtsx", password);

                if (ConfigurationManager.ConnectionStrings["PackageUploadConnection"] != null)
                {
                    SQLServerCDCSync.SavePackageToSQLServer(initpackage, ConfigurationManager.ConnectionStrings["PackageUploadConnection"].ConnectionString, true);
                }
            }

            if ( System.Diagnostics.Debugger.IsAttached ) {
                Console.WriteLine("Done");
                Console.ReadKey();
            }
        }
    }
}
