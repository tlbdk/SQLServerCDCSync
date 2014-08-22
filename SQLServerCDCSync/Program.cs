using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLServerCDCSync
{
    class Program
    {
        static void Main(string[] args)
        {
            string sourceconn = "user id=sa;password=Qwerty1234;server=localhost;Trusted_Connection=yes;database=SQLServerCDCSync;connection timeout=30";
            string destinationconn = "user id=sa;password=Qwerty1234;server=localhost;Trusted_Connection=yes;database=SQLServerCDCSyncDestination;connection timeout=30";
            string cdcdatabase = "SQLServerCDCSync";
            string[] tables = new string[] { "Test1", "Test2" };

            // Make sure external dependencies such as environment variables are initialized 
            SQLServerCDCSync.InitializeEnvironment();

            // Generate initial sync package
            SQLServerCDCSync.GenerateInitialLoadSSISPackage(@"C:\repos\SQLServerCDCSync\SQLServerCDCSync.SSISSample\InitialLoadTest1.dtsx",
                "System.Data.OracleClient", sourceconn, destinationconn, cdcdatabase, tables
            );

            SQLServerCDCSync.GenerateMergeLoadSSISPackage(@"C:\repos\SQLServerCDCSync\SQLServerCDCSync.SSISSample\MergeLoadTest1.dtsx",
                destinationconn, cdcdatabase, tables
            );
        }
    }
}
