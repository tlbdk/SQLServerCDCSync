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
            SQLServerCDCSync.GenerateInitialLoadSSISPackage(@"C:\repos\SQLServerCDCSync\SQLServerCDCSync.SSISSample\InitialLoadTest1.dtsx",
                "user id=sa;password=Qwerty1234;server=localhost;Trusted_Connection=yes;database=SQLServerCDCSync;connection timeout=30",
                "user id=sa;password=Qwerty1234;server=localhost;Trusted_Connection=yes;database=SQLServerCDCSyncDestination;connection timeout=30", 
                new String[] { "Test1" }
            );
        }
    }
}
