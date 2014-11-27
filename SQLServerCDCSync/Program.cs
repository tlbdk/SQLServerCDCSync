using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// http://www.codeproject.com/Articles/547311/ProgrammaticallyplusCreateplusandplusDeployplusSSI

namespace SQLServerCDCSync
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = LoadConfig(args[0]);
            var appSettings = config.AppSettings.Settings;
            var connectionStrings = ((System.Configuration.ConnectionStringsSection)config.GetSection("connectionStrings")).ConnectionStrings;

            string password = null;
            if (config.AppSettings.Settings.AllKeys.Contains("Password"))
            {
                password = appSettings["Password"].Value;
            }
            var errorlogpath = @"C:\logs";
            if (config.AppSettings.Settings.AllKeys.Contains("ErrorLogPath"))
            {
                errorlogpath = appSettings["ErrorLogPath"].Value;
            }
            string tables = "";
            if (config.AppSettings.Settings.AllKeys.Contains("Tables"))
            {
                tables = appSettings["Tables"].Value;
            }
            string packagepath = null;
            if (args.Length > 1)
            {
                packagepath = args[1];
                Console.WriteLine("Package path set to {0}", packagepath);
            }

            var generator = new SQLServerCDCSync(
                connectionStrings["SourceConnection"],
                connectionStrings["DestinationConnection"],
                connectionStrings["CDCDatabaseConnection"],
                connectionStrings["CDCStateConnection"],
                tables.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
            );
            generator.ErrorLogPath = errorlogpath;
            
            // Generate initial load and merge packages if we have a CDC connection
            if (connectionStrings["CDCDatabaseConnection"] != null)
            {
                var initpackage = generator.GenerateInitialLoadSSISPackageCDC();
                var mergepackage = generator.GenerateMergeLoadSSISPackageCDC();

                // Limit the package to max 4 tables;
                initpackage.MaxConcurrentExecutables = 4;

                // Save local version of the packages
                if (packagepath != null)
                {
                    Console.WriteLine("Save packages to {0}", packagepath);
                    SQLServerCDCSync.SavePackageToXML(initpackage, Path.Combine(packagepath, initpackage.Name + ".dtsx"), password);
                    SQLServerCDCSync.SavePackageToXML(mergepackage, Path.Combine(packagepath, mergepackage.Name + ".dtsx"), password);
                }

                // Upload packages to server
                if (connectionStrings["PackageUploadConnection"] != null)
                {
                    Console.WriteLine("Upload packages to {0}", connectionStrings["PackageUploadConnection"].ConnectionString);
                    SQLServerCDCSync.SavePackageToSQLServer(initpackage, initpackage.Name, connectionStrings["PackageUploadConnection"].ConnectionString, true);
                    SQLServerCDCSync.SavePackageToSQLServer(mergepackage, mergepackage.Name, connectionStrings["PackageUploadConnection"].ConnectionString, true);
                }
            }
            else
            {
                var initpackage = generator.GenerateInitialLoadSSISPackageSimple();
                // Limit the package to max 4 tables;
                initpackage.MaxConcurrentExecutables = 4;

                if (packagepath != null) {
                    Console.WriteLine("Save packages to {0}", packagepath);
                    SQLServerCDCSync.SavePackageToXML(initpackage, Path.Combine(packagepath, initpackage.Name + ".dtsx"), password);
                }

                if (connectionStrings["PackageUploadConnection"] != null)
                {
                    Console.WriteLine("Upload packages to {0}", connectionStrings["PackageUploadConnection"].ConnectionString);
                    SQLServerCDCSync.SavePackageToSQLServer(initpackage, initpackage.Name, connectionStrings["PackageUploadConnection"].ConnectionString, true);
                }
            }

            if (System.Diagnostics.Debugger.IsAttached) {
                Console.WriteLine("Done");
                Console.ReadKey();
            }
        }

        private static Configuration LoadConfig(String path)
        {
            ExeConfigurationFileMap configFileMap = new ExeConfigurationFileMap() { ExeConfigFilename = path };
            return ConfigurationManager.OpenMappedExeConfiguration(configFileMap, ConfigurationUserLevel.None);
        }

        private static String GetFullPathToExe(String path) {
            if (!Path.IsPathRooted(path))
            {
                var runpath = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
                path = Path.GetFullPath(Path.Combine(runpath, path));
            }
            return path;
        }
    }
}
