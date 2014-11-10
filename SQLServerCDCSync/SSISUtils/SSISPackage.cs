using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLServerCDCSync.SSISUtils
{
    class SSISPackage
    {
        public Application app;
        public Package package;

        public SSISPackage(String name) : this(name, new Application(), new Package())
        {
        }

        public SSISPackage(String name, Application app, Package package)
        {
            this.app = app;
            this.package = package;
            this.package.Name = name;
        }

        public TaskHost AddSQLTask(String name, ConnectionManager manager, String sql)
        {
            TaskHost sqltask = package.Executables.Add("STOCK:SQLTask") as TaskHost;
            sqltask.Name = name;
            sqltask.Properties["Connection"].SetValue(sqltask, manager.ID);
            sqltask.Properties["SqlStatementSource"].SetValue(sqltask, sql);
            return sqltask;
        }

        public PrecedenceConstraint AddConstraint(Executable exec1, Executable exec2, DTSExecResult requiredstatus = DTSExecResult.Success)
        {
            var constraint = package.PrecedenceConstraints.Add(exec1, exec2);
            constraint.Value = requiredstatus;
            return constraint;
        }


        public ConnectionManager AddConnectionManager(String name, String connstr, String connprovide, String initialcatalog = null)
        {
            String provider;

            if (connprovide == "System.Data.SqlClient")
            {
                provider = string.Format("ADO.NET:{0}", typeof(SqlConnection).AssemblyQualifiedName);
                var connbuilder = (new System.Data.SqlClient.SqlConnectionStringBuilder(connstr));
                connbuilder.ConnectTimeout = 300;
                if (initialcatalog != null)
                {
                    connbuilder.InitialCatalog = initialcatalog;
                }
                connstr = connbuilder.ConnectionString;
            }
            else if (connprovide == "System.Data.OracleClient")
            {
#pragma warning disable 612, 618 // Disable the Obsolete warning for the OracleClient component
                provider = string.Format("ADO.NET:{0}", typeof(System.Data.OracleClient.OracleConnection).AssemblyQualifiedName);
#pragma warning restore 612, 618
            }
            else if (connprovide == "Oracle.ManagedDataAccess.Client")
            {
                provider = string.Format("ADO.NET:{0}", typeof(Oracle.ManagedDataAccess.Client.OracleConnection).AssemblyQualifiedName);
            }
            else if (connprovide == "OLEDB")
            {
                provider = "OLEDB";
                var connbuilder = (new System.Data.OleDb.OleDbConnectionStringBuilder(connstr));
                if (connbuilder.Provider.StartsWith("SQLNCLI"))
                {
                    connbuilder["Connect Timeout"] = 300;
                    connbuilder["General Timeout"] = 300;

                    if (initialcatalog != null)
                    {
                        connbuilder["Initial Catalog"] = initialcatalog;
                    }

                }
                connstr = connbuilder.ConnectionString;
            }
            else
            {
                throw new Exception("Unknown connection provider type");
            }

            ConnectionManager connmanager = package.Connections.Add(provider);
            connmanager.ConnectionString = connstr;
            connmanager.Name = name;

            return connmanager;
        }

    }
}
