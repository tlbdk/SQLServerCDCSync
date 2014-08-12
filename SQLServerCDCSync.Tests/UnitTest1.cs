using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Data.SqlClient;
using System.IO;
using System.Data;
using System.Linq;

// Links:
// http://blogs.msdn.com/b/repltalk/archive/2010/09/30/walk-through-of-microsoft-sql-server-change-data-capture.aspx

namespace SQLServerCDCSync.Tests
{
    [TestClass]
    [DeploymentItem("test1.sql")]
    public class UnitTest1
    {
        private SqlConnection connection;

        private TestContext testContext;
        public TestContext TestContext
        {
            get { return testContext; }
            set { testContext = value; }
        }

        [TestInitialize()]
        public void TestInitialize()
        {
            connection = new SqlConnection("user id=sa;" +
                                       "password=Qwerty1234;server=localhost;" +
                                       "Trusted_Connection=yes;" +
                                       "database=SQLServerCDCSync; " +
                                       "connection timeout=30");
            connection.Open();

            var schema = connection.GetSchema("Columns", new string[4] { null, null, "Test1", null }).AsEnumerable().Select(s => s.Field<String>("Column_Name")).ToList();


            // Create test1 tables
            //string path = TestContext.DeploymentDirectory;
            //SqlCommand test1command = new SqlCommand(File.ReadAllText("test1.sql"), connection);
            //int affectedRows = test1command.ExecuteNonQuery();

            // Insert a value and delete it again and insert it again within a transaction
            /* var transaction = connection.BeginTransaction();
            SqlCommand command;
            command = new SqlCommand("INSERT INTO [dbo].[Test1] (FirstName, LastName, TestId) VALUES ('Troels Liebe', 'Bentsen', 2); SELECT Scope_Identity();", connection, transaction);
            id = command.ExecuteScalar().ToString();

            command = new SqlCommand("DELETE FROM [dbo].[Test1] WHERE ID = " + id + "AND TestID = 2", connection, transaction);
            if (command.ExecuteNonQuery() != 1)
            {
                throw new Exception("Could not delete row");
            }

            command = new SqlCommand("SET IDENTITY_INSERT [dbo].[Test1] ON;", connection, transaction);
            command.ExecuteNonQuery();

            command = new SqlCommand("INSERT INTO [dbo].[Test1] (ID, FirstName, LastName, TestId) VALUES (" + id + ",'Troels Liebe', 'Bentsen', 2);", connection, transaction);
            command.ExecuteNonQuery();

            command = new SqlCommand("SET IDENTITY_INSERT [dbo].[Test1] OFF;", connection, transaction);
            command.ExecuteNonQuery();

            transaction.Commit(); */

        }

        [TestCleanup()]
        public void TestCleanup()
        {
        }

        [TestMethod]
        public void Test1()
        {
           


        }


        [TestMethod]
        public void Test2()
        {



        }
    }
}
