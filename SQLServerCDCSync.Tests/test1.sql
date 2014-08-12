use master;
ALTER DATABASE [SQLServerCDCSync] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; 
DROP DATABASE [SQLServerCDCSync];
CREATE DATABASE [SQLServerCDCSync];

use SQLServerCDCSync;

EXEC sys.sp_cdc_enable_db;

CREATE TABLE [dbo].[Test1](
    [Id] [int] IDENTITY(1,1) NOT NULL,
    [FirstName] [nvarchar](max) NOT NULL,
    [LastName] [nvarchar](max) NOT NULL,
	[TestId] [int] NOT NULL,
CONSTRAINT [PK_Test1] PRIMARY KEY CLUSTERED ([Id] ASC) ON [PRIMARY]) ON [PRIMARY];

-- Enable CDC on Table
EXEC sys.sp_cdc_enable_table
	@source_schema = N'dbo',
	@source_name = N'Test1',
	@capture_instance = N'Test1',
	@role_name = NULL,
	@supports_net_changes = 1
;

declare @TestRowID int;

 -- Insert a value and delete it again and insert it again
INSERT INTO [dbo].[Test1] (FirstName, LastName, TestId) VALUES ('Troels Liebe', 'Bentsen', 1); 
SET @TestRowID = CAST(SCOPE_IDENTITY() AS INT);
DELETE FROM [dbo].[Test1] WHERE ID = @TestRowID;
SET IDENTITY_INSERT [dbo].[Test1] ON;
INSERT INTO [dbo].[Test1] (ID, FirstName, LastName, TestId) VALUES (@TestRowID,'Troels Liebe', 'Bentsen', 1);
SET IDENTITY_INSERT [dbo].[Test1] OFF;

-- Insert a value and delete it again and insert it again within a transaction
BEGIN TRAN
INSERT INTO [dbo].[Test1] (FirstName, LastName, TestId) VALUES ('Troels Liebe', 'Bentsen', 2); 
SET @TestRowID = CAST(SCOPE_IDENTITY() AS INT);
DELETE FROM [dbo].[Test1] WHERE ID = @TestRowID;
SET IDENTITY_INSERT [dbo].[Test1] ON;
INSERT INTO [dbo].[Test1] (ID, FirstName, LastName, TestId) VALUES (@TestRowID,'Troels Liebe', 'Bentsen', 2);
SET IDENTITY_INSERT [dbo].[Test1] OFF;
COMMIT