use SQLServerCDCSync;

-- DROP Table
IF OBJECT_ID('dbo.Test1Copy', 'U') IS NOT NULL DROP TABLE [dbo].[Test1Copy];

-- Copy table structure
SELECT * INTO [dbo].[Test1Copy] FROM [dbo].[Test1] WHERE 1 = 2;

BEGIN TRAN

-- http://www.mattmasson.com/2012/01/processing-modes-for-the-cdc-source/
SET IDENTITY_INSERT [dbo].[Test1Copy] ON;
MERGE [dbo].[Test1Copy] AS D
USING cdc.fn_cdc_get_net_changes_Test1(0x0000002A000001190019, 0x0000002A000001200008, 'all with merge') AS S
ON (D.Id = S.Id)
-- Insert
WHEN NOT MATCHED BY TARGET AND __$operation = 5
    THEN INSERT (Id, FirstName, LastName, TestId) VALUES(S.Id, S.FirstName, S.LastName, S.TestId)
-- Update
WHEN MATCHED AND __$operation = 5
    THEN UPDATE SET D.FirstName = S.FirstName, D.LastName = S.LastName, D.TestId = S.TestId
-- Delete
WHEN MATCHED AND __$operation = 1
    THEN DELETE
OUTPUT $action, inserted.*, deleted.*;
SET IDENTITY_INSERT [dbo].[Test1Copy] OFF;

/*

-- Insert all new rows
SET IDENTITY_INSERT [dbo].[Test1Copy] ON;
INSERT INTO [dbo].[Test1Copy] (Id, FirstName, LastName, TestId)
SELECT Id, FirstName, LastName, TestId
FROM cdc.fn_cdc_get_net_changes_Test1(0x0000002A000001190019, 0x0000002A000001200008, 'all')
WHERE __$operation = 2;
SET IDENTITY_INSERT [dbo].[Test1Copy] OFF;

-- Update all updated rows 
UPDATE D
SET D.FirstName = S.FirstName, D.LastName = S.LastName, D.TestId = S.TestId
FROM [dbo].[Test1Copy] D, cdc.fn_cdc_get_net_changes_Test1(0x0000002A000001190019, 0x0000002A000001200008, 'all') S
WHERE D.[Id] = S.[Id] AND S.__$operation = 4;


-- Batch delete
DELETE FROM [dbo].[Test1Copy] 
WHERE Id IN
(
    SELECT [Id]
    FROM cdc.fn_cdc_get_net_changes_Test1(0x0000002A000001190019, 0x0000002A000001200008, 'all') S
	WHERE S.__$operation = 4
);
*/
COMMIT

SELECT * from dbo.Test1
EXCEPT
SELECT * FROM dbo.Test1Copy;