declare @cdcstate nvarchar(256);
set @cdcstate = (SELECT TOP 1 state FROM [SQLServerCDCSyncDestination].[dbo].[cdc_states]);

declare @start_lsn binary(10);
declare @end_lsn binary(10);
declare @rowcount int;

set @rowcount = 0;
set @start_lsn = SQLServerCDCSync.sys.fn_cdc_increment_lsn(CONVERT(binary(10), SUBSTRING(@cdcstate, CHARINDEX('/CS/', @cdcstate) + 4, CHARINDEX('/', @cdcstate, CHARINDEX('/CS/', @cdcstate) + 4) - CHARINDEX('/CS/', @cdcstate) - 4), 1));
set @end_lsn = convert(binary(10), SUBSTRING(@cdcstate, CHARINDEX('/CE/', @cdcstate) + 4, CHARINDEX('/', @cdcstate, CHARINDEX('/CE/', @cdcstate) + 4) - CHARINDEX('/CE/', @cdcstate) - 4), 1);

IF @end_lsn > @start_lsn BEGIN
SELECT * FROM SQLServerCDCSync.cdc.fn_cdc_get_net_changes_Test1(@start_lsn, @end_lsn, 'all with merge');

SET IDENTITY_INSERT [dbo].[Test1] ON;
MERGE [dbo].[Test1] AS D
USING SQLServerCDCSync.cdc.fn_cdc_get_net_changes_Test1(@start_lsn, @end_lsn, 'all with merge') AS S
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
;
set @rowcount = @@ROWCOUNT;
SET IDENTITY_INSERT [dbo].[Test1] OFF;
END
SELECT @rowcount as NumberOfRecords;