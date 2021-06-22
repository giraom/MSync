
--dbcc traceon(610)--perform minimal logging when we load a clustered table which is not empty
go

if not exists(select * from sys.schemas where name = 'rpl')
	exec('create schema rpl authorization dbo')
go
if exists (select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where CONSTRAINT_NAME='fk_rpl_ImportLog_Subscription')
	alter table rpl.ImportLog drop constraint fk_rpl_ImportLog_Subscription 
go
if exists (select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where CONSTRAINT_NAME='fk_rpl_ImportLogDetail_ImportLog')
	alter table rpl.ImportLogDetail drop constraint fk_rpl_ImportLogDetail_ImportLog 
go
if exists (select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where CONSTRAINT_NAME='fk_rpl_SubscriptionTable_Subscription')
	alter table rpl.SubscriptionTable drop constraint fk_rpl_SubscriptionTable_Subscription 
go
if exists (select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where CONSTRAINT_NAME='fk_rpl_SubscriptionColumn_SubscriptionTable')
	alter table rpl.SubscriptionColumn drop constraint fk_rpl_SubscriptionColumn_SubscriptionTable 
go
if exists (select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where CONSTRAINT_NAME='fk_rpl_SubscriptionRoutine_Subscription')
	alter table rpl.SubscriptionRoutine drop constraint fk_rpl_SubscriptionRoutine_Subscription 
go


--Sources
if object_id('rpl.Subscription') is not null
	drop table rpl.Subscription
go
CREATE TABLE rpl.Subscription(
	SubscriptionId int identity NOT NULL,
	SubscriptionName  varchar(100) NULL,
	PriorityGroup tinyint null,
	ServerName varchar(100) NULL,
	DatabaseName varchar(100) NULL,
	IsActive bit null constraint df_Subscription_IsActive default (1),
	FrequencyInMinutes int,
	DelayAlertInMinutes int,
	Initialize bit null,
	Login varchar(100),
	Pass varchar (100),
	DoubleReadRVRange bit,
	SubscriptionSequence tinyint,
	rv rowversion
 CONSTRAINT PK_Subscription PRIMARY KEY CLUSTERED (SubscriptionId ASC)
) ON [PRIMARY]
go
create index IX_Rpl_Subscription on rpl.Subscription  (ServerName, DatabaseName)

GO

--Routine
if object_id('rpl.SubscriptionRoutine') is not null
	DROP TABLE rpl.SubscriptionRoutine
GO
CREATE TABLE rpl.SubscriptionRoutine(
		RoutineId int identity NOT NULL,
		SubscriptionId int not null,
		RoutineName varchar(128) NULL,
		IsActive bit null constraint df_SubscriptionRoutine_IsActive default (1),
		RoutineSequence tinyint,
	rv rowversion
 CONSTRAINT PK_SubscriptionRoutine PRIMARY KEY CLUSTERED (RoutineId ASC)
) ON [PRIMARY]
GO
create unique index UDX_SubscriptionRoutine on rpl.SubscriptionRoutine  (SubscriptionId, RoutineName) 
alter table rpl.SubscriptionRoutine add constraint fk_rpl_SubscriptionRoutine_Subscription foreign key (SubscriptionId) references rpl.Subscription(SubscriptionId) ON DELETE CASCADE

go


--Tables
if object_id('rpl.SubscriptionTable') is not null
	DROP TABLE rpl.SubscriptionTable
GO
CREATE TABLE rpl.SubscriptionTable(
		TableId int identity NOT NULL,
		SubscriptionId int not null,
		SchemaName varchar(100) NULL,
		TableName varchar(128) NULL,
		PublisherSchemaName varchar(128) NULL,
		PublisherTableName varchar(128) NULL,
		IsActive bit null constraint df_SubscriptionTable_IsActive default (1),
		PkName varchar(128) NULL,
		KeyCount smallint NULL,
		has_identity bit null,
		Initialize bit null,
		InitialRowCount bigint null,
		IsCustom bit default(0),
		GetProcName varchar(100),
		ExcludeFromChecks bit,
		IgnoreKeyColumn	varchar	(100),
		IgnoreColumns	varchar	(1000),
		rv rowversion
 CONSTRAINT PK_SubscriptionTable PRIMARY KEY CLUSTERED (TableId ASC)
) ON [PRIMARY]
GO
--create index IX_SubscriptionTable_SubscriptionId on rpl.SubscriptionTable  (SubscriptionId) 
create unique index UX_SubscriptionTable on rpl.SubscriptionTable  (SubscriptionId, SchemaName, TableName) 
alter table rpl.SubscriptionTable add constraint fk_rpl_SubscriptionTable_Subscription foreign key (SubscriptionId) references rpl.Subscription(SubscriptionId)  ON DELETE CASCADE

go


if OBJECT_ID('rpl.SubscriptionColumn') is not null
	DROP TABLE rpl.SubscriptionColumn
	GO
	CREATE TABLE rpl.SubscriptionColumn(
		ColumnId int identity NOT NULL,
		TableId int NOT NULL,
		ColumnName varchar(128) NOT NULL,
		DataType varchar(128) NULL,
		ColumnLength int NULL,
		IsKey bit NULL,
		ColId int NULL,
		IsIdentity int null,
		ApplyCompression bit null,
		rv rowversion
		CONSTRAINT PK_SubscriptionColumn PRIMARY KEY CLUSTERED (ColumnId ASC)
	) ON [PRIMARY]

	GO
	create unique index UDX_SubscriptionColumn on rpl.SubscriptionColumn (TableId, ColumnName)
	alter table rpl.SubscriptionColumn add constraint FK_rpl_SubscriptionColumn_SubscriptionTable foreign key (TableId) references rpl.SubscriptionTable(TableId) ON DELETE CASCADE
	GO

	
if object_id('rpl.ImportLog') is not null
	drop table rpl.ImportLog
go
create table rpl.ImportLog (
	ImportLogId int not null identity constraint PK_Rpl_ImportLog PRIMARY key clustered
	, SubscriptionId int
	, RvFrom varbinary(8)
	, RvTo varbinary(8)
	, StartDate datetime 
	, EndDate datetime
	, Success bit
	, TotalRows bigint null
	, RvTotalRows bigint NULl
	, TotalKbs bigint
	, Threads tinyint null
	, UseStage bit null constraint df_rpl_ImportLog default (1)
	, message varchar(max),
	rv rowversion
)
go
create index IDX_RPL_ImportLog_SubscriptionId on rpl.ImportLog  (SubscriptionId)   include ([Success], [EndDate],[RvTo], TotalRows)
alter table rpl.ImportLog add constraint fk_rpl_ImportLog_Subscription foreign key (SubscriptionId) references rpl.Subscription(SubscriptionId) ON DELETE CASCADE
go


if object_id('rpl.ImportLogDetail') is not null
	drop table rpl.ImportLogDetail
go
create table rpl.ImportLogDetail (
	ImportLogDetailId int not null identity constraint PK_Rpl_ImportDetailLog PRIMARY key clustered
	, ImportLogId int
	, SchemaName varchar(100)
	, TableName varchar(128)
	, TotalRows bigint
	, TotalKbs bigint
	, rv rowversion
)
go
create index IDX_RPL_ImportLogDetail_SubscriptionId on rpl.ImportLog  (SubscriptionId)   include ([Success], [EndDate],[RvTo], TotalRows)
alter table rpl.ImportLogDetail add constraint fk_rpl_ImportLogDetail_ImportLog foreign key (ImportLogId) references rpl.ImportLog(ImportLogId) ON DELETE CASCADE
go


--PROCS


IF OBJECT_ID(N'spPrintLongSql', 'P') IS NOT NULL
   DROP PROCEDURE dbo.spPrintLongSql
GO
CREATE PROCEDURE dbo.spPrintLongSql( @string nvarchar(max) )
AS
SET NOCOUNT ON
 
set @string = rtrim( @string )
 
declare @cr char(1), @lf char(1)
set @cr = char(13)
set @lf = char(10)
 
declare @len int, @cr_index int, @lf_index int, @crlf_index int, @has_cr_and_lf bit, @left nvarchar(4000), @reverse nvarchar(4000)
set @len = 4000
 
while ( len( @string ) > @len )
begin
   set @left = left( @string, @len )
   set @reverse = reverse( @left )
   set @cr_index = @len - charindex( @cr, @reverse ) + 1
   set @lf_index = @len - charindex( @lf, @reverse ) + 1
   set @crlf_index = case when @cr_index < @lf_index then @cr_index else @lf_index end
   set @has_cr_and_lf = case when @cr_index < @len and @lf_index < @len then 1 else 0 end
   print left( @string, @crlf_index - 1 )
   set @string = right( @string, len( @string ) - @crlf_index - @has_cr_and_lf )
end
 
print @string
go

if object_id('dbo.spExec') is not null
drop proc dbo.spExec
go
create proc dbo.spExec (@sql varchar(max), @debug bit = 0, @exec bit = 1, @raiserror bit=1)
as
begin
	begin try
		if @exec = 1
			exec (@sql)
		if @debug = 1
		begin
			if len(@sql) < 8000
				print @sql
			else 
				exec dbo.spPrintLongSql @sql
			print 'GO'			
		end
	end try
	begin catch
		declare @error varchar(255), @severity int, @state int
		select @error = ERROR_MESSAGE()
			, @severity = ERROR_SEVERITY()
			, @state = ERROR_STATE()
		
		print @sql
		print 'GO'			
		
		if @raiserror = 1
			raiserror (@error, @severity, @state)
		else 
			print error_message()
	end catch
end
go

if OBJECT_ID('rpl.fnEncrypt') is not null
	drop function rpl.fnEncrypt
go
create function rpl.fnEncrypt (@string nvarchar(100) )
returns nvarchar(100)
with encryption
as
begin
	declare @key nvarchar(20)='BR@Z1L===fun!'
	return(convert(nvarchar(100), EncryptByPassPhrase(@key, @string ),1))
end

go

if OBJECT_ID('rpl.fnDecrypt') is not null
	drop function rpl.fnDecrypt
go
create function rpl.fnDecrypt (@v nvarchar(100) )
returns nvarchar(100)
with encryption
as
begin
	declare @key nvarchar(20)='BR@Z1L===fun!'
	declare @bin varbinary(8000) = convert(varbinary(8000), @v, 1)
	return(convert(nvarchar(100), DecryptByPassPhrase(@key, @bin)))

end

go

if OBJECT_ID('rpl.fnEncryptDecryptString') is not null
	drop function rpl.fnEncryptDecryptString
go
CREATE FUNCTION rpl.[fnEncryptDecryptString] (@pClearString VARCHAR(100))
RETURNS NVARCHAR(MAX)
WITH ENCRYPTION
AS
     BEGIN
         DECLARE @vEncryptedString NVARCHAR(100);
         DECLARE @vIdx INT;
         DECLARE @vBaseIncrement INT;
         SET @vIdx = 1;
         SET @vBaseIncrement = 128;
         SET @vEncryptedString = '';
         WHILE @vIdx <= LEN(@pClearString)
             BEGIN
                 SET @vEncryptedString = @vEncryptedString + NCHAR(ASCII(SUBSTRING(@pClearString, @vIdx, 1))^129);
                 SET @vIdx = @vIdx + 1;
             END;
         RETURN @vEncryptedString;
     END; 
GO




if object_id('rpl.spGetSubscriptions') is not null
	drop proc rpl.spGetSubscriptions
go
create proc rpl.spGetSubscriptions (@PriorityGroup int=0, @SubscriptionId int=0) 
as
if @SubscriptionId is null
	set @SubscriptionId = 0

	select *
		, 'Data Source='+ServerName+';Initial Catalog='+DatabaseName+';'
			+ case when isnull(Login,'')='' or isnull(Pass,'')='' then 'Integrated Security=true;' 
				else 'User ID='+Login+';Password='+rpl.fnEncryptDecryptString(Pass)+';' 
				end	as ConnectionString
		, '' as MergeIdentifier 
	from rpl.Subscription s
	outer apply (select max(EndDate) LastDate from rpl.ImportLog l where l.SubscriptionId = s.SubscriptionId and l.Success=1 ) l
	where (isActive=1 
		and (@SubscriptionId = 0  or SubscriptionId = @SubscriptionId)
		and (@PriorityGroup = 0 or isnull(PriorityGroup,0) = @PriorityGroup)
		and (LastDate is null or s.FrequencyInMinutes <= datediff(mi, LastDate, getdate())  )
		)
	or (isnull(PriorityGroup,0) = @PriorityGroup and SubscriptionId = @SubscriptionId) --if user is forcing execution of a subscription then ignore active and time information

	order by SubscriptionSequence, SubscriptionId
go 


if object_id('rpl.spGetRoutineList') is not null
	drop proc rpl.spGetRoutineList
go
create proc rpl.spGetRoutineList (@SubscriptionId int) 
as
	select RoutineName
	from rpl.SubscriptionRoutine
	where isActive=1
	and SubscriptionId = @SubscriptionId
	order by RoutineSequence, RoutineId
go

if object_id('rpl.spGetTableList') is not null
	drop proc rpl.spGetTableList
go
create proc rpl.spGetTableList (@SubscriptionId int) 
as
	select SchemaName, TableName, Initialize, PublisherSchemaName, PublisherTableName, InitialRowCount
	from rpl.SubscriptionTable
	where isActive=1
	and SubscriptionId = @SubscriptionId
	order by InitialRowCount, SchemaName, TableName
go


if object_id('rpl.spReturnGetProcName') is not null
	drop proc rpl.spReturnGetProcName
go
create proc rpl.spReturnGetProcName (@SubscriptionId int, @Table varchar(200), @GetProc varchar(200) output) 
as
	select @GetProc = case when isnull(GetProcName,'') <> '' then GetProcName else 'rpl.spGet_' + coalesce(PublisherSchemaName, SchemaName) +'_'+ coalesce(PublisherTableName, TableName) end
	from rpl.SubscriptionTable
	where SubscriptionId = @SubscriptionId
	and SchemaName+'_'+TableName = @Table
go



if OBJECT_ID('rpl.spGetColumnList') is not null
	drop proc rpl.spGetColumnList
go
create proc rpl.spGetColumnList (@SubscriptionId int, @TableName varchar(200), @UseStage bit=0)
as
set @TableName = replace(replace(replace(@TableName,']',''),'[',''),'#Initialize','')

	select pt.SchemaName
			, pt.TableName
			, 'RplOperation' ColumnName
			, 'char(1)' DataType
			, 1 ColumnLength
			, 0 IsKey
			, 0 IsIdentity
			, 0 ColId
			, 0 ApplyCompression
		from rpl.SubscriptionTable pt 
		where pt.SchemaName+'.'+pt.TableName = @TableName
		and @UseStage = 1 -- append RplOperation column if pulling data to load into stage
	union all	
	select pt.SchemaName
			, pt.TableName
			, pc.ColumnName
			, pc.DataType
			, pc.ColumnLength
			, pc.IsKey
			, pc.IsIdentity
			, pc.ColId
			, pc.ApplyCompression
		from rpl.SubscriptionColumn pc
		join rpl.SubscriptionTable pt on pc.TableId = pt.TableId
		where pt.SchemaName+'.'+pt.TableName = @TableName
		and pt.SubscriptionId = @SubscriptionId
		and pc.ColumnName <> 'rv'
	union all
	select pt.SchemaName
			, pt.TableName
			, 'sourcerv' ColumnName
			, 'varbinary(8)' DataType
			, 8 ColumnLength
			, 0 IsKey
			, 0 IsIdentity
			, 1024 ColId
			, 0 ApplyCompression
		from rpl.SubscriptionTable pt 
		where pt.SchemaName+'.'+pt.TableName = @TableName

	order BY ColId

	if @@ROWCOUNT = 0
	begin
		declare @error varchar(255)= 'Table '+@TableName+' returned no columns!'
		raiserror(@error,16,1)
	end

go



if object_id('rpl.spTruncateStage') is not null
	drop proc rpl.spTruncateStage 
go
create proc rpl.spTruncateStage (@SubscriptionId int, @Table varchar(200)='', @debug bit=0)
as
set nocount on
set @Table = isnull(replace(replace(@Table,']',''),'[',''),'')

declare @sql varchar(max)
declare t_cursor cursor fast_forward for
	select 'rpl.stg_'+SchemaName+'_'+TableName 
	from rpl.SubscriptionTable 
	where isActive=1 
	and SubscriptionId = @SubscriptionId 
	and (@Table = '' or SchemaName+'.'+TableName = @Table )
	order by 1
open t_cursor
fetch next from t_cursor into @table
while @@FETCH_STATUS=0
begin
	set @sql = 'if exists (select * from '+@table+') 
					truncate table '+@table+''
	exec dbo.spExec @sql, @debug
	fetch next from t_cursor into @table
end
close t_cursor
deallocate t_cursor

GO
if object_id('rpl.fnGetStgRowCount') is not null
	drop function rpl.fnGetStgRowCount
go
create function rpl.fnGetStgRowCount (@UseStage bit, @SubscriptionId int) 
returns table 
as return(
	SELECT sch.Name SchemaName, tbl.name TableName,   max(PA.rows) TotalRows, sum(au.total_pages * 8) size_kbs
	FROM sys.tables TBL with (nolock)
	INNER JOIN sys.schemas sch on sch.schema_id = tbl.schema_id
	INNER JOIN sys.partitions PA with (nolock) ON TBL.object_id = PA.object_id
	INNER JOIN sys.indexes IDX with (nolock) ON PA.object_id = IDX.object_id	AND PA.index_id = IDX.index_id
	LEFT JOIN  sys.allocation_units AS au ( NOLOCK ) ON (au.type IN (1, 3) AND au.container_id = PA.hobt_id) 
            OR  (au.type = 2  AND au.container_id = PA.partition_id) 
	WHERE IDX.index_id < 2--get cix or head 
	and (
		 exists (
			select * from rpl.SubscriptionTable st 
			where @UseStage = 1
			and st.SubscriptionId = @SubscriptionId
			and st.IsActive=1
			and sch.name = 'rpl' 
			and tbl.name = 'stg_'+st.SchemaName + '_'+st.TableName)
		or exists (select * from rpl.SubscriptionTable  st 
			where @UseStage = 0 
			and st.IsActive=1
			and st.SubscriptionId = @SubscriptionId
			and st.SchemaName  = SCH.name 
			AND st.TableName = TBL.name)
		)
	group by sch.Name, tbl.name 
)
GO
/*
select * from rpl.fnGetStgRowCount(0)
select * from rpl.fnGetStgRowCount(1)
*/
go





if object_id('rpl.spLogTable') is not null
	drop proc rpl.spLogTable 
go
create proc rpl.spLogTable (@subscriptionId int, @table varchar(100), @rows bigint)
as 

declare @ImportLogId int
	
--get current import process open
select top 1 @ImportLogId = ImportLogId
	from rpl.ImportLog
	where SubscriptionId = @SubscriptionId 
	and EndDate is null 
	order by ImportLogId desc

insert into rpl.ImportLogDetail (ImportLogId, TableName, TotalRows)
values (@ImportLogId, @table, @rows)

GO



if object_id('rpl.spEnd') is not null
	drop proc rpl.spEnd 
go
create proc rpl.spEnd (@SubscriptionId int,  @message varchar(max), @success bit=1, @TotalRows bigint=null output, @TotalKbs bigint=null output)
as 

declare @ImportLogId int
	, @UseStage bit=1--by default the rows will come from staging tables

--get current import process open
select top 1 @ImportLogId = ImportLogId
		, @UseStage = UseStage 
	from rpl.ImportLog
	where SubscriptionId = @SubscriptionId 
	and EndDate is null 
	order by ImportLogId desc

--collect rowcounts per table
delete from rpl.ImportLogDetail where ImportLogId = @ImportLogId
and TableName not in (select RoutineName from rpl.SubscriptionRoutine);

with a as (
	select @ImportLogId ImportLogId, SchemaName, TableName, TotalRows, size_kbs
	from rpl.fnGetStgRowCount (@UseStage, @SubscriptionId)
)
insert into rpl.ImportLogDetail (ImportLogId, SchemaName, TableName, TotalRows, TotalKbs)
select a.ImportLogId, a.SchemaName, a.TableName, a.TotalRows, a.size_kbs
from a;

select @TotalRows = sum(TotalRows) 
		, @TotalKbs = sum(TotalKbs)
	from rpl.ImportLogDetail 
	where ImportLogId = @ImportLogId

if @success = 1 and (@TotalRows > 0 or exists (select * from rpl.SubscriptionRoutine where SubscriptionId = @SubscriptionId and IsActive=1) ) 
begin
	update rpl.Subscription set Initialize = 0
	where SubscriptionId = @SubscriptionId
	and Initialize = 1

	update st set Initialize = 0
	from rpl.SubscriptionTable st
	where SubscriptionId = @SubscriptionId
	and Initialize = 1
	--make sure the table was loaded
	and exists (select * from rpl.ImportLogDetail ld 
		where ld.ImportLogId = @ImportLogId
		and  (		(ld.TableName  =  st.TableName and ld.SchemaName = st.SchemaName)
				or	(ld.SchemaName='rpl' and ld.TableName = 'stg_'+st.SchemaName+'_'+st.TableName)
				)
		)
end

--close import process
update rpl.ImportLog set 
	message = @message
	, EndDate = getdate()
	, Success=@success
	, TotalRows = isnull(@TotalRows,0)
	, UseStage = @UseStage
	, TotalKbs = @TotalKbs
where ImportLogId = @ImportLogId

GO


if object_id('rpl.spGetLastSuccessfullRv') is not null
	drop proc rpl.spGetLastSuccessfullRv
go
create proc rpl.spGetLastSuccessfullRv (@SubscriptionId int, @rv varchar(20) output)
AS
declare @rvtobin varbinary(8)

if 1 = isnull((select DoubleReadRVRange from  rpl.Subscription where SubscriptionId = @SubscriptionId),0)
begin 
	--SQL may assign rowversions before the values are saved to the rv index, which causes the get procs to skip rows in rare ocasions
	-- to prevent this issue we double read ranges at the distributor, which gets the last successfull rv from 2 runs ago 
	SELECT top 2 @rvtobin = rvto from rpl.ImportLog where Success = 1 and SubscriptionId = @SubscriptionId order by ImportLogId desc
end
else
begin
	--get rvto from last sucessfull run
	select top 1 @rvtobin = rvto from rpl.ImportLog where Success = 1 and SubscriptionId = @SubscriptionId order by importlogid desc
end

IF @rv IS NULL 
	SET @rv = '0x'

--convert to string
SET @rv = CONVERT(VARCHAR(20), @rvtobin, 1)

IF @rv IS NULL 
	SET @rv = '0x'

go

if object_id('rpl.spStart') is not null
	drop proc rpl.spStart 
go
create proc rpl.spStart (@SubscriptionId int, @rvto varchar(20), @rvfrom varchar(20) output, @threads tinyint=1, @RvTotalRows bigint output)
as 
declare @rvfrom_bin varbinary(8) 
	, @rvto_bin varbinary(8) = convert(varbinary(8), @rvto, 1)
	, @UseStage bit=1
	, @Initialize bit=0

select @Initialize = Initialize from rpl.Subscription where SubscriptionId = @SubscriptionId

if @Initialize = 1
begin 
	set @rvfrom_bin = 0x
	set @UseStage = 0
end
else
begin
	--get rvfrom as rvto from last sucessfull run
	exec [rpl].[spGetLastSuccessfullRv] @SubscriptionId = @SubscriptionId, @rv = @rvfrom output

	set @rvfrom_bin  = convert(varbinary(8), @rvfrom, 1)
	if @rvfrom = '0x'
		set @UseStage = 0
end

--set @rvfrom = convert(varchar(20), @rvfrom_bin, 1)

set @RvTotalRows = cast(@rvto_bin as bigint) - cast(@rvfrom_bin as bigint)

insert into rpl.ImportLog(SubscriptionId, RvFrom, RvTo, StartDate, Success, RvTotalRows, threads, UseStage)
values (@SubscriptionId, @rvfrom_bin, @rvto_bin, getdate(), 0, @RvTotalRows, @threads, @UseStage)

GO

if object_id('rpl.spCleanProdTables') is not null
	DROP PROC rpl.spCleanProdTables
GO
create proc rpl.spCleanProdTables (@SubscriptionId int, @debug bit=0, @Table varchar(200)='')
as
set nocount on
set @Table = isnull(replace(replace(@Table,']',''),'[',''),'')

declare @sql varchar(max)
		,  @FK_Schema 	varchar(128)
		,  @FK_Table	varchar(128)
		,  @FK_Name		varchar(128)
		,  @FK_Column	varchar(128)
		,  @PK_Schema	varchar(128)
		,  @PK_Table	varchar(128)
		,  @PK_Column	varchar(128)
		,  @SchemaName  varchar(128)
		,  @TableName  varchar(128)
		, @UPDATE_RULE varchar(128)
		, @DELETE_RULE varchar(128)
		
--EXEC rpl.spTruncateStage @SubscriptionId, @Table, @Debug

begin try
	begin transaction

	/*
	update rpl.ImportLog set Success = 0 
	where SubscriptionId = @SubscriptionId
	and Success = 1
	*/

	IF OBJECT_ID('tempdb..#Fks') IS NOT NULL
		DROP TABLE #Fks

	; with constraint_columns as (
			select kf.TABLE_SCHEMA, kf.TABLE_NAME, KF.CONSTRAINT_NAME 
			 ,STUFF(
                   (SELECT
                        ', ' + kf2.COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KF2
                        WHERE KF2.CONSTRAINT_NAME = KF.CONSTRAINT_NAME
                        ORDER BY kf2.ORDINAL_POSITION
                        FOR XML PATH(''), TYPE
                   ).value('.','varchar(max)')
                   ,1,2, ''
              ) AS COLUMN_NAME
			from INFORMATION_SCHEMA.KEY_COLUMN_USAGE KF
			group by kf.TABLE_SCHEMA, kf.TABLE_NAME, KF.CONSTRAINT_NAME
	)
	SELECT RC.CONSTRAINT_NAME FK_Name
			--, RC.UNIQUE_CONSTRAINT_NAME PkName
			, RC.MATCH_OPTION MatchOption
			, RC.UPDATE_RULE UpdateRule
			, RC.DELETE_RULE DeleteRule
			, rc.UNIQUE_CONSTRAINT_SCHEMA , rc.UNIQUE_CONSTRAINT_NAME

			, KP.TABLE_SCHEMA PK_Schema
			, KP.Table_Name PK_Table
			, KP.COLUMN_NAME PK_Column

			, KF.TABLE_SCHEMA FK_Schema
			, KF.TABLE_NAME FK_Table
			, KF.COLUMN_NAME FK_Column
			, rc.UPDATE_RULE
			, rc.DELETE_RULE
			--select *
	into #Fks
	--select *
	FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
	join constraint_columns kp on kp.TABLE_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA AND kp.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME 
	join constraint_columns kF on kf.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA AND kf.CONSTRAINT_NAME = rc.CONSTRAINT_NAME 
	--fk references one of the tables being truncated
	where (@Table = '' or kp.TABLE_SCHEMA+'.'+kp.TABLE_NAME = @Table )
	and exists (
			select * from rpl.SubscriptionTable s 
			join dbo.VIV_TotalRows r on s.SchemaName = r.SchemaName and s.TableName = r.TableName
			where IsActive=1 
			and r.TotalRows > 0
			and SubscriptionId = @SubscriptionId and s.SchemaName = KP.TABLE_SCHEMA and s.TableName = kp.TABLE_NAME)

	if @debug = 1 
		select * from #Fks

	--drop FKs
	DECLARE fk_cursor CURSOR FAST_FORWARD FOR
		SELECT FK_Schema 
			,  FK_Table
			,  FK_Name
			,  FK_Column
			,  PK_Schema
			,  PK_Table
			,  PK_Column
		--select *
		FROM #Fks
	OPEN fk_cursor
	FETCH NEXT FROM fk_cursor INTO  @FK_Schema,  @FK_Table,  @FK_Name,  @FK_Column,  @PK_Schema,  @PK_Table,  @PK_Column
	WHILE @@FETCH_STATUS=0
	BEGIN 
		SET @sql= 'alter table ['+@FK_Schema+'].['+@FK_Table+'] drop constraint ['+@FK_Name+']'
		EXEC dbo.spExec @SQL, @debug,1,1
		FETCH NEXT FROM fk_cursor INTO  @FK_Schema,  @FK_Table,  @FK_Name,  @FK_Column,  @PK_Schema,  @PK_Table,  @PK_Column
	END 
	CLOSE fk_cursor
	DEALLOCATE fk_cursor

	--Truncate tables
	declare t_cursor cursor fast_forward for
		select SchemaName, TableName 
		from rpl.SubscriptionTable 
		where SubscriptionId = @SubscriptionId 
		and IsActive=1 
		and (@Table = '' or SchemaName+'.'+ TableName = @Table )
		order by 1
	open t_cursor
	fetch next from t_cursor into @SchemaName, @TableName
	while @@FETCH_STATUS=0
	begin
		begin try
			set @sql = 'if exists (select * from ['+@SchemaName+'].['+@TableName+']) truncate table ['+@SchemaName+'].['+@TableName+']'
			EXEC dbo.spExec @SQL, @debug,1,1
		end try
		begin catch
			print  ERROR_MESSAGE()
			--if truncate fails then we atempt a delete, for instance if table is replicated or is used by views with schemabinding 
			set @sql = 'delete from ['+@SchemaName+'].['+@TableName+']'
			EXEC dbo.spExec @SQL, @debug,1,1
		end catch
		fetch next from t_cursor into @SchemaName, @TableName
	end
	close t_cursor
	deallocate t_cursor

	--Recreate FKs
	DECLARE fk_cursor CURSOR FAST_FORWARD FOR
		SELECT FK_Schema 
			,  FK_Table
			,  FK_Name
			,  FK_Column
			,  PK_Schema
			,  PK_Table
			,  PK_Column
			, UPDATE_RULE, DELETE_RULE
		--select *
		FROM #Fks
	OPEN fk_cursor
	FETCH NEXT FROM fk_cursor INTO  @FK_Schema,  @FK_Table,  @FK_Name,  @FK_Column,  @PK_Schema,  @PK_Table,  @PK_Column, @UPDATE_RULE, @DELETE_RULE
	WHILE @@FETCH_STATUS=0
	BEGIN 
		SET @sql= 'alter table ['+@FK_Schema+'].['+@FK_Table+'] with nocheck add constraint ['+@FK_Name + '] foreign key ('+@FK_Column+') references ['+@PK_Schema+'].['+@PK_Table+'] ('+@PK_Column+') ON DELETE '+@DELETE_RULE+' ON UPDATE '+@UPDATE_RULE
		EXEC dbo.spExec @SQL, @debug,1,1
		FETCH NEXT FROM fk_cursor INTO  @FK_Schema,  @FK_Table,  @FK_Name,  @FK_Column,  @PK_Schema,  @PK_Table,  @PK_Column, @UPDATE_RULE, @DELETE_RULE
	END 
	CLOSE fk_cursor
	DEALLOCATE fk_cursor

	commit transaction
end try
begin catch
	rollback transaction
	declare @error varchar(255), @severity int, @state int
		select @error = ERROR_MESSAGE()
			, @severity = ERROR_SEVERITY()
			, @state = ERROR_STATE()
		
	raiserror (@error, @severity, @state)
end catch

set nocount off
go
--exec rpl.spCleanProdTables 1,1 

go

if object_id('rpl.spDisableIndexes') is not null
	drop proc rpl.spDisableIndexes
go
create proc rpl.spDisableIndexes (@SubscriptionId int, @debug bit=0, @Table varchar(100)= '' )
as
declare @sql varchar(max) = ''

--disable non clustered indexes
declare t_cursor cursor fast_forward for
	select 'alter index ['+i.name+'] on ['+rt.SchemaName+'].['+rt.TableName+ '] disable '  
	FROM rpl.SubscriptionTable rt
	join sys.schemas s on s.name = rt.SchemaName
	join sys.tables t on t.schema_id = s.schema_id and t.name = rt.TableName
	join sys.indexes i on i.object_id = t.object_id and i.index_id >=2
	where rt.isActive =  1 and i.is_disabled=0
	and SubscriptionId = @SubscriptionId	
	ORDER BY rt.SchemaName, rt.TableName, i.name
open t_cursor
fetch next from t_cursor into @sql
while @@FETCH_STATUS=0
begin
	exec dbo.spExec @sql, @debug
	fetch next from t_cursor into @sql
end
close t_cursor
deallocate t_cursor


GO
--disable constraints
if object_id('rpl.spDisableConstraints') is not null
	drop proc rpl.spDisableConstraints
go
create proc rpl.spDisableConstraints (@SubscriptionId int, @debug bit=0)
as
declare @sql varchar(max) = ''

--disable constraints
declare t_cursor cursor fast_forward for
	select 'alter table ['+SchemaName+'].['+TableName+ '] nocheck constraint ['+  CONSTRAINT_NAME+']'
	FROM rpl.SubscriptionTable st
	cross apply (
			select distinct kf.TABLE_SCHEMA, kf.TABLE_NAME, KF.CONSTRAINT_NAME
			FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
			join INFORMATION_SCHEMA.KEY_COLUMN_USAGE KF on kf.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA AND kf.CONSTRAINT_NAME = rc.CONSTRAINT_NAME 
			where kf.TABLE_SCHEMA = st.SchemaName and  kf.TABLE_NAME = st.TableName
	) const 
	where isActive =  1 
open t_cursor
fetch next from t_cursor into @sql
while @@FETCH_STATUS=0
begin
	exec dbo.spExec @sql, @debug
	fetch next from t_cursor into @sql
end
close t_cursor
deallocate t_cursor

go

--enable constraints
if object_id('rpl.spEnableConstraints') is not null
	drop proc rpl.spEnableConstraints
go
create proc rpl.spEnableConstraints (@SubscriptionId int, @debug bit=0)
as
declare @sql varchar(max) = ''
declare t_cursor cursor fast_forward for
	select 'alter table ['+SchemaName+'].['+TableName+ '] check constraint ['+  CONSTRAINT_NAME + ']'
	FROM rpl.SubscriptionTable st
	cross apply (
			select distinct kf.TABLE_SCHEMA, kf.TABLE_NAME, KF.CONSTRAINT_NAME
			FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
			join INFORMATION_SCHEMA.KEY_COLUMN_USAGE KF on kf.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA AND kf.CONSTRAINT_NAME = rc.CONSTRAINT_NAME 
			where kf.TABLE_SCHEMA = st.SchemaName and  kf.TABLE_NAME = st.TableName
	) const 
	where isActive =  1 
open t_cursor
fetch next from t_cursor into @sql
while @@FETCH_STATUS=0
begin
	exec dbo.spExec @sql, @debug
	fetch next from t_cursor into @sql
end
close t_cursor
deallocate t_cursor
GO
--exec rpl.spEnableConstraints 1,1
go


--enable constraints
if object_id('rpl.spEnableIndexes') is not null
	drop proc rpl.spEnableIndexes
go
create proc rpl.spEnableIndexes (@SubscriptionId int, @tablename varchar(100)='', @debug bit=0, @exec bit=1)
as
set @TableName = replace(replace(replace(@TableName,']',''),'[',''),'#Initialize','')

declare @sql varchar(max) = ''
--rebuild non clustered indexes
declare t_cursor cursor fast_forward for
	select 'alter index ['+i.name+'] on ['+rt.SchemaName+'].['+rt.TableName+ '] rebuild '  
	FROM rpl.SubscriptionTable rt
	join sys.schemas s on s.name = rt.SchemaName
	join sys.tables t on t.schema_id = s.schema_id and t.name = rt.TableName
	join sys.indexes i on i.object_id = t.object_id and i.index_id >=2
	where rt.isActive =  1 and i.is_disabled=1
	and rt.SubscriptionId = @SubscriptionId	
	and (@tablename = '' or @tablename = null 
		or rt.SchemaName+'.'+rt.TableName = @TableName
		or rt.TableName = @TableName
	)
	ORDER BY rt.SchemaName, rt.TableName, i.name
open t_cursor
fetch next from t_cursor into @sql
while @@FETCH_STATUS=0
begin
	exec dbo.spExec @sql, @debug
	fetch next from t_cursor into @sql
end
close t_cursor
deallocate t_cursor

GO


if OBJECT_ID('rpl.spSubscribeRoutine') is not null
	drop proc rpl.spSubscribeRoutine
go
create proc rpl.spSubscribeRoutine (@SubscriptionId int, @RoutineName varchar(128), @Sequence tinyint=0)
as
	declare @RoutineId int 
	select @RoutineId = RoutineId from rpl.SubscriptionRoutine where SubscriptionId = @SubscriptionId and RoutineName = @RoutineName
	if @RoutineId is null
		insert into rpl.SubscriptionRoutine (SubscriptionId, RoutineName, IsActive, RoutineSequence)
		VALUES (@SubscriptionId, @RoutineName, 1, @Sequence)
	else 
		update rpl.SubscriptionRoutine set RoutineSequence = @Sequence, IsActive= 1 
		where SubscriptionId = @SubscriptionId
go

if OBJECT_ID('rpl.spUnSubscribeRoutine') is not null
	drop proc rpl.spUnSubscribeRoutine
go
create proc rpl.spUnSubscribeRoutine (@SubscriptionId int, @RoutineName varchar(128))
as
		delete from  rpl.SubscriptionRoutine 
		where SubscriptionId = @SubscriptionId
		and RoutineName = @RoutineName
go




if OBJECT_ID('rpl.spUnSubscribeTable') is not null
	drop proc rpl.spUnSubscribeTable
go
create proc rpl.spUnSubscribeTable (@SubscriptionId int, @SchemaName varchar(100), @TableName varchar(100), @debug bit=0)
as	
declare @sql varchar(max)

if not exists (select * from rpl.Subscription where SubscriptionId = @SubscriptionId )
begin
	raiserror('Invalid SubscriptionId!', 16,0)
	return(0)
end

if not exists (select * from rpl.SubscriptionTable where SchemaName= @SchemaName and TableName = @TableName and SubscriptionId = @SubscriptionId)
begin
	set @sql = 'Table '+@SchemaName+'.'+@TableName+' is not in Subscription!'
	raiserror(@sql, 16,0)
	return(0)
end

begin try
	begin transaction

	--remove stg table
	set @sql = 'if object_id(''rpl.stg_'+@SchemaName+'_'+@TableName+''') is not null drop table rpl.stg_'+@SchemaName+'_'+@TableName
	exec spExec @sql, @debug

	--remove get proc
	set @sql = 'if object_id(''rpl.spMerge_'+@SchemaName+'_'+@TableName+''') is not null drop proc rpl.spMerge_'+@SchemaName+'_'+@TableName
	exec spExec @sql, @debug

	delete rc from  rpl.SubscriptionTable rt
	join rpl.SubscriptionColumn rc on rc.tableid = rt.TableId
	where rt.SchemaName = @SchemaName and rt.TableName = @TableName

	delete rt from  rpl.SubscriptionTable rt where SchemaName = @SchemaName and TableName = @TableName

	set @sql = 'Table '+@SchemaName+'.'+@TableName+' unsubscribed sucessfully!'
	print @sql

	commit transaction
end try
begin catch
	rollback transaction
	declare @error varchar(255), @severity int, @state int
		select @error = ERROR_MESSAGE()
			, @severity = ERROR_SEVERITY()
			, @state = ERROR_STATE()
		
	raiserror (@error, @severity, @state)
end catch
go


if OBJECT_ID('rpl.spSubscribeTable') is not null
	drop proc rpl.spSubscribeTable
go
create proc rpl.spSubscribeTable (@SubscriptionId int, @SchemaName varchar(100), @TableName varchar(100), @debug bit =0, @exec bit=1
	, @PublisherSchemaName varchar(100)=null, @PublisherTableName varchar(100)=null
	, @KeyName varchar(100) ='', @IgnoreKeyColumn varchar(100) ='', @IgnoreColumns varchar(1000) =''
	, @GetProcName varchar(100)='')
as
DECLARE @sql varchar(max)
	, @TableId int=0, @PkName varchar(100)=''
	, @KeyColumns varchar(max)=''
	, @join  varchar(max)=''
	, @ColumnDefinitions varchar(max)=''
	, @InsertColumns varchar(max)=''
	, @ValueColumns VARCHAR(max)= '' 
	, @UpdateColumns VARCHAR(max)= ''
	, @SelectColumns VARCHAR(max)= ''
	, @StgTableName varchar(100)=''
	, @ProcName varchar(100)=''
	, @Has_Identity bit
	, @message varchar(255)
	, @IsCustom bit

if not exists (select * from rpl.Subscription where SubscriptionId = @SubscriptionId )
begin
	raiserror('Invalid SubscriptionId!', 16,0)
	return(0)
end

if not exists (select * from sys.tables t
			inner join sys.schemas s on t.schema_id = s.schema_id
			where s.name = @SchemaName
			and t.name  = @TableName )
begin
	raiserror('Invalid table Name!', 16,0)
	return(0)
end


if @PublisherSchemaName is null or @PublisherSchemaName = ''
	set @PublisherSchemaName = @SchemaName

if @PublisherTableName is null or @PublisherTableName = ''
	set @PublisherTableName = @TableName

set nocount on	
begin try
	begin transaction
			
--get table properties
	; with t as (
		SELECT  s.name SchemaName
				, t.name TableName
				, i.name PKName
				, count(*) KeyCount
				, OBJECTPROPERTY ( OBJECT_ID(s.name+'.'+t.name) , 'TableHasIdentity' )  Has_Identity
				, TotalRows
			from sys.tables t
			inner join sys.schemas s on t.schema_id = s.schema_id
			cross apply (
				 select top 1 *
				 from sys.indexes i 
				 where i.object_id = t.object_id
				 and is_unique = 1
				 and (isnull(@KeyName,'') = '' or i.name = @keyname) 
				 order by i.is_primary_key desc, i.is_unique_constraint desc
				) i
			outer apply (
				SELECT   SUM(PART.rows) TotalRows
				FROM sys.tables TBL with (nolock)
				INNER JOIN sys.schemas sch on sch.schema_id = tbl.schema_id
				INNER JOIN sys.partitions PART with (nolock) ON TBL.object_id = PART.object_id
				INNER JOIN sys.indexes IDX with (nolock) ON PART.object_id = IDX.object_id	AND PART.index_id = IDX.index_id
				WHERE IDX.index_id < 2--get cix or head 
				and sch.Name = @SchemaName
				and tbl.Name = @TableName
			) r
			inner join sys.index_columns ic on ic.object_id = t.object_id AND i.index_id = ic.index_id
			inner join sys.columns c on c.object_id = t.object_id AND ic.column_id = c.column_id
			inner join sys.types p on p.system_type_id = c.system_type_id
			where ic.key_ordinal > 0
			and s.name = @SchemaName
			and t.name  = @TableName 
			group by s.name, t.name, i.name, r.TotalRows
		)
	merge rpl.SubscriptionTable p using t on p.SchemaName = t.SchemaName and p.TableName = t.TableName and p.SubscriptionId = @SubscriptionId
	when not matched by target then insert (SubscriptionId, SchemaName, TableName, PkName, KeyCount, has_identity, Initialize, PublisherSchemaName, PublisherTableName, InitialRowCount, GetProcName, IgnoreKeyColumn, IgnoreColumns)
		values (@SubscriptionId, t.SchemaName, t.TableName, t.PkName, t.KeyCount, t.has_identity, 0, @PublisherSchemaName, @PublisherTableName, t.TotalRows, @GetProcName, @IgnoreKeyColumn, @IgnoreColumns)
	when matched then update set 
		PkName = t.PKName
		, KeyCount = t.KeyCount
		, has_identity = t.has_identity
		, PublisherSchemaName= @PublisherSchemaName
		, PublisherTableName = @PublisherTableName
		, Initialize = 0
		, InitialRowCount = t.TotalRows
		, SubscriptionId = @SubscriptionId
		, GetProcName = @GetProcName
		, IgnoreKeyColumn = @IgnoreKeyColumn
		, IgnoreColumns = @IgnoreColumns
		;

	select @TableId = TableId
		, @PkName = PkName
		, @Has_Identity = Has_Identity 
		, @IsCustom = IsCustom
	from  rpl.SubscriptionTable rt 
	where SchemaName = @SchemaName and TableName = @TableName

	if @PkName is null or @PkName = ''
	begin
		set @message = 'Table '+@SchemaName+'.'+@TableName+ ' does not  have a primary key!!'
		raiserror (@message,16,1)
	end
	else if @IsCustom = 1
	begin
		set @sql = 'Table '+@SchemaName+'.'+@TableName+' is custom so subscription must be done manually!'
		raiserror (@sql,16,1)
		return(0)
	end

--ADD SOURCERV COLUMN if it does not exist
	if not exists (select * from sys.tables t
				join sys.schemas s on t.schema_id = s.schema_id
				join sys.columns c on c.[object_id] = t.[object_id] and c.name = 'sourcerv'
				where s.Name = @SchemaName 
				and t.Name = @TableName)
	begin
		set @sql = 'alter table ['+@SchemaName+'].['+@TableName+'] add sourcerv varbinary(8) ' 
		exec spExec @sql, @debug, @exec
		--set @sql = 'create index IX_'+@SchemaName+'_'+@TableName+'_sourcerv on ['+@SchemaName+'].['+@TableName+'] (sourcerv) ' 
		--exec spExec @sql, @debug, @exec
	end

--get column properties
	delete sc from rpl.SubscriptionColumn sc
	join rpl.SubscriptionTable st on sc.Tableid = st.TableId
	where st.SchemaName = @SchemaName and st.TableName = @TableName
	and st.SubscriptionId = @SubscriptionId

	; with c as (
			SELECT rt.TableId
				, c.NAME ColumnName
				, case when ty.name in ('nvarchar','nchar', 'varchar', 'char', 'varbinary') and c.max_length = -1 then  ty.name + ' (max)'
					when ty.name in ('nvarchar','nchar') then ty.name + ' ('+ cast(c.max_length / 2 as varchar) +')'
					when ty.name in ('varchar','char', 'varbinary') then ty.name + ' ('+ cast(c.max_length as varchar) +')'
					when ty.name in ('numeric', 'decimal') then ty.name + ' ('+ cast(c.precision as varchar)+ ','+ cast(c.scale as varchar) +')'
					when ty.name in ('timestamp','rowversion') then 'varbinary(8)'
					else ty.name end DataType
				, case when ty.name in ('nvarchar','nchar') then c.max_length / 2 
					else c.max_length end ColumnLength
				, case when exists (
							SELECT *
							from sys.indexes i 
							inner join sys.index_columns ic on ic.object_id = i.object_id AND ic.index_id = i.index_id 
							inner join sys.columns icc on icc.object_id = ic.object_id AND icc.column_id = ic.column_id
							where i.object_id = t.object_id
							and i.name = rt.PkName
							and icc.name = c.name 
							and ic.key_ordinal > 0
							and c.name <> isnull(@IgnoreKeyColumn,'')
					) 
					then 1 else 0 end IsKey
				, c.column_id ColId
				, c.is_identity IsIdentity
				--select *
			from sys.tables t
			inner join sys.schemas s on t.schema_id = s.schema_id
			inner join sys.columns c on c.object_id = t.object_id
			inner join sys.types ty on ty.user_type_id = c.user_type_id
			inner join rpl.SubscriptionTable rt on rt.SchemaName = s.name and rt.TableName = t.name 
			where c.is_computed = 0		
			and s.name = @SchemaName
			and t.name  = @TableName 
			and c.name not in ('sourcerv','rv')
			and charindex(c.name, ','+isnull(@IgnoreColumns,0)+',')=0
			and SubscriptionId = @SubscriptionId
			)
	merge rpl.SubscriptionColumn p using c on p.TableId = c.TableId and c.ColId = p.ColId
	when not matched by target then insert (TableId, ColumnName, DataType, ColumnLength, IsKey, ColId, IsIdentity)
		values (c.TableId, c.ColumnName, c.DataType, c.ColumnLength, c.IsKey, c.ColId, c.IsIdentity) 
	when matched then update set
		 ColumnName = c.ColumnName
		 , DataType = c.DataType
		 , ColumnLength = c.ColumnLength
		 , IsKey = c.IsKey
		 , IsIdentity = c.IsIdentity
		;

	if @debug=1
		select sc.* from rpl.SubscriptionColumn sc
		join rpl.SubscriptionTable st on sc.Tableid = st.TableId
		where st.SchemaName = @SchemaName and st.TableName = @TableName
		and st.SubscriptionId = @SubscriptionId
		order by colid

--build lists
	select @StgTableName = 'rpl.stg_'+@SchemaName+'_'+@TableName
		, @ProcName = 'rpl.spMerge_'+@SchemaName+'_'+@TableName

	select @KeyColumns = @KeyColumns  + case when @KeyColumns = '' then '' else ', ' end + '[' + ColumnName+']'
		, @join = @join + case when @join = '' then '' else ' and ' end + 't.[' + ColumnName+']' + ' = s.[' + ColumnName+']'
	from rpl.SubscriptionColumn 
	where TableId = @TableId
	and IsKey=1
	order by ColId
		
	select @ColumnDefinitions = @ColumnDefinitions + case when @ColumnDefinitions = '' then '' else ', ' end + '[' + ColumnName+']' + ' ' + case when ApplyCompression = 1 then 'varbinary(max)' else DataType end 
	from rpl.SubscriptionColumn 
	where TableId = @TableId
	order by ColId

	select @InsertColumns =  @InsertColumns + case when @InsertColumns = '' then '' else ', ' end +'[' + ColumnName+']'
		, @ValueColumns =  @ValueColumns + case when @ValueColumns = '' then '' else ', ' end +'s.[' + ColumnName+']'
		, @SelectColumns =  @SelectColumns + case when @SelectColumns = '' then '' else ', ' end + case when ApplyCompression = 1 then 'cast(DECOMPRESS(['+ColumnName+']) as '+DataType+') as ['+ColumnName+']' else '['+ColumnName+']' end
	from rpl.SubscriptionColumn 
	where TableId = @TableId
		order by ColId

	select  @UpdateColumns = @UpdateColumns + case when @UpdateColumns = '' then '' else ', ' end + ' ['+ColumnName+'] = s.['+ColumnName+']'
	from rpl.SubscriptionColumn 
	where TableId = @TableId
	and IsIdentity = 0 
	and IsKey = 0
	order by ColId

--create stg table
	set @sql  = 'if object_id ('''+@StgTableName+''') is not null 
			drop table '+@StgTableName
	exec spExec @sql, @debug, @exec

	set @sql = '
			create table '+@StgTableName+' (
				RplOperation char(1),
				 '+@ColumnDefinitions+'
				 , sourcerv varbinary(8)
			)'
	exec spExec @sql, @debug, @exec
	
	set @sql  = 'create unique clustered index ucx_'+replace(@StgTableName,'.','_')+ ' on ' + @StgTableName + '('+@KeyColumns+')' 
	exec spExec @sql, @debug, @exec
	
--create merge proc
	set @sql  = 'if object_id ('''+@ProcName+''') is not null 
			drop proc '+@ProcName
	exec spExec @sql, @debug, @exec

	set @sql = 'create proc '+@ProcName + '
as 
'
	IF @Has_Identity = 1
		SET @sql = @sql + '
set identity_insert ['+@SchemaName+'].['+@TableName+'] on
'
	SET @sql = @sql + '
update t set sourcerv = s.sourcerv 
from ['+@SchemaName+'].['+@TableName+'] t
join '+@StgTableName+' s on s.RplOperation = ''D'' and '+@join+'

delete t
from ['+@SchemaName+'].['+@TableName+'] t
join '+@StgTableName+' s on s.RplOperation = ''D'' and '+@join+'

; with s as (
	select '+@SelectColumns+', RplOperation, SourceRv
	from  '+@StgTableName+'
	where RplOperation <> ''D''
)
merge ['+@SchemaName+'].['+@TableName+'] t
using s on '+@join+'
when matched and (t.sourcerv <> s.sourcerv or t.sourcerv is null) then update set 
	'+@UpdateColumns+'
	'+case when len(@UpdateColumns)>1 then ' ,' else '' end+ 'sourcerv = s.sourcerv 
when not matched by target and s.RplOperation <> ''D'' then insert ('+@InsertColumns+', sourcerv)
values ('+@ValueColumns+', s.sourcerv); 

'

	IF @Has_Identity = 1
		SET @sql = @sql + 'set identity_insert ['+@SchemaName+'].['+@TableName+'] off
'
	exec spExec @sql, @debug, @exec
		
	commit transaction
	print 'Table '+@SchemaName+'.'+@TableName+ ' subscribed successfully!'
end try
begin catch
	rollback transaction
	declare @error varchar(255), @severity int, @state int
		select @error = ERROR_MESSAGE()
			, @severity = ERROR_SEVERITY()
			, @state = ERROR_STATE()

	set @sql = 'Table '+@SchemaName+'.'+@TableName+' subscription failed!'
	print @sql
		
	raiserror (@error, @severity, @state)
end catch

set nocount off
go


if object_id('rpl.spCreateSubscription') is not null
	drop proc rpl.spCreateSubscription
go
create proc [rpl].[spCreateSubscription] (
	@Name varchar(100)= null
	, @PriorityGroup tinyint=1
	, @Server varchar(100)
	, @Database varchar(100)
	, @Frequency int=0
	, @SubscriptionId int=null output
	, @Debug bit=0
	, @Login varchar(100) = null
	, @Password varchar(100)=null 
	)
as
/*
select @SubscriptionId = SubscriptionId
	from rpl.Subscription
	where ServerName = @Server
	and DatabaseName = @Database
	and FrequencyInMinutes = @Frequency

if @SubscriptionId is null
begin
*/
	if @Password is not null
		set @Password = rpl.fnEncryptDecryptString(@Password)

	insert into rpl.Subscription (ServerName, DatabaseName, FrequencyInMinutes, IsActive, Initialize, SubscriptionName, PriorityGroup, Login, Pass, DelayAlertInMinutes,DoubleReadRVRange,SubscriptionSequence)
	values (@Server, @Database, @Frequency, 0, 0, @Name, @PriorityGroup, @Login, @Password, 60,0,1)

	set @SubscriptionId = SCOPE_IDENTITY()
--end

--add checkpoint table from publisher

declare @sql varchar(max)
	, @checktable varchar(100) = 'DatesFromSubscription_'+cast(@SubscriptionId as varchar)

set @sql = 'if object_id(''rpl.'+@checktable+''') is not null drop table rpl.'+@checktable
exec spExec @sql, @Debug

set @sql = 'create table rpl.'+@checktable+' (date datetime constraint pk_rpl_DatesFromSubscription_'+cast(@SubscriptionId as varchar)+' primary key clustered)'
exec spExec @sql, @Debug

exec rpl.spSubscribeTable @SubscriptionId = @SubscriptionId
	, @SchemaName = 'rpl'
	, @TableName = @checktable
	, @debug = @debug
	, @PublisherSchemaName = 'rpl'
	, @PublisherTableName = 'Dates'

select @SubscriptionId SubscriptionId

go


if object_id('dbo.fnNullVal') is not null
	drop function dbo.fnNullVal
go
create function dbo.fnNullVal (@Type varchar(100) )
returns varchar(100)
as
begin 
	declare @NullVal varchar(100)

	select @NullVal = case  when @Type LIKE '%char%' then ''''''
							when @Type LIKE '%text%' then ''''''
							when @Type LIKE 'decimal%' then '0'
							when @Type LIKE 'numeric%' then '0'
							when @Type LIKE 'varbinary%' then '0x'
							when @Type in ('tinyint','smallint','float','money','int','bit','smallmoney','bigint') then '0'
							when @Type in ('uniqueidentifier') then '''00000000-0000-0000-0000-000000000000'''
							when @Type in ('datetime', 'date','smalldatetime') then '''01/01/1999'''
							else ''''''
							--TODO: image, datetime, varbinary
						end 
	return(@NullVal)
end		
go
--select * , dbo.fnNullVal(DataType) from rpl.SubscriptionColumn  select @@servername
go
if OBJECT_ID('dbo.VIV_TotalRows') is not null
drop view dbo.VIV_TotalRows
go
create view dbo.VIV_TotalRows
as
	SELECT sch.Name SchemaName, tbl.name TableName,   SUM(PART.rows) TotalRows
		,   SUM(PART.rows) TotalRowsFormatted
		--,   format(SUM(PART.rows),'n') TotalRowsFormatted
	FROM sys.tables TBL with (nolock)
	INNER JOIN sys.schemas sch on sch.schema_id = tbl.schema_id
	INNER JOIN sys.partitions PART with (nolock) ON TBL.object_id = PART.object_id
	INNER JOIN sys.indexes IDX with (nolock) ON PART.object_id = IDX.object_id	AND PART.index_id = IDX.index_id
	WHERE IDX.index_id < 2--get cix or head 
	group by sch.Name, tbl.name 
go


if object_id('rpl.spSubscriptionCompare') is not null
	drop proc rpl.spSubscriptionCompare
go
create proc [rpl].[spSubscriptionCompare] (@subscriptionid int=0, @debug bit = 0, @exec bit=0, @MaxRows int = 100000, @Days int=1, @TableName varchar(100)='', @RunAtPubliher bit=0, @AutoFix bit=0)
as
declare @sql varchar(max) 
	, @PublisherServer varchar(100)=''
	, @SubscriberServer varchar(100)=''
	, @PublisherDatabase varchar(100)
	, @SubscriberDatabase varchar(100)= db_name()
	, @schema varchar(100)
	, @table varchar(100)
	, @tableid int
	, @KeyColumns varchar(max)=''
	, @select varchar(max)=''
	, @from varchar(max)=''
	, @where varchar(max)=''
	, @join varchar(max)=''
	, @CompareColumns varchar(max)=''
	, @DateColumn varchar(100)
	, @TotalRowsFormatted varchar(100)
	, @TotalRows int 
	, @date varchar(10)
	, @FirstKeyColumn varchar(100)
	, @startdate varchar(25) = convert(varchar, dateadd(mi,-15,getdate()), 21)--ignore the recent 15 mintues
	, @DummyUpdateColumn varchar(100)

if @MaxRows is null
	set @MaxRows = 0

set @date = convert(varchar, dateadd(dd, -abs(@days), getdate()), 120) 	

if @AutoFix = 1 and (@RunAtPubliher = 0 or @debug =0 or @exec =1)
begin
	raiserror ('If @AutoFix = 1 then @RunAtPubliher must be 1', 16,0)
	return(0)
end

if @RunAtPubliher = 1
begin
	print '--changed execution mode to print commands, results should be executed directly at the publisher, to present doublehop authentication issues you should RDP into the publisher server, and make sure it has a linked server to the subscriber'
	set @exec = 0
	set @debug = 1
	set @SubscriberServer = '['+@@SERVERNAME+'].'
end
		
declare s_cursor cursor fast_forward for	 
	select subscriptionid, ServerName, DatabaseName from rpl.subscription where subscriptionid = @subscriptionid
open s_cursor
fetch next from s_cursor into @subscriptionid, @PublisherServer, @PublisherDatabase
while @@FETCH_STATUS=0
begin
	--append publisher servername if needed
	if @RunAtPubliher = 0 and @PublisherServer <> @@SERVERNAME and @PublisherServer not like '.%'
		set @PublisherServer = '['+@PublisherServer+'].'
	else
		set @PublisherServer = ''

	declare t_cursor cursor fast_forward for 
		select st.tableid, st.SchemaName, st.TableName , sc.DateColumn, r.TotalRowsFormatted, r.TotalRows
		from rpl.subscriptiontable st
		left join dbo.VIV_TotalRows r on r.SchemaName  = st.SchemaName and st.TableName = r.TableName
		outer apply (--gets the first column that matches one of these names to be used as date filter when totalrows is greater than threshold
			select top 1 sc.ColumnName as DateColumn 
			from rpl.SubscriptionColumn sc 
			join (values (1, 'DateLastChanged')
						,(2, 'DateCreated' )
						,(3, 'TimeRegistered')
						,(4, 'DateLastModified')
						,(5, 'Timestamp')
						,(6, 'DateLastChangedGMT')

				) v (priority, ColumnName) on v.ColumnName = sc.ColumnName
			where sc.TableId = st.TableId
			order by v.priority
		) sc
		where (st.SubscriptionId = @subscriptionid or @subscriptionid = 0)
		and st.TableName not like 'DatesFromSubscription%'
		and (@TableName = '' or st.TableName = @TableName)
		order by r.TotalRows, st.SchemaName, st.TableName

	open t_cursor
	fetch next from t_cursor into @tableid, @schema, @table, @DateColumn, @TotalRowsFormatted, @TotalRows
	while @@FETCH_STATUS=0
	begin
		select 	@KeyColumns =''
			, @select =''
			, @CompareColumns =''
			, @where =''
			, @join =''
		
		--if table does not have DateLastChanged then we only compare the tables that have less rows than threshold... otherwise query takes too long
		if @DateColumn is null and @TotalRows > @MaxRows
		begin
			set @sql ='select ''Table '+@schema+'.'+@table +' skipped because it has no DateLastChanged and number of rows exceeds threshold ('+@TotalRowsFormatted+') '''
			exec (@sql)
		end
		else
		begin
			select @KeyColumns = @KeyColumns  + case when @KeyColumns = '' then '' else ', ' end + 'coalesce(s.[' + ColumnName+'], p.[' + ColumnName+']) as [' + ColumnName+']'
				, @join = @join + case when @join = '' then '' else ' and ' end + 's.[' + ColumnName+']' + ' = p.[' + ColumnName+']'
				, @FirstKeyColumn = ColumnName
			from rpl.SubscriptionColumn 
			where TableId = @TableId
			and IsKey=1
			and ColumnName not in ('rv', 'sourcerv')
			order by ColId

			select @DummyUpdateColumn = ColumnName
			from rpl.SubscriptionColumn 
			where TableId = @TableId
			and ColumnName =  'sourcerv'
			
			--if table does not have sourcerv then get the first non key columns for dummy update
			if @DummyUpdateColumn is null
				select top 1 @DummyUpdateColumn = ColumnName
				from rpl.SubscriptionColumn 
				where TableId = @TableId
				and IsKey=0

			set @select = 'select ''['+@schema+'].['+@table+']'' as TableName
		 , case	when p.['+@FirstKeyColumn+'] is null then ''Missing in Publisher'' 
				when s.['+@FirstKeyColumn+'] is null then ''Missing in Subscriber'' '+char(13)

			set @from = '
from (select * from '+@SubscriberServer+'['+@SubscriberDatabase+'].['+@schema+'].['+@table+'] with (nolock) '+case when @DateColumn is null or @TotalRows <= @MaxRows then '' else ' where '+@DateColumn+' >= ''' + @Date + '''' end +' ) s 
full outer join (select * from '+@PublisherServer+'['+@PublisherDatabase+'].['+@schema+'].['+@table+'] with (nolock) '+case when @DateColumn is null or @TotalRows <= @MaxRows then '' else ' where '+@DateColumn+' >= ''' + @Date + '''' end +' ) p on ' + @join+char(13)
		
			--get approx date
			if @RunAtPubliher = 0
				set @from = @from + 'outer apply (select top 1 date as source_approx_date from rpl.DatesFromSubscription_'+cast(@subscriptionid as varchar) +' d where d.sourcerv < = p.rv order by date desc) d ' + char(13)
			else
				set @from = @from + 'outer apply (select top 1 date as source_approx_date from rpl.Dates d where d.rv < = p.rv order by date desc) d ' + char(13)

			set @where  = 'where (p.['+@FirstKeyColumn+'] is null
	or s.['+@FirstKeyColumn+'] is null ' +char(13)

			select @select = @select+'			when isnull(p.[' + ColumnName+'], '+dbo.fnNullVal (DataType)+') <> isnull(s.[' + ColumnName+'],'+dbo.fnNullVal (DataType)+') then ''Column does not match [' + ColumnName+']'' '+char(13) 
				, @CompareColumns = @CompareColumns+'	, p.[' + ColumnName+'] as [publisher_'+ ColumnName+'], s.[' + ColumnName+'] as [subscriber_'+ ColumnName+']'+char(13)
				, @where = @where + '	or isnull(p.[' + ColumnName+'], '+dbo.fnNullVal (DataType)+') <> isnull(s.[' + ColumnName+'],'+dbo.fnNullVal (DataType)+')'+char(13)
			from rpl.SubscriptionColumn 
			where TableId = @TableId
			and IsKey=0
			and ColumnName not in ('rv', 'sourcerv')
			and isIdentity=0
			and DataType not in ('image')
			order by ColId

			set @CompareColumns = @CompareColumns + '	, p.rv publisher_rv, s.sourcerv subscriber_sourcerv'+char(13)

			set @select = @select+'	end Difference '+char(13)
			set @select = @select + '	, ' + @KeyColumns+char(13)+@CompareColumns + '	, d.source_approx_date '
			set @where = @where + ') and (source_approx_date < dateadd(mi, -15, getdate()) or source_approx_date is null)'
		
			set @sql = '
if object_id(''tempdb..#'+@schema+'_'+@table+''')  is not null
	drop table #'+@schema+'_'+@table+'

'+@select +'
into #'+@schema+'_'+@table+ @from + @where+'

if @@rowcount > 0
begin
	select * from #'+@schema+'_'+@table+'
	'
	if @AutoFix = 1
	begin
			set @sql = @sql + '
	--auto fix, push inserts and updates
	update p set sourcerv= p.sourcerv
	from #'+@schema+'_'+@table+' s
	join '+@PublisherServer+'['+@PublisherDatabase+'].['+@schema+'].['+@table+'] p on ' + @join +'
	where [Difference] = ''Missing in Subscriber''
	or [Difference] like ''Column does not match%''

	--push deletes
	update p set dt= p.dt
	from #'+@schema+'_'+@table+' s
	join '+@PublisherServer+'['+@PublisherDatabase+'].rpl.del_'+@schema+'_'+@table+' p on ' + @join +'
	where [Difference] = ''Missing in Publisher'' '

	end
	
	set @sql = @sql + '
end
else 
	select ''Table '+@schema+'.'+@table+' matched '+@TotalRowsFormatted+' rows!'''
			
			
			exec spExec @sql, @debug, @exec
		end
		fetch next from t_cursor into @tableid, @schema, @table, @DateColumn, @TotalRowsFormatted, @TotalRows
	end
	close t_cursor
	deallocate t_cursor
	fetch next from s_cursor into @subscriptionid, @PublisherServer, @PublisherDatabase
end
close s_cursor
deallocate s_cursor

go

go

go
IF OBJECT_ID('rpl.spR') IS NOT null
DROP PROC rpl.spR
go
CREATE proc rpl.spR
as

if object_id('tempdb..#t') is not null
	drop table #t
  
select * into #t
from rpl.Subscription s
outer apply (
	select top 1 ImportLogId,RvFrom,RvTo,StartDate,EndDate,Success,TotalRows,RvTotalRows,Threads,UseStage,message
	from rpl.ImportLog l 
	where l.SubscriptionId = s.SubscriptionId
	order by ImportLogId desc
	) l
where s.IsActive=1
order by s.SubscriptionSequence, s.SubscriptionId

select * from #t

declare @SubscriptionId int, @enddate datetime, @usestage bit
declare t_cursor cursor fast_forward for
	select SubscriptionId, enddate, usestage from #t
open t_cursor
fetch next from t_cursor into @SubscriptionId, @enddate, @usestage
while @@FETCH_STATUS=0
begin 
	SELECT TOP 10 *,DATEDIFF(ss, StartDate, EndDate) seconds
	FROM rpl.ImportLog l 
	where SubscriptionId = @SubscriptionId
	ORDER BY ImportLogId DESC

	if @enddate is null --still in progress
		select * , TotalRows TotalRowsFormatted --, format(TotalRows,'n') TotalRowsFormatted
		from [rpl].[fnGetStgRowCount] (@usestage, @SubscriptionId) ORDER BY SchemaName, TableName
	else
		SELECT * , TotalRows TotalRowsFormatted --, format(TotalRows,'n') TotalRowsFormatted
		FROM rpl.ImportLogDetail 
		WHERE ImportLogId = (SELECT MAX(ImportLogId) FROM rpl.ImportLog where SubscriptionId = @SubscriptionId)
		ORDER BY SchemaName, TableName
	
	fetch next from t_cursor into @SubscriptionId, @enddate, @usestage
end
close t_cursor 
deallocate t_cursor

go

IF OBJECT_ID('rpl.spFakeInitialization') IS NOT null
DROP PROC rpl.spFakeInitialization
go
create proc rpl.spFakeInitialization (@SubscriptionId int, @RvTo varchar(20), @Activate bit=0)
as

declare  @rvto_bin varbinary(8) = convert(varbinary(8), @rvto, 1)

insert into rpl.ImportLog(SubscriptionId,RvFrom,RvTo,StartDate,EndDate,Success,TotalRows,RvTotalRows,Threads,UseStage,message)
values (@SubscriptionId, 0x, @rvto_bin, getdate(), getdate(), 1, -1, -1, 1, 0, 'Fake Initialization')

update rpl.Subscription set IsActive= @Activate where SubscriptionId=@SubscriptionId

go

if OBJECT_ID('[rpl].[vwImportLogDetail]') is not null
	drop view  [rpl].[vwImportLogDetail]
go
CREATE view [rpl].[vwImportLogDetail]
as
select s.ServerName, s.DatabaseName, s.SubscriptionId, l.StartDate, l.Success, l.RvFrom, l.RvTo, d.* 
from rpl.ImportLogDetail d
join rpl.ImportLog l on d.ImportLogId = l.ImportLogId
join rpl.Subscription s on s.SubscriptionId  = l.SubscriptionId
GO
if object_id('rpl.vwSubscriptionTable')is not null
drop view rpl.vwSubscriptionTable
go
create view rpl.vwSubscriptionTable
as
select s.*, t.SchemaName
, t.TableName
, t.PublisherSchemaName
, t.PublisherTableName
, t.IsActive TableIsActive
, t.PkName
, t.KeyCount
, t.has_identity
, t.Initialize TableInitialize
, t.InitialRowCount
, t.IsCustom
, t.GetProcName
from rpl.subscription s
join rpl.SubscriptionTable t on s.subscriptionid = t.subscriptionid
go

if object_id('rpl.vwSubscriptionColumn')is not null
drop view rpl.vwSubscriptionColumn
go
create view rpl.vwSubscriptionColumn
as
select s.*
, t.SchemaName
, t.TableName
, t.PublisherSchemaName
, t.PublisherTableName
, t.IsActive TableIsActive
, t.PkName
, t.KeyCount
, t.has_identity
, t.Initialize TableInitialize
, t.InitialRowCount
, t.IsCustom
, t.GetProcName
, c.ColumnId
, c.TableId
, c.ColumnName
, c.DataType
, c.ColumnLength
, c.IsKey
, c.ColId
, c.IsIdentity
from rpl.subscription s
join rpl.SubscriptionTable t on s.subscriptionid = t.subscriptionid
join rpl.subscriptionColumn c on c.tableid=t.tableid
go

if OBJECT_ID('rpl.spReSubscribeTables') is not null
	drop proc rpl.spReSubscribeTables
go
create proc rpl.spReSubscribeTables (@SubscriptionId int=0, @debug bit =0, @exec bit=1)
as
declare @SchemaName varchar(100), @TableName varchar(100),@PublisherSchemaName varchar(100), @PublisherTableName varchar(100)

declare t_cursor cursor fast_forward for
	select SubscriptionId, SchemaName, TableName, PublisherSchemaName, PublisherTableName
	from rpl.SubscriptionTable
	where @SubscriptionId = 0 or SubscriptionId = @SubscriptionId
open t_cursor
fetch next from t_cursor into @SubscriptionId, @SchemaName, @TableName,@PublisherSchemaName, @PublisherTableName
while @@FETCH_STATUS=0
begin
	exec rpl.spSubscribeTable @SubscriptionId, @SchemaName, @TableName,0,1,@PublisherSchemaName, @PublisherTableName
	fetch next from t_cursor into @SubscriptionId, @SchemaName, @TableName,@PublisherSchemaName, @PublisherTableName
end
close t_cursor
deallocate t_cursor

go

if object_id('rpl.spRVCheck') is not null
	drop proc rpl.spRVCheck
go
create proc rpl.spRVCheck (
	@rv varbinary(8) = 0x0000000054576178,
	@subscriptionid int = 5,
	@tablename varchar(100)= 'shopservice.tbl_products'
	)
as

declare @sql varchar(max)

select *
from rpl.ImportLog 
where @rv between rvfrom and rvto
and SubscriptionId=@subscriptionid


select @sql = 'exec rpl.spGet_'+replace(@tablename,'.','_') + ' ''' + convert(varchar, rvfrom,1) + ''',''' + convert(varchar, rvto , 1)+''''
from rpl.ImportLog 
where @rv between rvfrom and rvto
and SubscriptionId=@subscriptionid

select @sql

go

if object_id('rpl.spSub') is not null
	drop proc rpl.spSub
go
create proc rpl.spSub
as
select * from rpl.Subscription order by SubscriptionSequence, SubscriptionId
select * from rpl.subscriptionTable order by SubscriptionId, TableName
select * from rpl.subscriptionRoutine order by SubscriptionId, RoutineSequence

go


go
if not exists (select * from sysobjects where name='rpl')
	create synonym rpl for rpl.spr
go
if not exists (select * from sysobjects where name='sub')
	create synonym sub for rpl.spSub

go

create or alter function [dbo].[ConvertTimeToHHMMSS]
(
    @time decimal(28,3), 
    @unit varchar(20)
)
returns varchar(20)
as
begin

    declare @seconds decimal(18,3), @minutes int, @hours int;

    if(@unit = 'hour' or @unit = 'hh' )
        set @seconds = @time * 60 * 60;
    else if(@unit = 'minute' or @unit = 'mi' or @unit = 'n')
        set @seconds = @time * 60;
    else if(@unit = 'second' or @unit = 'ss' or @unit = 's')
        set @seconds = @time;
    else set @seconds = 0; -- unknown time units

    set @hours = convert(int, @seconds /60 / 60);
    set @minutes = convert(int, (@seconds / 60) - (@hours * 60 ));
    set @seconds = @seconds % 60;

    return 
        convert(varchar(9), convert(int, @hours)) + ':' +
        right('00' + convert(varchar(2), convert(int, @minutes)), 2) + ':' +
        right('00' + convert(varchar(6), @seconds), 6)

end
