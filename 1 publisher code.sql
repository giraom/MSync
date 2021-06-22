
if not exists(select * from sys.schemas where name = 'rpl')
	exec('create schema rpl authorization dbo')
go
if exists (select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where CONSTRAINT_NAME='FK_rpl_PublicationColumn_PublicationTable')
	alter table rpl.PublicationColumn drop constraint FK_rpl_PublicationColumn_PublicationTable 
GO
if OBJECT_ID('rpl.Dates') is not null
	DROP TABLE rpl.Dates
GO
create table rpl.Dates (date datetime constraint pk_rpl_dates primary key clustered, sourcerv varbinary(8), rv rowversion)
go
insert into rpl.Dates (date) values (getdate())
go
if OBJECT_ID('rpl.PublicationTable') is not null
	DROP TABLE rpl.PublicationTable
	GO
	CREATE TABLE rpl.PublicationTable(
		TableId int identity NOT NULL,
		SchemaName varchar(50) NULL,
		TableName varchar(128) NULL,
		PkName varchar(128) NULL,
		KeyCount smallint NULL,
		has_identity bit null,
		IsCustom bit default(0),
		rv rowversion
		CONSTRAINT PK_PublicationTable PRIMARY KEY CLUSTERED (TableId ASC)
	) ON [PRIMARY]
	GO
	create unique index UDX_PublicationTable on rpl.PublicationTable  (SchemaName, TableName) 
	GO

if OBJECT_ID('rpl.PublicationColumn') is not null
	DROP TABLE rpl.PublicationColumn
	GO
	CREATE TABLE rpl.PublicationColumn(
		ColumnId int identity NOT NULL,
		TableId int NOT NULL,
		ColumnName varchar(128) NOT NULL,
		DataType varchar(128) NULL,
		ColumnLength int NULL,
		IsKey bit NULL,
		ColId int NULL,
		IsIdentity int null,
		ApplyCompression bit null,
		TrackDeletes bit null,
		rv rowversion
		CONSTRAINT PK_PublicationColumn PRIMARY KEY CLUSTERED (ColumnId ASC)
	) ON [PRIMARY]

	GO
	create unique index UDX_PublicationColumn on rpl.PublicationColumn (TableId, ColumnName)
	alter table rpl.PublicationColumn add constraint FK_rpl_PublicationColumn_PublicationTable foreign key (TableId) references rpl.PublicationTable(TableId)
	GO


	/*Note:
	account.TBL_ProductReviews has some bad index names:
	PK_TBL_ProductRatings
	UK_TBL_ProductRatings
	*/
 go

if OBJECT_ID('rpl.spGetCurrentTimestamp') is not null
	drop proc rpl.spGetCurrentTimestamp 
go
create proc rpl.spGetCurrentTimestamp (@rv varchar(20) output)
as
	declare @rv_bin varbinary(8)
		, @Minutes int
	
	--here we get the rv about 1 minute ago, in a form to delay the subscribers.
	--this is an attempt to prevent gaps, which we suspect are caused by the fact that the publisher may already have generated some RV values, but they were not yet available for reads when the subscription ran.

	select top 1 @rv_bin = rv, @Minutes = datediff(mi, date, getdate()) 
	from rpl.Dates
	where date < dateadd(ss, -15, getdate())--force a delay to make sure RV was released
	order by date desc
	
	if @Minutes > 60 --date is too old, probably the job that inserts stopped running
		 set @rv_bin = @@dbts

	set @rv = convert(varchar, @rv_bin, 1)
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
			print @sql
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

if OBJECT_ID('rpl.spUnPublishTable') is not null
	drop proc rpl.spUnPublishTable
go
create proc rpl.spUnPublishTable (@SchemaName varchar(100), @TableName varchar(100), @debug bit=0, @removeRv bit=0)
as	
declare @sql varchar(max)

if not exists (select * from rpl.PublicationTable where SchemaName= @SchemaName and TableName = @TableName )
begin
	raiserror('Table is not Published!', 16,0)
	return(0)
end

begin try
	begin transaction
	--remove trigger 
	set @sql = 'if exists (select * from sysobjects where name =''trg_rpl_del_'+@TableName+''' and type = ''TR'') drop trigger ['+@SchemaName+'].trg_rpl_del_'+@TableName
	exec spExec @sql, @debug

	--remove get proc 
	set @sql = 'if object_id(''rpl.spGet_'+@SchemaName+'_'+@TableName+''') is not null drop proc rpl.spGet_'+@SchemaName+'_'+@TableName
	exec spExec @sql, @debug

	--remove del table
	set @sql = 'if object_id(''rpl.del_'+@SchemaName+'_'+@TableName+''') is not null drop table rpl.del_'+@SchemaName+'_'+@TableName
	exec spExec @sql, @debug

	if @removeRv = 1
	begin
		--remove rv index
		set @sql = 'if exists (select * from sysindexes where name =''IX_'+@SchemaName+'_'+@TableName+'_rv'') drop index ['+@SchemaName+'].['+@TableName+'].IX_'+@SchemaName+'_'+@TableName+'_rv'
		exec spExec @sql, @debug
 
		--remove rv column
		set @sql = 'if exists (select * from sys.tables t inner join sys.schemas s on t.schema_id = s.schema_id	inner join sys.columns c on c.object_id = t.object_id where c.name =''rv'' and s.name = '''+@SchemaName+''' and t.name = '''+@TableName+''') alter table ['+@SchemaName+'].['+@TableName+'] drop column rv'
		exec spExec @sql, @debug
	end
	delete rc from  rpl.PublicationTable rt
	join rpl.PublicationColumn rc on rc.tableid = rt.TableId
	where rt.SchemaName = @SchemaName and rt.TableName = @TableName

	delete rt from  rpl.PublicationTable rt where SchemaName = @SchemaName and TableName = @TableName

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
--exec rpl.spUnPublishTable 'shopservice','TBL_Products'

go

if OBJECT_ID('rpl.spPublishTable') is not null
	drop proc rpl.spPublishTable
go

create proc [rpl].[spPublishTable] (@SchemaName varchar(100), @TableName varchar(100), @debug bit=0, @exec bit=1, @ProcsOnly bit=0, @RebuildDeleteTable bit=1
	, @KeyName varchar(100) ='', @IgnoreKeyColumn varchar(100) =''
	, @SkipIndex bit=0)
as	
declare @sql varchar(max)
	, @TableId int=0, @PkName varchar(100)=''
	, @KeyColumns varchar(max)=''
	, @KeyColumnDefinitions varchar(max)=''
	, @TrackDeleteColumns varchar(max)=''
	, @TrackDeleteColumnDefinitions varchar(max)=''
	, @Columns varchar(max)=''
	, @ColumnsWithCompression varchar(max)=''
	, @ColumnDefinitions varchar(max)=''
	, @DeleteColumns varchar(max)=''
	, @LogTableName varchar(100)=''
	, @TriggerName varchar(100)=''
	, @ProcName varchar(100)=''
	, @IsCustom bit
	, @IsNew bit=1
	, @RvColumnName varchar(100)=''
	, @RvColumnType varchar(100)=''

if not exists (select * from sys.objects t
			inner join sys.schemas s on t.schema_id = s.schema_id
			where s.name = @SchemaName
			and t.name  = @TableName )
begin
	raiserror('Invalid table Name!', 16,0)
	return(0)
end

--if exists (select * from rpl.PublicationTable where SchemaName= @SchemaName and TableName = @TableName)
--begin
--	raiserror('Table is already Published! You dont need to publish again unless you: a) Added Columns, b) Removed Columns, c) Changed the PK. In this case please execute rpl.spUnpublishTable first. Beware all subscriptions will be broken in the meantime!', 16,0)
--	return(0)
--end

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
			from sys.objects t
			inner join sys.schemas s on t.schema_id = s.schema_id
			cross apply (
				 select top 1 *
				 from sys.indexes i 
				 where i.object_id = t.object_id
				 and is_unique = 1
				 and (isnull(@KeyName,'') = '' or i.name = @keyname) 
				 order by i.is_primary_key desc, i.is_unique_constraint desc
				) i
			inner join sys.index_columns ic on ic.object_id = t.object_id AND i.index_id = ic.index_id
			inner join sys.columns c on c.object_id = t.object_id AND ic.column_id = c.column_id
			inner join sys.types p on p.system_type_id = c.system_type_id
			where ic.key_ordinal > 0
			and s.name = @SchemaName
			and t.name  = @TableName 
			group by s.name, t.name, i.name
		)
	merge rpl.PublicationTable p using t on p.SchemaName = t.SchemaName and p.TableName = t.TableName
	when not matched by target then insert (SchemaName, TableName, PkName, KeyCount, has_identity)
		values (t.SchemaName, t.TableName, t.PkName, t.KeyCount, t.has_identity)
	when matched then update set 
		PkName = t.PKName
		, KeyCount = t.KeyCount
		, has_identity = t.has_identity
		, @IsNew = 0
	;

	select @TableId = TableId
		, @PkName = PkName 
		, @IsCustom	 = IsCustom
	from  rpl.PublicationTable rt 
	where SchemaName = @SchemaName and TableName = @TableName
	
	if @PkName is null or @PkName = ''
	begin
		set @sql = 'Table '+@SchemaName+'.'+@TableName+' does not have a primary key!'
		raiserror (@sql,16,1)
		return(0)
	end
	else if @IsCustom = 1
	begin
		set @sql = 'Table '+@SchemaName+'.'+@TableName+' is custom so publish must be done manually!'
		raiserror (@sql,16,1)
		return(0)
	end

	select @RvColumnName = c.name 
		from sys.objects t
		join sys.schemas s on t.schema_id = s.schema_id
		join sys.columns c on c.[object_id] = t.[object_id] 
		inner join sys.types ty on ty.user_type_id = c.user_type_id and ty.name in ('timestamp','rowversion')
		where s.Name = @SchemaName 
		and t.Name = @TableName

	select @RvColumnType = ty.name 
		from sys.objects t
		join sys.schemas s on t.schema_id = s.schema_id
		join sys.columns c on c.[object_id] = t.[object_id] and c.name = 'rv'
		inner join sys.types ty on ty.user_type_id = c.user_type_id 
		where s.Name = @SchemaName 
		and t.Name = @TableName

	if @RvColumnType not in ('', 'timestamp','rowversion')
	begin
		set @sql = 'Table '+@SchemaName+'.'+@TableName+' already has column [rv] of type ['+@RvColumnType+'], please rename this column, the column name rv must be reverved for mSync!'
		raiserror (@sql,16,1)
		return(0)
	end
	else if @RvColumnName = '' and @RvColumnType = ''
	begin
		set @sql = 'alter table ['+@SchemaName+'].['+@TableName+'] add rv rowversion ' 
		exec spExec @sql, @debug, @exec
	end
	else if @RvColumnName <> '' and @RvColumnType = ''
	begin--there is already a column of type rowversion and name other than RV, so we create an alias to the existing column with name RV
		set @sql = 'alter table ['+@SchemaName+'].['+@TableName+'] add rv as ['+@TimestampColumn+'] ' 
		exec spExec @sql, @debug, @exec
	end

--get column properties
	/*
	delete pc from rpl.PublicationColumn pc
	join rpl.PublicationTable pt on pc.Tableid = pt.TableId
	where pt.SchemaName = @SchemaName and pt.TableName = @TableName
	*/
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
			from sys.objects t
			inner join sys.schemas s on t.schema_id = s.schema_id
			inner join sys.columns c on c.object_id = t.object_id
			inner join sys.types ty on ty.user_type_id = c.user_type_id
			inner join rpl.PublicationTable rt on rt.SchemaName = s.name and rt.TableName = t.name 
			where 1=1 --c.is_computed = 0		
			and s.name = @SchemaName
			and t.name  = @TableName 
			and c.name not in ('sourcerv','rv')
			)
	merge rpl.PublicationColumn p using c on p.TableId = c.TableId and c.ColId = p.ColId
	when not matched by target then insert (TableId, ColumnName, DataType, ColumnLength, IsKey, ColId, IsIdentity)
		values (c.TableId, c.ColumnName, c.DataType, c.ColumnLength, c.IsKey, c.ColId, c.IsIdentity) 
	when matched then update set
		 ColumnName = c.ColumnName
		 , DataType = c.DataType
		 , ColumnLength = c.ColumnLength
		 , IsKey = c.IsKey
		 , IsIdentity = c.IsIdentity
		;

--build lists
	select @KeyColumns = @KeyColumns  + case when @KeyColumns = '' then '' else ', ' end + '[' + ColumnName+']'
		, @KeyColumnDefinitions = @KeyColumnDefinitions + case when @KeyColumnDefinitions = '' then '' else ', ' end + '[' + ColumnName+']' + ' ' + DataType
	from rpl.PublicationColumn 
	where TableId = @TableId
	and IsKey=1
	order by ColId

	select @TrackDeleteColumns = @TrackDeleteColumns  + ', [' + ColumnName+']'
		, @TrackDeleteColumnDefinitions = @TrackDeleteColumnDefinitions + ', [' + ColumnName+']' + ' ' + DataType
	from rpl.PublicationColumn 
	where TableId = @TableId
	and IsKey=0 and TrackDeletes=1
	order by ColId
		
	select @Columns  = @Columns + case when @Columns = '' then '' else ', ' end +'[' + ColumnName+']'
		, @ColumnsWithCompression = @ColumnsWithCompression + case when @ColumnsWithCompression = '' then '' else ', ' end + case when ApplyCompression = 1 then 'COMPRESS(['+ColumnName+']) as ['+ColumnName+']' else '['+ColumnName+']' end
		, @ColumnDefinitions = @ColumnDefinitions + case when @ColumnDefinitions = '' then '' else ', ' end + '[' + ColumnName+']' + ' ' + DataType
		, @DeleteColumns = @DeleteColumns + case when @DeleteColumns = '' then '' else ', ' end + case when IsKey = 1 or ColumnName = 'rv' then ' ['+ColumnName+']' else 'NULL as ['+ColumnName+']' end 
	from rpl.PublicationColumn 
	where TableId = @TableId
	order by ColId

	--set @Columns = @columns + replace(@Columns, ', [rv]', ', [rv] as sourcerv' )
	--set @DeleteColumns = replace(@DeleteColumns, ', [rv]', ', [rv] as sourcerv' )

	select @LogTableName = 'rpl.del_'+@SchemaName+'_'+@TableName
		, @TriggerName = 'trg_rpl_del_'+@TableName
		, @ProcName = 'rpl.spGet_'+@SchemaName+'_'+@TableName

--Add RV Index
	set @sql = 'if not exists (select * from sysindexes where name =''IX_'+@SchemaName+'_'+@TableName+'_rv'')
		create index IX_'+@SchemaName+'_'+@TableName+'_rv on ['+@SchemaName+'].['+@TableName + '] (rv desc) include ('+@KeyColumns+')  with (fillfactor=90, online='
		+ case when @@VERSION like '%Enterprise%' then 'on)' else 'off)' end
	if @SkipIndex = 0
		exec spExec @sql, @debug, @exec

--drop and create del table
	if @RebuildDeleteTable=1 or @IsNew = 1
	begin
		set @sql='if object_id('''+@LogTableName+''') is not null drop table '+@LogTableName 
		exec spExec @sql, @debug, @exec
	
		set @sql = 'create table '+@LogTableName + '(
			rv rowversion
			, dt datetime
			, sourcerv varbinary(8)
			, '+@KeyColumnDefinitions + @TrackDeleteColumnDefinitions + '
		)'
		exec spExec @sql, @debug, @exec

		set @sql = 'create unique clustered index cix_'+replace(@LogTableName,'.','_') +' on '+@LogTableName +' (rv desc) ' 
		exec spExec @sql, @debug, @exec
	end

--drop and create del trigger	
	set @sql = 'if exists (select * from sysobjects where xtype=''TR'' and name='''+@TriggerName+''' and schema_name(uid) = '''+@SchemaName+''' ) drop trigger ['+@SchemaName+'].'+@TriggerName 
	exec spExec @sql, @debug, @exec

	set @sql = 'create trigger '+@TriggerName+' on ['+@SchemaName+'].['+@TableName + '] for delete 
	as
	insert into '+@LogTableName+' (dt, sourcerv, '+@KeyColumns + @TrackDeleteColumns +')
	select getdate(), rv, '+@KeyColumns + @TrackDeleteColumns+'
	from deleted 
	'	
	exec spExec @sql, @debug, @exec

--drop and create get proc
	set @sql  = 'if object_id ('''+@ProcName+''') is not null 
		drop proc '+@ProcName
	exec spExec @sql, @debug, @exec

	set @sql = 'create proc '+@ProcName+' (@rvfrom varchar(20)=''0x'', @rvto varchar(20)=''0x9999999999999999'')
as 
set deadlock_priority low

if @rvfrom  is null or @rvfrom = ''''
	set @rvfrom = ''0x''
if @rvto  is null or @rvto = ''''
	set @rvto = ''0x9999999999999999''

declare @rvfrom_bin varbinary(8) = convert(varbinary(8), @rvfrom, 1), @rvto_bin varbinary(8) = convert(varbinary(8), @rvto, 1)

if @rvfrom_bin = 0x
BEGIN
	select ''I'' as RplOperation, '+@Columns+', rv as sourcerv
	from ['+@SchemaName+'].['+@TableName+'] /*with (nolock)*/ --here we use dirty reads to prevent long locks during initialization, but we risk skiping rows due ghost reads, or raising PK violations due to row movement
	where [rv] <= @rvto_bin
END
ELSE
BEGIN
	;with a as (
		select ''U'' as RplOperation, '+@ColumnsWithCompression+', rv as sourcerv
		from ['+@SchemaName+'].['+@TableName+'] with (serializable, forceseek)--here we need to read committed, no nolock, we force seek because sql sometimes choses CI scan, and we dont want that
		where rv > @rvfrom_bin and rv <= @rvto_bin
		union all
		select ''D'' as RplOperation, '+@DeleteColumns+', rv as sourcerv
		from '+@LogTableName+' with (serializable, forceseek) 
		where rv > @rvfrom_bin and rv <= @rvto_bin
		and @rvfrom_bin > 0x
	),
	b as (
		select *,
		 row_number() over (partition by '+@KeyColumns+' order by sourcerv desc) rpl_rowid
		from a)
	select RplOperation, '+@Columns+', sourcerv 
	from b where rpl_rowid=1 --if a row was inserted/updated/deleted multiple times within a replication window, this clause will transfer only the last RplOperation.
END
'
	exec spExec @sql, @debug, @exec

	print 'Table '+@SchemaName+'.'+@TableName+ ' published successfully!'

	if @Debug=1
		--retrieve table and columns for review purposes	
		select * from rpl.PublicationTable rt
		join rpl.PublicationColumn rc on rt.TableId = rc.TableId
		where SchemaName = @SchemaName
			and TableName  = @TableName 
		order by rt.SchemaName, rt.TableName, rc.ColId
	
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

exec rpl.spPublishTable 'rpl', 'Dates'
go

if object_id('rpl.spPub') is not null
	drop proc rpl.spPub
go
create proc rpl.spPub
as
select * from rpl.PublicationTable order by SchemaName, TableName
go

if object_id('rpl.fnRvToDate') is not  null
	drop function rpl.fnRvToDate
go
create function rpl.fnRvToDate (@rv varbinary(8))
returns datetime
as
begin
	return(select max(Date) from rpl.dates where rv <= @rv) 
end 
go

if object_id('rpl.fnDateToRv') is not  null
	drop function rpl.fnDateToRv
go
create function rpl.fnDateToRv (@dt datetime)
returns varbinary(8)
as
begin
	return(select max(rv) from rpl.dates where date <= @dt) 
end 
go

if object_id('rpl.spCreateCheckpoint') is not  null
	drop proc rpl.spCreateCheckpoint
go
create proc rpl.spCreateCheckpoint
as
begin
	insert into rpl.Dates (date) values (getdate())
end
go


/*
--CREATE CHECKPOINT JOB
begin try
	declare @db varchar(100) = db_name()
		, @name varchar(255) 
		, @jobId BINARY(16)
		, @guid uniqueidentifier = newid()

	set @name = 'MSyncReplication Checkpoint '+@db

	if not exists (select * from msdb..sysjobs where name = @name)
	begin
		begin transaction

		EXEC msdb.dbo.sp_add_job @job_name= @name, 
				@enabled=1, 
				@notify_level_eventlog=0, 
				@notify_level_email=0, 
				@notify_level_netsend=0, 
				@notify_level_page=0, 
				@delete_level=0, 
				@description=N'Create entry in table rpl.Dates every minute', 
				@category_name=N'', 
				@owner_login_name=N'sa', 
				@job_id = @jobId OUTPUT

		EXEC  msdb.dbo.sp_add_jobstep @job_id=@jobId, 
				@step_name=N'Create Checkpoint', 
				@step_id=1, 
				@cmdexec_success_code=0, 
				@on_success_action=1, 
				@on_success_step_id=0, 
				@on_fail_action=2, 
				@on_fail_step_id=0, 
				@retry_attempts=0, 
				@retry_interval=0, 
				@os_run_priority=0, @subsystem=N'TSQL', 
				@command=N'insert into rpl.Dates (date) values (getdate())', 
				@database_name= @db, 
				@flags=0

		EXEC msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1

		EXEC msdb.dbo.sp_add_jobschedule @job_id=@jobId, 
				@name=N'Every minute', 
				@enabled=1, 
				@freq_type=4, 
				@freq_interval=1, 
				@freq_subday_type=4, 
				@freq_subday_interval=1, 
				@freq_relative_interval=0, 
				@freq_recurrence_factor=0, 
				@active_start_date=20170404, 
				@active_end_date=99991231, 
				@active_start_time=0, 
				@active_end_time=235959, 
				@schedule_uid= @guid

		EXEC msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = @@SERVERNAME

		COMMIT TRANSACTION
	end
end try
begin catch
	rollback transaction
	set @name = error_message()
	raiserror (@name, 16,1)
end catch

go
*/
