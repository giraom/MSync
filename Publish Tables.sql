--Run this to build the commands to publish the tables you wish
select s.name, o.name
	,'exec rpl.spPublishTable '''+s.name+''','''+o.name+''''
	--select *
	,'drop table '+s.name+'.'+o.name
 from sys.objects o
 join sys.schemas s on o.schema_id = s.schema_id
 where o.type='u'
 and s.name not in ('rpl')
 and o.name not in ('import_stats','sysdiagrams')
 and not exists (select * from rpl.PublicationTable p where p.schemaname=s.name and p.tablename=o.name)
 order by 1,2


select * from rpl.PublicationTable
select * from rpl.PublicationColumn

