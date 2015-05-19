COMMIT;
set search_path = etl_temp,report,star,star_secure,hist,staging,lookups,logging,ztrack; 
set session characteristics as transaction isolation level read committed;


insert /*+direct*/ into etl_temp.tmp_gena_ocpr (char1,char2,char3,int1,int2,int3,date1,int4)
SELECT 'DAU',upper(a.ip_country),upper(a.latest_locale), a.game_id, a.sn_id, a.client_id, a.stat_date::date,a.user_uid
FROM star_secure.a_user_day a
WHERE stat_date between current_date-35 and current_date-2
--and game_id = 5002852
GROUP BY 1,2,3,4,5,6,7,8
;

-- ADDING CASES
insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char1,int4,date1,int1,int5)
Select 
'Tickets',
case when length(a.social_network_uid__c) - length(translate(a.social_network_uid__c,'1234567890','')) = length(a.social_network_uid__c) then
	case when length(a.social_network_uid__c) between 2 and 18 then a.social_network_uid__c::INT 
	else -999 end 
else -999 end as user_uid,
a.CREATEDDATE::date,
a.game_name as 'game_id',
Count(distinct a.CASENUMBER)
 from (Select 
a.CASENUMBER,
a.CREATEDDATE::date,
b.game_id as game_name,
a.social_network_uid__c
 from staging.s_sfdc_case a 
left join
report.a_team_datastore b ON
a.game__c = b.varchar1 AND b.counter ='game_name'
where  a.DISPOSITION__C <> 'Duplicate Incident'
AND a.CREATEDDATE::DATE between current_date-35 and current_date-2
AND a.LAST_QUEUE_OWNER__C NOT IN ('(SAD)', 'Bounces', 'CSIT', 'DxDiag', 'Email Demo', 'Awaiting Player Reply', 'Fake Queue', 'Gerardo Enrique Romero Rivera', 'Kim Florence', 'Legacy No Answer', 'No Game Queue', 'ZZ - CS Product Only', 'Game Closure'))a

group by 1,2,3,4
;

insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char1,int4,date1,int5,char2,char3,int1,int2,int3)
Select 
'Tickets Final',
a.int4 as 'user_uid',
a.date1::date,
Sum(b.int5) as 'Tickets',
a.char2 as 'ip_country',
a.char3 as 'latest_locale',
a.int1 as 'game_id',
a.int2 as 'sn_id',
a.int3 as 'client_id'
from etl_temp.tmp_gena_ocpr a
left join etl_temp.tmp_gena_ocpr b
on 
a.int4=b.int4 and a.int1=b.int1 and a.date1::date=b.date1::date and b.char1='Tickets'
where a.char1 = 'DAU'
Group by 1,2,3,5,6,7,8,9
;

insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char1,int4,date1,int5,char2,char3,int1,int2,int3,int6)
Select 
'Installs',
a.int4 as 'UID',
a.date1,
Sum(a.int5) as 'Tickets',
a.char2 as 'ip_country',
a.char3 as 'latest_locale',
a.int1 as 'game_id',
a.int2 as 'sn_id',
a.int3 as 'client_id',
Count(distinct b.user_uid) as 'Installs'
from etl_temp.tmp_gena_ocpr a
left join ztrack.s_zt_install b
on b.user_uid=a.int4 
and b.game_id=a.int1
and b.sn_id=a.int2
and b.client_id=a.int3
and b.install_date=a.date1
where a.char1 = 'Tickets Final'
group by 1,2,3,5,6,7,8,9
;

insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char1,int4,date1,int5,char2,char3,int1,int2,int3,int6,int7)
Select 
'Revenue',
a.int4 as 'UID',
a.date1,
Sum(a.int5) as 'Tickets',
a.char2 as 'ip_country',
a.char3 as 'latest_locale',
a.int1 as 'game_id',
a.int2 as 'sn_id',
a.int3 as 'client_id',
a.int6 as 'Installs',
Sum(b.amount) as 'Revenue'
from etl_temp.tmp_gena_ocpr a
left join report.v_payment b
on b.user_uid=a.int4 
and b.game_id=a.int1
and b.sn_id=a.int2
and b.client_id=a.int3
and b.date_trans::date=a.date1
where a.char1 = 'Installs'
group by 1,2,3,5,6,7,8,9,10
;

insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char15,date1,int1,int2,int3,char2,char3,int6,int7,int5,int4)
Select 
'Prefinal',
a.date1 as 'date',
a.int1 as 'game_id',
a.int2 as 'sn_id',
a.int3 as 'client_id',
a.char2 as 'ip country',
a.char3 as 'game language',
Sum(a.int6) as 'Installs',
Sum(a.int7) as 'Revenue',
Sum(a.int5) as 'Tickets',
Count(a.int4) as 'DAU'
from etl_temp.tmp_gena_ocpr a
where a.char1 = 'Revenue'
group by 1,2,3,4,5,6,7
;

insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char15,date1,int1,int2,int3,char1,char2,char3,char4,char5,char6,char7,int6,int7,int5,int4)
Select 
'Final',
a.date1 as 'date',
a.int1 as 'game_id',
a.int2 as 'sn_id',
a.int3 as 'client_id',
f.country_name,
f.country_region,
f.country_sub_region,
f.country_territory,
f.primary_language,
d.primary_language as 'Game Language',
b.mobile_device_model,
a.int6 as 'Installs',
a.int7 as 'Revenue',
a.int5 as 'Tickets',
a.int4 as 'DAU'
from etl_temp.tmp_gena_ocpr a
left join 
lookups.l_country f
on a.char2 = f.country_code
left join 
lookups.l_country d
on a.char3 = d.country_code
left join
star_secure.a_user b
on 
a.int1=b.game_id and 
a.int2=b.sn_id and 
a.int3=b.client_id and
a.int4=b.user_uid
where a.char15 = 'Prefinal'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
--limit 20000
;

insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char15,date1,char1,char2,char3,char4,char5,char6,char7,char8,char9,char10,char11,char12,int5,int6,int1,int2,int3,int4)
Select
'Final Final',
a.date1 as 'date',
--dateobj as 'date2',
d.client_name as 'Long client name',
d.OS as 'OS',
d.friendly_name as 'Short client name',
c.social_network as 'Social Network',
a.char1 as country_name,
a.char2 as country_region,
a.char3 as country_sub_region,
a.char4 as country_territory,
a.char5 as primary_language,
a.char6 as 'Game Language',
a.char7 as 'Mobile Device Model',
b.game_name,
e.num_week as 'Week',
e.num_year as 'Year',
Sum(a.int6) as 'Installs',
Sum(a.int7) as 'Revenue',
Sum(a.int5) as 'Tickets',
Sum(a.int4) as 'DAU'
from etl_temp.tmp_gena_ocpr a
inner join 
lookups.l_game b 
on a.int1 = b.game_id
left join
lookups.l_social_network c
on a.int2=c.sn_id
left join
lookups.l_client d
on a.int3=d.client_id
left join
    star.d_date e
on a.date1::date=e.dateobj::date 
where a.char15 = 'Final'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
--limit 10000
;

Select

char1 as 'Long client name',
lower(char2) as 'OS',
char3 as 'Short client name',
char4 as 'Social Network',
char5 as country_name,
char6 as country_region,
char7 as country_sub_region,
char8 as country_territory,
char9 as primary_language,
char10 as 'Game Language',
char11 as 'Mobile Device Model',
char12 as game_name,
int5 as 'Week',
int6 as 'Year',
max(e.dateobj::date) as 'date',
Case when Avg(int1) is null then 0 else Avg(int1) end as 'Installs',
Case when Avg(int2) is null then 0 else Avg(int2) end as 'Revenue',
Case when Avg(int3) is null then 0 else Avg(int3) end as 'Tickets',
Case when Avg(int4) is null then 0 else Avg(int4) end as 'DAU'
from etl_temp.tmp_gena_ocpr a
left join
    star.d_date e
on a.int5=e.num_week and a.int6=e.num_year
where char15 = 'Final Final'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14