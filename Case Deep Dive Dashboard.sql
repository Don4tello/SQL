COMMIT;
set search_path = etl_temp,report,star,star_secure,hist,staging,lookups,logging,ztrack; 
set session characteristics as transaction isolation level read committed;
-- 1 ADDING TIERING
INSERT /*+ direct */ into etl_temp.tmp_gena_ocpr(char1,char2,char3,char4,float1,date1)
Select 'tier',a.varchar2,a.varchar3,a.varchar4,a.value1,a.dateobj
from(Select a.counter,a.varchar2,a.varchar3,a.varchar4,a.value1,a.date1,b.dateobj,rank() over (partition by a.Varchar2,b.dateobj order by a.date1 desc) as rank
from report.a_team_datastore a
left join star.d_date b
on a.date1<= b.dateobj and b.dateobj between current_date-90 and current_date-1
where counter='tier') a
where a.rank=1
group by 1,2,3,4,5,6;
-- 2 COUNTING REPLIES
insert /*+direct*/ into etl_temp.tmp_gena_ocpr (char1,char2,char3,date1,char4)
select
'message',
a.ID,
a.PARENTID,
A.MESSAGEDATE,
a.createdbyid
from

(select
ID,
CREATEDBYID,
PARENTID,
CREATEDDATE,
SYSTEMMODSTAMP,
INCOMING,
MESSAGEDATE,
rank() over (partition by id order by systemmodstamp desc) as rank
from
staging.s_sfdc_email_message

where

INCOMING = 'false' 
and date(CREATEDDATE) BETWEEN current_date -100 AND current_date - 1 
AND date(MESSAGEDATE) BETWEEN current_date -100 AND current_date - 1 
AND createdbyid not in ( '00560000001Z0PbAAK', '00560000001WAMzAAO') -- Social Support User & ZRM Admin
) a

where a.rank = 1
;

-- 4 COMBINING REPLIES AND MASS REPLIES
insert /*+direct*/ into etl_temp.tmp_gena_ocpr (int1,char2,date1,char1,char4)
Select count(a.char2),a.char3,a.date1::date, 'messagefinal', a.char4
from etl_temp.tmp_gena_ocpr a
where a.char1='message'
group by 2,3,4,5
;

-- 5 ADDING CASES
insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char15,date1,char1,char2,char3,char4,char5,char6,char7,char8,char9,char10,char12,char13,int1,char14,char11)
Select 
'Tickets',
a.CREATEDDATE::date,
a.CASENUMBER,
a.CATEGORY__C,
a.game__c,
a.id,
a.CATEGORY_TYPE__C,
a.status,
a.CURRENT_ISSUE_1__C,
case when a.LAST_QUEUE_OWNER__C ilike '%black%' then 'Zynga Diamond'
     when a.LAST_QUEUE_OWNER__C ilike '%billing%' then 'Billing'
     when a.LAST_QUEUE_OWNER__C in ('BBB','CSBBB','ERT','Privacy','Report Abuse Queue','Resolve - Tier 3','Special Cases-ESC-1','Wire Confirmations','Wire Requests') then 'ERT'
     when a.LAST_QUEUE_OWNER__C ilike '%winback%' then 'Winback'
     when a.LAST_QUEUE_OWNER__C ilike '%special%' then 'Special'
else 'Standard' end as 'Queue Divisions', 
a.Segmentation_Group__c, 
a.Channel_SLA_Time__c, 
a.locale_name__c,
a.DISPOSITION__C,
case when length(a.social_network_uid__c) - length(translate(a.social_network_uid__c,'1234567890','')) = length(a.social_network_uid__c) then
	case when length(a.social_network_uid__c) between 2 and 18 then a.social_network_uid__c::INT 
	else -999 end 
else -999 end as user_uid,
a.Game_Origin__c,
a.Case_Channel__c
from (Select 
a.CASENUMBER,
a.CATEGORY__C,
a.id,
a.CREATEDDATE,
a.game__c,
a.CATEGORY_TYPE__C,
a.CURRENT_ISSUE_1__C,
a.SUBJECT,
a.LAST_QUEUE_OWNER__C, 
a.Segmentation_Group__c, 
a.Channel_SLA_Time__c, 
a.status,
a.locale_name__c,
a.DISPOSITION__C,
a.social_network_uid__c,
a.Game_Origin__c,
a.Case_Channel__c,
rank() over (partition by a.id order by a.systemmodstamp desc) as rank
 from staging.s_sfdc_case a 
where date(a.CREATEDDATE) BETWEEN current_date-100
AND current_date - 1 AND a.DISPOSITION__C <> 'Duplicate Incident'
AND a.LAST_QUEUE_OWNER__C NOT IN ('(SAD)', 'Bounces', 'CSIT', 'DxDiag', 'Email Demo', 'Awaiting Player Reply', 'Fake Queue', 'Gerardo Enrique Romero Rivera', 'Kim Florence', 'Legacy No Answer', 'No Game Queue', 'ZZ - CS Product Only', 'Game Closure'))a
where a.rank=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;

-- 7 LAST SURVEY SCORES AND SURVEY DATE
INSERT /*+ direct */ into etl_temp.tmp_gena_ocpr (char1,date1,char2,char3,float1,float2,float3,float4,float5,char4)
SELECT
    'Surveys',
    b.CREATEDDATE::date,
    b.CASE__C,
    a.survey_id,
    a.NPS,
    a.FCRR,
    a.Num_contacts,
    a.CSAT,
    a.Agent_Rating,
    b.ownerid
FROM
    report.survey_response a
inner join
staging.s_sfdc_survey_taker b on
a.survey_id=b.id
where b.CREATEDDATE::DATE BETWEEN current_date -100 AND current_date - 1
group by 1,2,3,4,5,6,7,8,9,10
;
-- 8 adding Category Names to tmp_gena
insert /*+direct*/ into etl_temp.tmp_gena_ocpr (char1,char7,char8)
select 'categories',a.ID, a.NAME
from (select a.ID,a.NAME,a.systemmodstamp,rank()over(partition by a.ID order by a.systemmodstamp desc) as rank
from staging.s_sfdc_category a
group by 1,2,3) a
where a.rank=1 and a.name <> 'Other - MET Closure Duplicate Incident'
group by 1,2,3
;

-- 9 LEFT JOINING ALL ABOVE
insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char1,int5,int6,char2,char3,char4,char5,char6,char8,
--char7,char9,char10,char11,char12,
char14,
--float1,float2,float3,float4,char15,
date1,
--date2,
date3,
--int1,
char13,int2,int3)
Select 
'Final',
b.int1 as 'UID',
d.game_id as 'Game ID',
b.char1 as 'Casenumber',
d.varchar2 as 'Game Name',
d.varchar5 as 'Division',
a.char4 as 'OwnerID',
c.char8 as 'Category Name',
b.char4 as case_id,
case when b.char7 is null then 'Not used'
else b.char7 end as 'Current Issue 1',
b.date1 as 'Ticket Creation Date',
date(max(a.date1)) as 'Send Date',
case when Sum(a.int1) = 1 then 'First Contact'
when Sum(a.int1) = 2 then 'Second Contact'
when Sum(a.int1) = 3 then 'Third Contact'
when Sum(a.int1) >= 4 then 'Fourth Contact or more'
else null
end as 'Actual Resolution per Case',
Count(distinct b.char1) as 'Casenumbercount',
Sum(a.int1) as 'Responses sent'
from
etl_temp.tmp_gena_ocpr b
left join
etl_temp.tmp_gena_ocpr a on
a.char2=b.char4 and a.char1='messagefinal'
left join
etl_temp.tmp_gena_ocpr c on
b.char2=c.char7 and c.char1 = 'categories'
left join
report.a_team_datastore d ON
b.char3 = d.varchar1 AND counter ='game_name'
where
date(b.date1) <current_date AND
date(a.date1)<current_date AND b.char15 in ('Tickets', 'Projects')

group by 1,2,3,4,5,6,7,8,9,10,11
;

--LAST login Date Last Pay Date Engagement Score
insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char15,char14,char2,int6,int5,int9,date1,float1,date2,date3,int7,int8,float2)
Select 
'Last info',
'FBUID',
b.char8,
a.game_id,
a.user_uid,
a.zid,
b.date1,
a.engagement_30day,
max(a.last_payment_timestamp),
max(a.lastdate),
case when date(max(a.last_payment_timestamp)) >= date(b.date1) then 1 else 0 end as 'Money spend after creating ticket?',
case when date(max(a.lastdate)) >= date(b.date1) then 1 else 0 end as 'Still playing after creating Ticket?',
row_number() over (partition by b.char8 order by max(a.lastdate) desc) as rank
from a_user a
inner join etl_temp.tmp_gena_ocpr b
on a.user_uid=b.int5 and a.game_id=b.int6 AND b.char1 = 'Final'
Group by 1,2,3,4,5,6,7,8
;

--LAST login Date Last Pay Date Engagement Score
insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char15,char14,char2,int6,int5,int9,date1,float1,date2,date3,int7,int8,float2)
Select 
'Last info',
'ZID',
b.char8,
a.game_id,
a.zid,
a.zid,
b.date1,
a.engagement_30day,
max(a.last_payment_timestamp),
max(a.lastdate),
case when date(max(a.last_payment_timestamp)) >= date(b.date1) then 1 else 0 end as 'Money spend after creating ticket?',
case when date(max(a.lastdate)) >= date(b.date1) then 1 else 0 end as 'Still playing after creating Ticket?',
row_number() over (partition by b.char8 order by max(a.lastdate) desc) as rank
from a_user a
inner join etl_temp.tmp_gena_ocpr b
on a.zid=b.int5 and a.game_id=b.int6 AND b.char1 = 'Final'
Group by 1,2,3,4,5,6,7,8
;
insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char15,char14,char2,int6,int5,int9,date1,char3,char4,float1)
Select
'a_user_day',
'FBUID',
b.char8,
a.game_id,
a.user_uid,
a.zid,
a.stat_date,
a.is_7_day_return,

a.is_30_day_return,
row_number() over (partition by a.user_uid,a.stat_date,a.game_id order by a.stat_date desc) as rank
from star_secure.a_user_day a
inner join etl_temp.tmp_gena_ocpr b
on a.user_uid=b.int5 and a.game_id=b.int6 and b.date1=stat_date::date AND b.char1 = 'Final'
group by 1,2,3,4,5,6,7,8,9
;

insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char15,char14,char2,int6,int5,int9,date1,char3,char4,float1)
Select
'a_user_day',
'ZID',
b.char8,
a.game_id,
a.zid,
a.zid,
a.stat_date,
a.is_7_day_return,
a.is_30_day_return,
row_number() over (partition by a.zid,a.stat_date,a.game_id order by a.stat_date desc) as rank
from star_secure.a_user_day a
inner join etl_temp.tmp_gena_ocpr b
on a.zid=b.int5 and a.game_id=b.int6 and b.date1=stat_date::date AND b.char1 = 'Final'
group by 1,2,3,4,5,6,7,8,9
;

INSERT /*+ direct */ into etl_temp.tmp_gena_ocpr(char15,char1,date1,date2,char2,char3,char4,int2,int3,int1,float1)
Select
'agents',
b.id as createdbyid,
a.date1::date as sent_date,
b.LASTMODIFIEDDATE::date,
a.char3 as case_id,
b.NAME,
c.char2 as casenumber,
c.int5 as UID,
c.int6 as game_id,
count(distinct a.char2) as msg_id,
row_number() over (partition by b.id,a.char3 order by b.LASTMODIFIEDDATE::date desc) as rank
from s_sfdc_user b
left join etl_temp.tmp_gena_ocpr a
on b.id=a.char4 and a.char1 = 'message'
inner join etl_temp.tmp_gena_ocpr c
on a.char3 = c.char8 and c.char1='Final'
group by 1,2,3,4,5,6,7,8,9
;

INSERT /*+ direct */ into etl_temp.tmp_gena_ocpr(char15,char1,date1,char2,char3,char4,int2,int3,int1,float1,char5)
Select 'agentprod',a.char1,a.date1,a.char2,a.char3,a.char4,a.int2,a.int3,a.int1,a.float1,b.char4
from etl_temp.tmp_gena_ocpr a
left join etl_temp.tmp_gena_ocpr b
on b.char4=a.char1 and a.char2=b.char2 and b.char1 = 'Surveys'
where a.char15 = 'agents' and a.float1 = 1
;

insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char15,char14,char2,int6,int5,int9,date1,float1,date2,date3,int7,int8,float2,float3)
Select 'engagement',char14,char2,int6,int5,int9,date1,float1,date2,date3,int7,int8,float2,
row_number() over (partition by char2 order by date1 desc) as rank
from etl_temp.tmp_gena_ocpr
where char15 = 'Last info'
;

insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char15,char14,char2,int6,int5,int9,date1,char3,char4,float1,float3)
select '7dayreturn',char14,char2,int6,int5,int9,date1,char3,char4,float1,
row_number() over (partition by char2 order by date1 desc) as rank
from etl_temp.tmp_gena_ocpr
where char15 = 'a_user_day'
;

Select
e.date1::date as 'Sent date',
e.char3 as 'Agent',
a.char2 as 'Casenumber',
a.char3 as 'Game Name',
a.char4 as 'Division',
a.int6 as 'Game ID',
  split_part(a.char6,' - ',1) as 'Category Level 1'
  ,case when length(a.char6)-length(translate(a.char6,' - ','')) >= 3 then split_part(a.char6,' - ',2)
        when length(a.char6)-length(translate(a.char6,' - ','')) =  1 then split_part(a.char6,' - ',2)
        when length(a.char6)-length(translate(a.char6,' - ','')) <  1 then 'none'
        when regexp_like(split_part(a.char6,' - ',length(a.char6)-length(translate(a.char6,' - ',''))+1),'^[0-9]+$')
        then 'none'
   else 'none' end as 'Category Level 2'
  ,case when length(a.char6)-length(translate(a.char6,' - ','')) >= 4 then split_part(a.char6,' - ',3)
        when length(a.char6)-length(translate(a.char6,' - ','')) <= 1 then 'none'
        when regexp_like(split_part(a.char6,' - ',length(a.char6)-length(translate(a.char6,' - ',''))+1),'^[0-9]+$')
        then 'none'
   else 'none'
end as 'Category Level 3',
case when regexp_instr(a.char6,'[A-Z]+-[0-9]+$') > 0 then substr(a.char6,regexp_instr(a.char6,'[A-Z]+-[0-9]+$')) end as 'Jira',
a.char6 as 'Category Name',
b.char5 as 'Category Type',
b.char8 as 'Queue Division',
b.char9 as 'Segmentation Group',
b.char4 as 'Case ID',
b.char12 as 'Language',
b.char13 as 'Disposition',
b.char6 as 'Project',
case when b.char14 is null then 'unknown' else b.char14 end as 'Game Origin',
b.char11 as 'Channel',
date(e.date1) as 'Survey Date',
b.char6 as 'Status',
a.char14 as 'Current Issue 1',
a.char13 as 'Actual Resolution per Case',
a.int2 as 'Casenumbercount',
a.int3 as 'Responses sent',
a.int5 as 'User UID',
date(a.date1) as 'Ticket Creation Date',
f.float1 as 'NPS',
f.float2 as 'Was your Issue resolved?',
f.float4 as 'CSAT',
f.float5 as 'Agent Rating',
case when f.float3 = 1.0 then 'First Contact'
when f.float3 = 2.0 then 'Second Contact'
when f.float3 = 3.0 then 'Third Contact'
when f.float3 = 4.0 then 'Fourth Contact or more'
when f.float3 = 0 then null
else null
end as 'Customer Effort',
c.float1 as '30 Day Engagement',
c.date2 as 'Last Payment Date',
c.date3 as 'Last Login Date',
case when d.char3 = 'f' then 0 when d.char3 = 't' then 1 else null end as '7 Day Return',
case when d.char4 = 'f' then 0 when d.char4 = 't' then 1 else null end as '30 Day Return',
c.int8 as 'Still playing?',
c.int7 as 'Still paying?',
case when c.char14 = 'FBUID' then 'FBUID' 
when c.char14 = 'ZID' then 'ZID' 
when d.int9 is null then 'Unknown'
else 'Unknown' end
as 'SNUID',
d.int9 as 'ZID',
Case when f.float1 <= 6 then Count(distinct f.float1) else 0 end as 'detractors',
Case when f.float1 BETWEEN 7 AND 8 then Count(distinct f.float1) else 0 end as 'passives',
Case when f.float1 >= 9 then Count(distinct f.float1) else 0 end as 'promoters',
Count(distinct f.float1) as 'Surveynumbercount'
from 
etl_temp.tmp_gena_ocpr e
left join
etl_temp.tmp_gena_ocpr a
on a.char8=e.char2 and a.char5=e.char1 and a.char1='Final'
LEFT JOIN
etl_temp.tmp_gena_ocpr b
on e.char2=b.char4 and b.char15 = 'Tickets'
LEFT JOIN
etl_temp.tmp_gena_ocpr f
on f.char4=e.char5 and e.char2=f.char2 and f.char1 = 'Surveys'
LEFT JOIN
etl_temp.tmp_gena_ocpr c
on e.char2=c.char2 and e.int3=c.int6 and c.char15 ='engagement' and c.float3= 1
LEFT JOIN
etl_temp.tmp_gena_ocpr d
on e.char2=d.char2 and e.int3=d.int6 and d.char15 ='7dayreturn' and d.float3=1
where  e.char15 ='agentprod' and e.float1 = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42