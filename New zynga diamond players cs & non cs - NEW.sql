--<?xml version="1.0" encoding="utf-8"?>
--<report displayname="New Diamond Players per Month">
--  <description>This Report shows you GPX Metrics in a summary by Queue Divisions and Games for a Quarter and a single Day </description>
--  <query name="List of Diamond Players" dsn="vwh"><![CDATA[



COMMIT;
set search_path = etl_temp,report,star,star_secure,hist,staging,lookups,logging,ztrack; 
set session characteristics as transaction isolation level read committed;
INSERT /*+ direct */ INTO etl_temp.tmp_gena_ocpr(char15,int1,float1)
SELECT
    'All Payments',
    a.user_uid,
    SUM(a.amount)
FROM
    report.v_payment a
inner join
(select a.user_uid, a.lastdate, a.email_addr,c.varchar2 as game_name, row_number() over (partition by a.user_uid order by a.lastdate desc) as rank 
from star_secure.a_user_day a
left join
report.a_team_datastore c
on a.game_id = c.game_id and c.counter = 'game_name'
where a.country = 'US'
) c
on a.user_uid=c.user_uid
where rank = 1
GROUP BY 1,2
;
INSERT /*+ direct */ INTO etl_temp.tmp_gena_ocpr(char15,int1,float2)
SELECT
    'All Payments final',
    a.user_uid,
    Sum(a.amount) as 'Total Spend'
FROM
    report.v_payment a
inner join
etl_temp.tmp_gena_ocpr b
on b.int1=a.user_uid and b.char15 = 'All Payments'
left join
report.a_team_datastore c
on a.game_id = c.game_id and c.counter = 'game_name'
inner join star.d_date d
on a.date_trans::date=d.dateobj

where d.year_month_num ilike '20%'
GROUP BY 1,2
;

INSERT /*+ direct */ INTO etl_temp.tmp_gena_ocpr(char15,int1,float2,char4)
Select 
'Diamond',
int1 as 'user_uid',
Sum(float2) as 'Total Spend',
case when Sum(float2) <= 10000 then 'Non-Diamond Player'
when Sum(float2)>= 10000 then 'Zynga Diamond Player'
else 'Unknown'
end as 'Diamond Status'
from etl_temp.tmp_gena_ocpr
where char15 = 'All Payments final' and date1::date between '2014-01-01' and current_date-1
Group by 1,2

;
insert /*+ direct*/ into etl_temp.tmp_gena_ocpr (char15,date1,char2,char3,char5,char6,char7,char8,char9,char10,char12,char13,int1)
Select 
'Tickets',
a.CREATEDDATE::date,
a.CATEGORY__C,
a.game__c,
a.CATEGORY_TYPE__C,
case when a.CURRENT_ISSUE_2__C = 'EMTC Autoresponse' then 'EMTC'
     when a.CURRENT_ISSUE_2__C = 'MR' then 'EMTC Snippets' 
     when a.CURRENT_ISSUE_2__C = 'FIGSP AR' then 'FIGSP Auto Response'
     when a.CURRENT_ISSUE_2__C = 'AutoResponsePilot, INCENT' then 'AutoResponsePilot, INCENT'
     when a.CURRENT_ISSUE_2__C = 'AutoResponsePilot-Mobile OOD' then 'AutoResponsePilot-Mobile OOD'
     when a.CURRENT_ISSUE_2__C = 'AutoResponsePilot-Mobile Tech' then 'AutoResponsePilot-Mobile Tech'
     when a.CURRENT_ISSUE_2__C = 'AutoResponsePilot/incent' then 'AutoResponsePilot, INCENT'
     when a.CURRENT_ISSUE_2__C = 'AutoResponsePilot-E&I-E&I' then 'AutoResponsePilot-E&I'
     when a.CURRENT_ISSUE_2__C = 'AutoResponsePilot-E&I' then 'AutoResponsePilot-E&I'
     when a.CURRENT_ISSUE_2__C = 'AutoResponsePilot-Mobile OOD, incent' then 'AutoResponsePilot-Mobile OOD, incent'
     when a.CURRENT_ISSUE_2__C = 'AutoResponsePilot-Web Tech' then 'AutoResponsePilot-Web Tech'
     when a.CURRENT_ISSUE_2__C = 'AutoResponsePilot-Mobile Tech, incent' then 'AutoResponsePilot-Mobile Tech, incent'
     when a.CURRENT_ISSUE_2__C = 'AutoResponsePilot' then 'AutoResponsePilot'
     when a.CURRENT_ISSUE_2__C = 'AutoResponsePilot-Web Feeds' then 'AutoResponsePilot-Web Feeds'
     when a.CURRENT_ISSUE_2__C = 'DE-O' then 'Ortsbo German'
     when a.CURRENT_ISSUE_2__C = 'IT-O' then 'Ortsbo Italian'
     when a.CURRENT_ISSUE_2__C = 'ES-O' then 'Ortsbo Spanish'
     when a.CURRENT_ISSUE_2__C = 'PT-O' then 'Ortsbo Portuguese'
     when a.CURRENT_ISSUE_2__C = 'FR-O' then 'Ortsbo French'
     else 'Not tied to Projects' end,
a.CURRENT_ISSUE_1__C,
case when a.LAST_QUEUE_OWNER__C ilike '%black%' then 'Zynga Black'
     when a.LAST_QUEUE_OWNER__C ilike '%billing%' then 'Billing'
     when a.LAST_QUEUE_OWNER__C in ('BBB','CSBBB','ERT','Privacy','Report Abuse Queue','Resolve - Tier 3','Special Cases-ESC-1','Wire Confirmations','Wire Requests') then 'ERT'
     when a.LAST_QUEUE_OWNER__C ilike '%winback%' then 'Winback'
     when a.LAST_QUEUE_OWNER__C ilike '%black%' then 'Zynga Black'
     when a.LAST_QUEUE_OWNER__C ilike '%special%' then 'Special'
else 'Standard' end as 'Queue Divisions', 
a.Segmentation_Group__c, 
a.Channel_SLA_Time__c, 
a.locale_name__c,
a.DISPOSITION__C,
case when length(a.uid)-length(translate(a.uid,'1234567890','')) between 1 and 18 then
         case when length(translate (lower(a.uid), '01234567890abcdefghijklmnopqrstuvwxyz=.,?+-*/:;&#@_<>%!|()[]{}­`~?? ',''))=0 then 
              translate (lower(a.uid), 'abcdefghijklmnopqrstuvwxyz=.,?+-*/:;&#@_<>%!|()[]{}­`~?? ','')::INT 
              else -999 end  
         else -999 end as user_uid
from (Select 
a.CASENUMBER,
a.CATEGORY__C,
a.id,
a.CREATEDDATE,
a.game__c,
a.CATEGORY_TYPE__C,
a.CURRENT_ISSUE_2__C,
a.SUBJECT,
a.LAST_QUEUE_OWNER__C, 
a.Segmentation_Group__c, 
a.Channel_SLA_Time__c, 
a.CURRENT_ISSUE_1__C,
a.locale_name__c,
a.DISPOSITION__C,
a.social_network_uid__c as uid,
rank() over (partition by a.social_network_uid__c order by a.systemmodstamp desc) as rank
 from staging.s_sfdc_case a 
inner join star.d_date d
on a.createddate::date=d.dateobj
where d.year_month_num ilike '20%'
AND a.LAST_QUEUE_OWNER__C NOT IN ('(SAD)', 'Bounces', 'CSIT', 'DxDiag', 'Email Demo', 'Awaiting Player Reply', 'Fake Queue', 'Gerardo Enrique Romero Rivera', 'Kim Florence', 'Legacy No Answer', 'No Game Queue', 'ZZ - CS Product Only', 'Game Closure'))a
where a.rank=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
;


Select

Sum(a.float2)::int as 'Spend',
Count(distinct a.int1) as 'Number of UUIDs',
a.char4 as 'Diamond Status',
case when b.char15 = 'Tickets' then 'CS' else 'None-CS' end as 'CS Metric'
from etl_temp.tmp_gena_ocpr a
left join etl_temp.tmp_gena_ocpr b
on a.int1=b.int1 and b.char15 = 'Tickets'
where a.char15 = 'Diamond'
Group by 3,4
limit 10000

-- ]]></query>
--
--   <column colname="month" format="string"/>
--   <column colname="email" format="string"/>
--   <column colname="Last Game played" format="string"/>
--<parameter type="selectQuery" display="month" name="month" displayColumn="month" valueColumn="month" default="201401">
--<selectQuery name="month" dsn="vwh">
--select distinct year_month_num as month
--from d_date
--where dateobj between current_date-360 and current_date
--order by 1 desc
--</selectQuery>
--</parameter>
--  
--</report>
