--<?xml version="1.0" encoding="utf-8"?>
--<report displayname="GPX Metrics by Queue Division and Game - Quarter and Day">
--  <description>This Report shows you GPX Metrics in a summary by Queue Divisions and Games for a Quarter and a single Day </description>
--  <query name="GPX Metrics - Queue Divisions - Day" dsn="sample_vwh"><![CDATA[
--
--


set search_path = etl_temp,star,star_secure,hist,staging,lookups,logging,ztrack; 
set session characteristics as transaction isolation level read committed;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_join_OCPR (counter,join1,varchar1)
Select 'game_name',a.Varchar2,a.Varchar3
from(select Varchar2,Varchar3,Game_id,date3,Rank() over (partition by Varchar1 order by date3 desc)as rank
from report.a_team_datastore
where counter='game_name')a
where a.rank=1
group by 1,2,3;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_join_OCPR (counter,join1,varchar1)
Select 'agent',a.ID,a.sites__c
from(select ID,systemmodstamp,SITES__C,rank() over(partition by ID order by systemmodstamp desc) as rank
from staging.S_sfdc_user) a
where a.rank=1;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_join_OCPR(counter,join1,varchar1)
select 'Mass',a.id,a.ownerid
from (select a.id,a.ownerid,a.systemmodstamp,rank() over (partition by a.id order by a.systemmodstamp desc) as rank
from staging.s_sfdc_audit_mass_transaction a
where a.Campaign__c is not null
group by 1,2,3) a
where a.rank = 1
group by 1,2,3;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_join_OCPR (counter,join1,float1)
select 'att',a.ID,a.attribute
from (select a.ID,a.systemmodstamp,max(case when a.Player_Attributes__c ilike 'VIP' then 11 when a.Player_Attributes__c ilike 'ZIP' then 10 when a.Player_Attributes__c ilike 'Special' then 9 else 0 end) as attribute,
rank() over (partition by a.ID order by a.systemmodstamp desc) as rank
from staging.s_sfdc_contact a
inner join star.d_date b
on a.systemmodstamp::date=b.dateobj
where num_year||quarter_name = '$quarter$'
group by 1,2) a
where a.rank = 1 
group by 1,2,3;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_join_OCPR (counter,join1,varchar1)
Select 'case',ID,MassTransactionId__c
from staging.s_sfdc_case a
inner join star.d_date b
on a.systemmodstamp::date=b.dateobj
where MassTransactionId__c is not null
and num_year||quarter_name = '$quarter$'
group by 1,2,3;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_join_OCPR (counter,join1,varchar1,varchar2,float1,date1)
Select 'tier',a.varchar2,a.varchar3,a.varchar4,a.value1,a.dateobj
from(Select a.counter,a.varchar2,a.varchar3,a.varchar4,a.value1,a.date1,b.dateobj,rank() over (partition by a.Varchar2,b.dateobj order by a.date1 desc) as rank
from report.a_team_datastore a
inner join star.d_date b
on a.date1<= b.dateobj and num_year||quarter_name = '$quarter$'
where counter='tier') a
where a.rank=1
group by 1,2,3,4,5,6;
insert /*+direct*/ into etl_temp.tmp_sfdc_join_OCPR (counter,join1,varchar1,varchar2)
select 'cat',a.ID,a.NAME,case when instr(a.NAME,' -') = 0 then a.NAME else left(a.NAME,instr(a.NAME,' -')) end
from (select a.ID,a.NAME,a.systemmodstamp,rank()over(partition by a.ID order by a.systemmodstamp desc) as rank
from staging.s_sfdc_category a
group by 1,2,3) a
where a.rank=1
group by 1,2,3,4;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,varchar1,varchar2,date1,varchar5,varchar3,varchar4,float5,float3,float4,float6,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,float1,float2,float7,float8,float9,float10,float11,float12,varchar13,varchar14,varchar17,float13,varchar18)
Select 
'tickets',
CATEGORY__C,
disposition__c,
id,
createddate,
game__c as 'game',
'social_network',
'client',
c.game_id,
sn_id,
client_id,
case when length(a.social_network_uid__c) - length(translate(a.social_network_uid__c,'1234567890','')) = length(a.social_network_uid__c) then
	case when length(a.social_network_uid__c) between 2 and 18 then a.social_network_uid__c::INT 
	else -999 end 
else -999 end as user_uid,
origin,
'locale',
'site',
'game_origin',
case when a.createddate::date >= '2012-07-25' then 
        case when a.Case_Channel__c is null then 'EMAIL' 
        else a.Case_Channel__c end 
     when a.createddate::date < '2012-07-25' then 
        case when a.origin = 'Chat' then 'CHAT' 
        when a.origin = 'Phone' then 'PHONE' 
        else 'EMAIL' end
         end 
         || '--' || 
  (case when a.createddate::date >= '2012-07-25' then 
         case when a.Channel_SLA_Time__c is null then 'SLA-1' 
                else a.Channel_SLA_Time__c end 
        when a.createddate::date < '2012-07-25' then 
                case when a.by_paid_player__c is null then 'SLA-48 hours' 
                when a.by_paid_player__c = 'Unpaid' then 'SLA-48 hours' 
                when a.by_paid_player__c = 'Paid' then 'SLA-18 hours' 
                when a.by_paid_player__c = 'Platinum' then 'SLA-12 hours' 
                when a.by_paid_player__c = 'Diamond' then 'SLA-8 hours' 
                else 'SLA-48 hours' end end) as channel_sla,
masstransactionId__c,
contactid,
1 as chat_exclusion,
1 as queue_exclusion,
payment_likelihood_score__c,
1,
case when last_queue_owner__c in ('Billing Email', 'Billing Email-ESC-1', 'Billing Phone', 'Billing Phone-ESC-1', 'Billing zAll-ESC-2', 'Billing zRefunds') then 1 
	when last_queue_owner__c in ('Zynga Black', 'Zynga Black-ESC-1', 'Zynga Black-ESC-2', 'Zynga Black Phone', 'Zynga Black Phone-ESC-1')  then 2 
	when last_queue_owner__c in ('CSBBB','csbbb','BBB','ERT','Resolve - Tier 3','Report Abuse Queue','privacy','Privacy','Special  Cases-ESC-1','Special Cases - Tier 3','Special Cases-ESC-1','Wire Confirmations','Wire Requests') then 5
        when last_queue_owner__c in ('Special', 'Special Cases-ESC-1', 'Special ESC-1', 'Special-Rehab') then 3
	else 4 end as queue_group,
100 as life,
100 as thirty,
100 as sixty,
CATEGORY__C,
CATEGORY_TYPE__C,
by_paid_player__c,
-999 as Business_unit,
status
from staging.s_sfdc_case a
inner join star.d_date b
on a.createddate::date = b.dateobj
left join report.a_team_datastore c
on a.game__c=c.varchar1 and c.counter='game_name'
where num_year||quarter_name = '$quarter$'
;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,date1,join1,float1,float2,float3,float4,float5,float6,float7,float8,float9,float10,float11,float12,float13,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,varchar13,varchar14,varchar15,varchar17,varchar18)
Select'category',a.date1,a.varchar11,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float10,a.float11,a.float12,a.float13,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar14,b.varchar1,b.varchar2,a.varchar17,a.varchar18
from etl_temp.tmp_sfdc_case_b_OCPR a
left join etl_temp.tmp_sfdc_join_OCPR b
on a.join1=b.join1 and b.counter='cat'
where a.counter='tickets';
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,float1,float2,float3,float4,float5,float6,float7,float8,float9,float10,float11,float12,float13,float15,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,varchar13,varchar14,varchar15,varchar16,varchar17,varchar18)
Select 'Mass',a.join1,a.date1,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float10,a.float11,a.float12,a.float13,case when c.counter is null then 1 else 0 end,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,b.varchar1,a.varchar17,a.varchar18
from etl_temp.tmp_sfdc_case_b_OCPR a
left join etl_temp.tmp_sfdc_join_OCPR b
on a.join1=b.join1 and b.counter='Mass'
left join etl_temp.tmp_sfdc_join_OCPR c
on a.join1=c.join1 and c.counter='mass_edit'
where a.counter='category';
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,float1,float2,float3,float4,float5,float6,float7,float8,float9,float10,float11,float12,float13,float15,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,varchar13,varchar14,varchar15,varchar16,varchar17,float14,varchar18)
Select 'Seg',a.float6,a.date1,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float10,a.float11,a.float12,a.float13,a.float15,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,a.varchar16,case when a.float10 >= 501 then '10000+ Lifetime' when h.varchar1 is not null then h.varchar1 when i.varchar1 is not null then i.varchar1 when j.varchar1 is not null then j.varchar1 else '0-4.99 Last 30' end,case when a.float10 >= 501 then 9999 when h.varchar1 is not null then h.sn_id when i.varchar1 is not null then i.sn_id when j.varchar1 is not null then j.sn_id else 0 end,a.varchar18
from etl_temp.tmp_sfdc_case_b_OCPR a
left join report.a_team_datastore h
on (a.float10 between h.value1 and h.value2 OR a.float12 between h.value3 and h.value4 OR a.float11 between h.value5 and h.client_id) and a.date1 between h.date1 and h.date2 and h.game_id=a.float5 and h.counter='seg_game_rules'
left join report.a_team_datastore i 
on (a.float10 between i.value1 and i.value2 OR a.float12 between i.value3 and i.value4 OR a.float11 between i.value5 and i.client_id) and a.date1 between i.date1 and i.date2 and i.counter='seg_Global_rules'
left join report.a_team_datastore j
on a.varchar17=j.varchar2 and a.date1< '2012-07-25' and j.counter='seg_legacy_rules'
where a.counter='Mass';
INSERT /*+ direct */ into etl_temp.tmp_skb_OCPR (metric5,game_id,sn_id,client_id,user_uid)
select 'ticket',-2,a.float3,-2,a.float6
from etl_temp.tmp_sfdc_case_b_OCPR a
where a.counter = 'tickets' 
group by 1,2,3,4,5;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_join_OCPR (counter,float1,float2,float3,join1,Varchar1)
select 'v_user',b.game_id,b.sn_id,b.client_id,a.user_uid,a.ip_country
from star.v_user a
inner join etl_temp.tmp_skb_OCPR b
on a.user_uid = b.user_uid and a.game_id = b.game_id and a.client_id = b.client_id and a.sn_id = b.sn_id and b.metric5 = 'ticket'
group by 1,2,3,4,5,6;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,float1,float2,float3,float4,float5,float6,float7,float8,float9,float10,float11,float12,float13,float15,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,varchar13,varchar14,varchar15,varchar16,varchar17,float14,float16,varchar18)
Select 'country',a.varchar12,a.date1,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float10,a.float11,a.float12,a.float13,a.float15,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,a.varchar16,a.varchar17,a.float14,max(case when b.varchar1 IN ('AE','AT','AU','BE','CA','CH','DE','DK','FI','FR','GB','IE','IL','JP','NL','NO','NZ','SE','SG','US') then 1 else 0 end),a.varchar18
from (Select a.join1,a.date1,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float10,a.float11,a.float12,a.float13,a.float15,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,a.varchar16,a.varchar17,a.float14,a.varchar18,rank () over (partition by varchar2 order by float14 desc) as rank
from etl_temp.tmp_sfdc_case_b_OCPR a
where a.counter='Seg') a
left join etl_temp.tmp_sfdc_join_OCPR b
on a.join1 = b.join1 and b.counter = 'v_user'
where a.rank=1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,26,27,28,29,30,31,32,33,34,35,37;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,float1,float2,float3,float4,float5,float6,float7,float8,float9,float10,float11,float12,float13,float15,float16,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,varchar13,varchar14,varchar15,varchar16,varchar17,float14,varchar18)
Select 'special',a.varchar5,a.date1,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float10,a.float11,a.float12,a.float13,a.float15,a.float16,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,a.varchar16,Case when a.float10 <501 OR a.float10 is null then case when b.float1=11 then 'VIP' when b.float1=10 then 'ZIP' when b.float1=9 then case when a.date2>='2012-09-10' and a.float10<406 then 'Special' when a.date2<'2012-09-10' and a.date2>='2012-07-25' and a.float10<341 and a.float12<301 then 'Special' else a.varchar17 end else a.varchar17 end else a.varchar17 end,a.float14, a.varchar18
from etl_temp.tmp_sfdc_case_b_OCPR a
left join etl_temp.tmp_sfdc_join_OCPR b
on a.join1 = b.join1 and b.counter = 'att'
where a.counter='country';
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,date2,float1,float2,float3,float4,float5,float6,float7,float8,float9,float10,float11,float15,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,varchar13,varchar14,varchar15,varchar16,varchar17,varchar19,varchar18,varchar20)
Select 'tix_final',a.varchar2,a.date1,a.date1,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float13,1,a.float15,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,a.varchar16,case when a.varchar17 = '0-4.99 Last 30' THEN  case when a.float5 = 1 then '0-4.99 Last 30' else  case when a.varchar6 in ('In-game CP','Web') then  case when a.float16 = 0 then  case when a.date2::date>='2013-06-03' THEN 'Forums' else case when a.float7 is null then '0-4.99 Last 30'  when a.float7 =-1 then '0-4.99 Last 30'  when a.float7 < 85 and a.date2::date>='2013-02-22' THEN 'Forums'  when a.float7 < 80 and a.date2::date>='2013-02-08' THEN 'Forums'  when a.float7 < 10 and a.date2::date>='2012-07-25' THEN 'Forums'  else a.varchar17 END end else a.varchar17 end  else a.varchar17 END end  ELSE a.varchar17 END,b.varchar1,'New',a.varchar18
from etl_temp.tmp_sfdc_case_b_OCPR a
left join etl_temp.tmp_sfdc_join_OCPR b
on a.join1=b.join1 and b.counter='game_name'
where a.counter='special'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,date2,float1,float2,float3,float4,float5,float6,float7,float8,float9,float10,float11,float15,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,varchar13,varchar14,varchar15,varchar16,varchar17,varchar18,varchar19,float21,float22,float23,float24,float25,varchar20)
Select 'survey_final',a.varchar2,a.date1,b.CREATEDDATE,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float13,5,a.float15,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,a.varchar16,a.varchar17,'Survey',a.varchar19,c.Agent_Rating,c.FCRR,c.Num_contacts,c.CSAT,c.NPS,a.varchar20
from etl_temp.tmp_sfdc_case_b_OCPR a
inner join staging.s_sfdc_survey_taker b
on a.join1=b.Case__c
inner join report.survey_response c
on b.ID=c.survey_id
inner join star.d_date d
on b.CREATEDDATE::DATE = d.dateobj
where a.counter='tix_final' and num_year||quarter_name = '$quarter$'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,varchar1,varchar2,varchar3,varchar4,varchar5,date1)
select'stonecobra_1',case_id,case_id,email_id,workflow,site,createdbyid,sent_date
from report.uber_productivity a
left join star.d_date b
on a.sent_date::DATE = b.dateobj
where num_year||quarter_name = '$quarter$'
;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,date2,float1,float2,float3,float4,float5,float6,float7,float8,float9,float10,float15,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,varchar13,varchar14,varchar15,varchar16,varchar17,float11,varchar18,varchar19,float12,float13,float14,varchar20)
Select 'sc2',a.join1,a.date1,b.date1,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float13,a.float15,a.varchar1,b.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,case when b.varchar3 = 'Inbound' then a.varchar8 when b.varchar3 = 'Outbound' then case when b.varchar4 in ('Call Fusion') then 'Fusion' when b.varchar4 in ('San Francisco') then 'Zynga' when b.varchar4 is null then 'Unassigned' else b.varchar4 end end,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,b.varchar5,a.varchar17,case when b.varchar3 = 'Updated' then 2 when b.varchar3 = 'Outbound' then 4 end,b.varchar3,a.varchar19,case when a.varchar11 is null then 1 when a.varchar11 is not null then case when b.varchar3 = 'Updated' then 1 when b.varchar3 = 'Outbound' then case when a.varchar16 = b.varchar5 then 0 else 1 end end end,rank() over (partition by a.varchar1 order by b.date1 asc),rank() over (partition by a.varchar2,b.varchar3 order by b.date1 asc),a.varchar20
from etl_temp.tmp_sfdc_case_b_OCPR b
inner join etl_temp.tmp_sfdc_case_b_OCPR a
on a.join1 = b.join1 and a.counter = 'tix_final'
where b.counter='stonecobra_1';
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,date2,float1,float2,float3,float4,float5,float6,float7,float8,float9,float10,float11,float12,float13,float14,float15,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,varchar13,varchar14,varchar15,varchar16,varchar17,varchar18,varchar19,varchar20)
Select 'sc3',a.join1,a.date1,a.date2,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float10,a.float11,a.float12,case when a.float13 >1 then 2 else 1 end,a.float14,a.float15,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,a.varchar16,a.varchar17,a.varchar18,a.varchar19,a.varchar20
from etl_temp.tmp_sfdc_case_b_OCPR a
where counter= 'sc2'
and (case when a.float11 = 2 then 
(case when a.varchar6 = 'Email' then 
(case when a.float14 = 1 then 'Exclude' else 'Include' end)
else 'Include' end) 
else 'Include' end) = 'Include';
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,date2,float1,float2,float3,float4,float5,float6,float7,float8,float9,float10,float11,float12,float13,float14,float15,float16,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,varchar13,varchar14,varchar15,varchar16,varchar17,varchar18,varchar19,date3,varchar20)
Select 'ART',SPLIT_PART(a.varchar10,'--',2),a.date1,a.date2,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float10,a.float11,a.float12,a.float13,a.float14,a.float15,a.flag,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,a.varchar16,a.varchar17,a.varchar18,a.varchar19,lag (a.date2,1,'1900-01-01') over (partition by a.float6 order by a.date2 ASC),a.varchar20
from (Select a.join1,a.date1,a.date2,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float10,a.float11,a.float12,a.float13,a.float14,a.float15,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,a.varchar16,a.varchar17,a.varchar18,varchar19,lag (a.float11,1,0) over (partition by a.varchar2 order by a.date2 asc) as flag,a.varchar20
from etl_temp.tmp_sfdc_case_b_OCPR a
where counter in ('sc3','tix_final')
) a
where a.float11 <> a.flag
;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,date2,float1,float2,float3,float4,float5,float6,float7,float8,float9,float10,float11,float12,float13,float14,float15,float16,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,varchar13,varchar14,varchar15,varchar16,varchar17,varchar18,varchar19,date3,float17,float18,varchar20)
Select 'ART3',a.join1,a.date1,a.date2,a.float1,a.float2,a.float3,a.float4,a.float5,a.float6,a.float7,a.float8,a.float9,a.float10,a.float11,a.float12,a.float13,a.float14,a.float15,a.float16,a.varchar1,a.varchar2,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar11,a.varchar12,a.varchar13,a.varchar14,a.varchar15,a.varchar16,a.varchar17,a.varchar18,a.varchar19,a.date3,b.value1,round((datediff('mi',a.date3,a.date2)/60::float),2),a.varchar20
from etl_temp.tmp_sfdc_case_b_OCPR a
left join report.a_team_datastore b
on b.varchar1=a.join1 and b.counter='sla'
where a.counter='ART'
;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,date2,float1,float2,float3,float4,float5,float8,float9,float10,float15,varchar1,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar13,varchar14,varchar15,varchar17,varchar18,varchar19,varchar20,float6,float11,float19)
Select'agg_tix',a.varchar5,a.date1::date,a.date2::date,a.float1,a.float2,a.float3,a.float4,a.float5,a.float8,a.float9,a.float10,a.float15,a.varchar1,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,case when a.date2 <= current_date-65 then 'none' else a.varchar13 end,case when a.date2 <= current_date-65 then 'none' else a.varchar14 end,case when a.date2 <= current_date-65 then 'none' else a.varchar15 end,a.varchar17,a.varchar18,a.varchar19,a.varchar20,count (distinct a.float6),count (distinct a.varchar2),count (distinct a.varchar12)
from etl_temp.tmp_sfdc_case_b_OCPR a
where a.counter='tix_final'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,date2,float1,float2,float3,float4,float5,float8,float9,float10,float12,float13,float14,float15,varchar1,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar12,varchar13,varchar14,varchar15,varchar17,varchar18,varchar19,varchar20,float6,float11,float19,float20)
Select 'agg_prod',a.varchar5,a.date1::date,a.date2::date,a.float1,a.float2,a.float3,a.float4,a.float5,a.float8,a.float9,a.float10,a.float12,a.float13,a.float14,a.float15,a.varchar1,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar12,case when a.date2 <= current_date-60 then 'none' else a.varchar13 end,case when a.date2 <= current_date-65 then 'none' else a.varchar14 end,case when a.date2 <= current_date-65 then 'none' else a.varchar15 end,a.varchar17,a.varchar18,a.varchar19,a.varchar20,count (distinct a.float6),count (distinct a.varchar2),count (distinct a.varchar12),count (distinct a.varchar16)
from etl_temp.tmp_sfdc_case_b_OCPR a
where counter= 'sc3'
and (case when a.float11 = 2 then 
(case when a.varchar9 = 'Email' then 
(case when a.float14 = 1 then 'Exclude' else 'Include' end)
else 'Include' end) 
else 'Include' end) = 'Include'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,date2,float1,float2,float3,float4,float5,float8,float9,float10,float12,float13,float14,float15,float16,varchar1,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar13,varchar14,varchar15,varchar17,varchar18,varchar19,date3,float17,varchar22,varchar21,float18,float6,float11,float19,float20,varchar20)
Select'agg_art',a.varchar5,a.date1::date,a.date2::date,a.float1,a.float2,a.float3,a.float4,a.float5,a.float8,a.float9,a.float10,a.float12,a.float13,a.float14,a.float15,a.float16,a.varchar1,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,case when a.date2 <= current_date-65 then 'none' else a.varchar13 end,case when a.date2 <= current_date-65 then 'none' else a.varchar14 end,case when a.date2 <= current_date-65 then 'none' else a.varchar15 end,a.varchar17,a.varchar18,a.varchar19,a.date3::date,a.float17,case when a.float17 is null then '0' when a.float17 = 0 then '0' when a.float17 <= 4 then '1 to 4 hrs' when a.float17 <= 8 then '5 to 8 hrs' when a.float17 <= 12 then '9 to 12 hrs' when a.float17 <= 18 then '13 to 18 hrs' when a.float17 <= 24 then '19 to 24 hrs' when a.float17 <= 48 then '1 to 2 days' when a.float17 > 48 then '>2 days' end,case when float18> a.float17 then 'Out of SLA' else 'In SLA'end,sum (a.float18),count (distinct a.float6),count (distinct a.varchar2),count (distinct a.varchar12),count (distinct a.varchar16),a.varchar20
from etl_temp.tmp_sfdc_case_b_OCPR a
where a.counter='ART3'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,42
;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,varchar20,date2,float1,float2,float3,float4,float5,float8,float9,float10,float15,varchar1,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar13,varchar14,varchar15,varchar17,varchar18,varchar19,float33,float6,float19,float20,float21,float22,float23,float24,float25,float26,float27,float28,float29,float30,float31,float32)
Select 'agg_survey',a.varchar5,a.date1::date,a.varchar20,a.date2::date,a.float1,a.float2,a.float3,a.float4,a.float5,a.float8,a.float9,a.float10,a.float15,a.varchar1,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,case when a.date2 <= current_date-65 then 'none' else a.varchar13 end,case when a.date2 <= current_date-65 then 'none' else a.varchar14 end,case when a.date2 <= current_date-65 then 'none' else a.varchar15 end,a.varchar17,a.varchar18,a.varchar19,float23,count (distinct a.float6),count (distinct a.varchar12),count (distinct a.varchar16),count (case when float21=5 then 1 else null end),count (case when float21=4 then 1 else null end),count (case when float21=3 then 1 else null end),count (case when float21=2 then 1 else null end),count (case when float21=1 then 1 else null end),count (case when float22=1 then 1 else null end),count (case when float22=0 then 1 else null end),count (case when float24=1 then 1 else null end),count (case when float24=0 then 1 else null end),count (case when float25>= 9 then 1 else null end),count (case when float25 in (8,7) then 1 else null end),count (case when float25<= 6 then 1 else null end)
from etl_temp.tmp_sfdc_case_b_OCPR a
where a.counter='survey_final'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30;

INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR(counter,date1,varchar1,varchar2,varchar3,varchar4,varchar5,varchar6,float10,float21,float22,float23,float24,float25,float26,float27,float28,float29,float30,float31,float32,float11,float6,float1,float2,float3,float4,float5,float7)
select 
'finish',
c.dateobj::date,
a.counter as metric,
case when a.float9 =1 then 'Billing'
when a.float9=2 then 'Zynga Diamond'
when a.float9=3 then 'Special'
when a.float9=5 then 'ERT'
else 'Standard' end as 'Queue Divisions',
Case when a.float14=1 then 'Response - First' else 'Response - Subsequent' end as response_type,
a.varchar18 as transaction_type,
a.varchar21 as 'SLA Status',
case when a.varchar5 ilike 'unassigned' then 'Unassigned' 
when a.varchar5 ilike '%poker%' then
case when a.varchar4 in ('IPad App','Android App','IPhone App','Blackberry App') then 'Zynga Poker Mobile' else 'Zynga Poker' End
else  a.varchar5 end as game,
a.float14 as response_count,
Sum(a.float21),
Sum(a.float22),
Sum(a.float23),
Sum(a.float24),
Sum(a.float25),
Sum(a.float26),
Sum(a.float27),
Sum(a.float28),
Sum(a.float29),
Sum(a.float30),
Sum(a.float31),
Sum(a.float32),
sum(a.float11) as tickets_count,
(SUM(a.float30)+SUM(a.float31)+SUM(a.float32)) as 'Survey Count',
case when a.varchar21 = 'In SLA' and a.counter = 'agg_art' and a.varchar18 = 'Outbound' THEN Sum(a.float11) end as 'In SLA',
case when a.counter = 'agg_tix' and a.varchar18 = 'New' then Sum(a.float11) end as 'Volume - New',
case when a.counter = 'agg_prod' and a.varchar18 = 'Outbound' then Sum(a.float11) end as 'Volume - Outbound',
case when a.float14 = 1 and a.counter = 'agg_prod' and a.varchar18 = 'Outbound' then Sum(a.float11) end as 'First Responses',
case when a.float14 <> 1 and a.counter = 'agg_prod' and a.varchar18 = 'Outbound' then Sum(a.float11) end as 'Subsequent Responses',
case when a.counter = 'agg_prod' and a.varchar18 = 'Updated' then Sum(a.float11) end as 'Volume - Updated'
from etl_temp.tmp_sfdc_case_b_OCPR a
inner join etl_temp.tmp_sfdc_join_OCPR b
on a.join1=b.join1 and a.date1::date=b.date1::date and b.counter='tier' and b.varchar2 is not null
inner join star.d_date c
on a.date2::date=c.dateobj

where a.counter ilike '%AGG%'
and num_year||quarter_name = '$quarter$'
group by 1,2,3,4,5,6,7,8,9
;


Select 
varchar2 as 'Queue Divisions',
Sum(float2) as 'Volume - New',
Sum(float7) as 'Volume - Updated',
Sum(float3) as 'Volume - Outbound',
Sum(float3)*4.36 as 'Volume - Outbound Cost',
SUM(float29)/(SUM(float28)+SUM(float29)) as 'CSAT',
(SUM(float30)-SUM(float32))/(SUM(float30)+SUM(float31)+SUM(float32)) as 'NPS',
(Sum(float25)*1+Sum(float24)*2+Sum(float23)*3+Sum(float22)*4+Sum(float21)*5) / (Sum(float25)+Sum(float24)+Sum(float23)+Sum(float22)+Sum(float21)) as 'Agent Rating',
Sum(float4) / Sum(float3) as 'FCR%',
Sum(float1) / Sum(float3) as 'In SLA%',
Sum(float6) as 'Survey Count',
Sum(float6) /Sum(float2)  as 'Survey Ratio'

from etl_temp.tmp_sfdc_case_b_OCPR
where counter = 'finish' and date1 = '$singleday$'
group by 1
Order by 2 DESC
limit 1000;

-- ]]></query>
-- <query name="GPX Metrics - Queue Divisions - Quarter" dsn="sample_vwh"><![CDATA[

Select 
varchar2 as 'Queue Divisions',
Sum(float2) as 'Volume - New',
Sum(float7) as 'Volume - Updated',
Sum(float3) as 'Volume - Outbound',
Sum(float3)*4.36 as 'Volume - Outbound Cost',
SUM(float29)/(SUM(float28)+SUM(float29)) as 'CSAT',
(SUM(float30)-SUM(float32))/(SUM(float30)+SUM(float31)+SUM(float32)) as 'NPS',
(Sum(float25)*1+Sum(float24)*2+Sum(float23)*3+Sum(float22)*4+Sum(float21)*5) / (Sum(float25)+Sum(float24)+Sum(float23)+Sum(float22)+Sum(float21)) as 'Agent Rating',
Sum(float4) / Sum(float3) as 'FCR%',
Sum(float1) / Sum(float3) as 'In SLA%',
Sum(float6) as 'Survey Count',
Sum(float6) /Sum(float2)  as 'Survey Ratio'

from etl_temp.tmp_sfdc_case_b_OCPR
where counter  = 'finish'
group by 1
Order by 2 DESC
limit 1000;

-- ]]></query>
-- <query name="GPX Metrics - Games - Day" dsn="sample_vwh"><![CDATA[

Select 
varchar6 as 'Game',
Sum(float2) as 'Volume - New',
Sum(float7) as 'Volume - Updated',
Sum(float3) as 'Volume - Outbound',
Sum(float3)*4.36 as 'Volume - Outbound Cost',
SUM(float28)/(SUM(float28)+SUM(float29)) as 'CSAT',
(SUM(float30)-SUM(float32))/(SUM(float30)+SUM(float31)+SUM(float32)) as 'NPS',
(Sum(float25)*1+Sum(float24)*2+Sum(float23)*3+Sum(float22)*4+Sum(float21)*5) / (Sum(float25)+Sum(float24)+Sum(float23)+Sum(float22)+Sum(float21)) as 'Agent Rating',
Sum(float4) / Sum(float3) as 'FCR%',
Sum(float1) / Sum(float3) as 'In SLA%',
Sum(float6) as 'Survey Count',
Sum(float6) /Sum(float2)  as 'Survey Ratio'

from etl_temp.tmp_sfdc_case_b_OCPR
where counter = 'finish' and date1 = '$singleday$'
group by 1
Order by 2 DESC
limit 1000;

-- ]]></query>
-- <query name="GPX Metrics - Games - Quarter" dsn="sample_vwh"><![CDATA[


Select 
varchar6 as 'Game',
Sum(float2) as 'Volume - New',
Sum(float7) as 'Volume - Updated',
Sum(float3) as 'Volume - Outbound',
Sum(float3)*4.36 as 'Volume - Outbound Cost',
SUM(float28)/(SUM(float28)+SUM(float29)) as 'CSAT',
(SUM(float30)-SUM(float32))/(SUM(float30)+SUM(float31)+SUM(float32)) as 'NPS',
(Sum(float25)*1+Sum(float24)*2+Sum(float23)*3+Sum(float22)*4+Sum(float21)*5) / (Sum(float25)+Sum(float24)+Sum(float23)+Sum(float22)+Sum(float21)) as 'Agent Rating',
Sum(float4) / Sum(float3) as 'FCR%',
Sum(float1) / Sum(float3) as 'In SLA%',
Sum(float6) as 'Survey Count',
Sum(float6) /Sum(float2)  as 'Survey Ratio'

from etl_temp.tmp_sfdc_case_b_OCPR
where counter = 'finish'
group by 1
Order by 2 DESC
limit 1000;



-- ]]></query>

--   <column colname="Game" format="string"/>
--   <column colname="CSAT" format="percent"/>
--   <column colname="NPS" format="percent"/>
--   <column colname="FCR%" format="percent"/>
--   <column colname="In SLA%" format="percent"/>
--   <column colname="Survey Ratio" format="percent"/>
--  <parameter display="singleday" name="singleday" type="date" default="Today -1 days"/>g
--<parameter type="selectQuery" display="quarter" name="quarter" displayColumn="quarter" valueColumn="quarter" default="2014Q3">
--<selectQuery name="quarter" dsn="sample_vwh">
select distinct num_year||quarter_name as quarter
from d_date
where dateobj between current_date-360 and current_date
order by 1 desc
--</selectQuery>
--</parameter>
--  
--</report>