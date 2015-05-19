set search_path = cs,etl_temp,star,star_secure,hist,staging,lookups,logging,ztrack; 
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
group by 1,2) a
where a.rank = 1
and systemmodstamp >= current_date-360
group by 1,2,3;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_join_OCPR (counter,join1,varchar1)
Select 'case',ID,MassTransactionId__c
from staging.s_sfdc_case
where MassTransactionId__c is not null
and systemmodstamp >= current_date-360
group by 1,2,3;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_join_OCPR (counter,join1,varchar1,varchar2,float1,date1)
Select 'tier',a.varchar2,a.varchar3,a.varchar4,a.value1,a.dateobj
from(Select a.counter,a.varchar2,a.varchar3,a.varchar4,a.value1,a.date1,b.dateobj,rank() over (partition by a.Varchar2,b.dateobj order by a.date1 desc) as rank
from report.a_team_datastore a
left join star.d_date b
on a.date1<= b.dateobj and b.dateobj between current_date-360 and current_date-1
where counter='tier') a
where a.rank=1
group by 1,2,3,4,5,6;
insert /*+direct*/ into etl_temp.tmp_sfdc_join_OCPR (counter,join1,varchar1,varchar2)
select 'cat',a.ID,a.NAME,case when instr(a.NAME,' -') = 0 then a.NAME else left(a.NAME,instr(a.NAME,' -')) end
from (select a.ID,a.NAME,a.systemmodstamp,rank()over(partition by a.ID order by a.systemmodstamp desc) as rank
from staging.s_sfdc_category a where a.Name <> 'Facebook - Purchase Dispute Auto-reply'
group by 1,2,3) a
where a.rank=1
group by 1,2,3,4;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,varchar1,varchar2,date1,varchar5,varchar3,varchar4,float5,float3,float4,float6,varchar6,varchar7,varchar8,varchar9,varchar10,varchar11,varchar12,float1,float2,float7,float8,float9,float10,float11,float12,varchar13,varchar14,varchar17,float13,varchar18)
Select 'tickets',CATEGORY__C,disposition__c,id,createddate,game,social_network,client,game_id,sn_id,client_id,user_uid,last_queue_owner__c,locale,site,game_origin,channel_sla,masstransactionId__c,contactid,chat_exclusion,queue_exclusion,payment_likelihood_score__c,os,queue_group,life,thirty,sixty,CATEGORY__C,CATEGORY_TYPE__C,by_paid_player__c,Business_unit,status
from report.uber_ticket
where createddate::date >= current_date -360
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
from cs.v_user a
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
where a.counter='tix_final' and b.CREATEDDATE::DATE >= current_date - 360
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,varchar1,varchar2,varchar3,varchar4,varchar5,date1)
select'stonecobra_1',case_id,case_id,email_id,workflow,site,createdbyid,sent_date
from report.uber_productivity
where sent_date::date >= current_date - 360
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
Select'agg_tix',a.varchar5,a.date1::date,a.date2::date,a.float1,a.float2,a.float3,a.float4,a.float5,a.float8,a.float9,a.float10,a.float15,a.varchar1,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,case when a.date2 <= current_date-150 then 'none' else a.varchar13 end,case when a.date2 <= current_date-200 then 'none' else a.varchar14 end,case when a.date2 <= current_date-200 then 'none' else a.varchar15 end,a.varchar17,a.varchar18,a.varchar19,a.varchar20,count (distinct a.float6),count (distinct a.varchar2),count (distinct a.varchar12)
from etl_temp.tmp_sfdc_case_b_OCPR a
where a.counter='tix_final'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,date2,float1,float2,float3,float4,float5,float8,float9,float10,float12,float13,float14,float15,varchar1,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar12,varchar13,varchar14,varchar15,varchar17,varchar18,varchar19,varchar20,float6,float11,float19,float20)
Select 'agg_prod',a.varchar5,a.date1::date,a.date2::date,a.float1,a.float2,a.float3,a.float4,a.float5,a.float8,a.float9,a.float10,a.float12,a.float13,a.float14,a.float15,a.varchar1,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,a.varchar12,case when a.date2 <= current_date-60 then 'none' else a.varchar13 end,case when a.date2 <= current_date-200 then 'none' else a.varchar14 end,case when a.date2 <= current_date-200 then 'none' else a.varchar15 end,a.varchar17,a.varchar18,a.varchar19,a.varchar20,count (distinct a.float6),count (distinct a.varchar2),count (distinct a.varchar12),count (distinct a.varchar16)
from etl_temp.tmp_sfdc_case_b_OCPR a
where counter= 'sc3'
and (case when a.float11 = 2 then 
(case when a.varchar9 = 'Email' then 
(case when a.float14 = 1 then 'Exclude' else 'Include' end)
else 'Include' end) 
else 'Include' end) = 'Include'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,date2,float1,float2,float3,float4,float5,float8,float9,float10,float12,float13,float14,float15,float16,varchar1,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar13,varchar14,varchar15,varchar17,varchar18,varchar19,date3,float17,varchar22,varchar21,float18,float6,float11,float19,float20,varchar20)
Select'agg_art',a.varchar5,a.date1::date,a.date2::date,a.float1,a.float2,a.float3,a.float4,a.float5,a.float8,a.float9,a.float10,a.float12,a.float13,a.float14,a.float15,a.float16,a.varchar1,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,case when a.date2 <= current_date-65 then 'none' else a.varchar13 end,case when a.date2 <= current_date-65 then 'none' else a.varchar14 end,case when a.date2 <= current_date-65 then 'none' else a.varchar15 end,a.varchar17,a.varchar18,a.varchar19,a.date3::date,a.float17,case when a.float17 is null then '0' when a.float17 = 0 then '0' when a.float17 <= 4 then '1 to 4 hrs' when a.float17 <= 8 then '5 to 8 hrs' when a.float17 <= 12 then '9 to 12 hrs' when a.float17 <= 18 then '13 to 18 hrs' when a.float17 <= 24 then '19 to 24 hrs' when a.float17 <= 48 then '1 to 2 days' when a.float17 > 48 then '>2 days' end,case when float18> a.float17 then 'Out of SLA' else 'In SLA'end,AVG (a.float18),count (distinct a.float6),count (distinct a.varchar2),count (distinct a.varchar12),count (distinct a.varchar16),a.varchar20
from etl_temp.tmp_sfdc_case_b_OCPR a
where a.counter='ART3'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,42
;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_case_b_OCPR (counter,join1,date1,varchar20,date2,float1,float2,float3,float4,float5,float8,float9,float10,float15,varchar1,varchar3,varchar4,varchar5,varchar6,varchar7,varchar8,varchar9,varchar10,varchar13,varchar14,varchar15,varchar17,varchar18,varchar19,float33,float6,float19,float20,float21,float22,float23,float24,float25,float26,float27,float28,float29,float30,float31,float32)
Select 'agg_survey',a.varchar5,a.date1::date,a.varchar20,a.date2::date,a.float1,a.float2,a.float3,a.float4,a.float5,a.float8,a.float9,a.float10,a.float15,a.varchar1,a.varchar3,a.varchar4,a.varchar5,a.varchar6,a.varchar7,a.varchar8,a.varchar9,a.varchar10,case when a.date2 <= current_date-65 then 'none' else a.varchar13 end,case when a.date2 <= current_date-65 then 'none' else a.varchar14 end,case when a.date2 <= current_date-65 then 'none' else a.varchar15 end,a.varchar17,a.varchar18,a.varchar19,float23,count (distinct a.float6),count (distinct a.varchar12),count (distinct a.varchar16),count (case when float21=5 then 1 else null end),count (case when float21=4 then 1 else null end),count (case when float21=3 then 1 else null end),count (case when float21=2 then 1 else null end),count (case when float21=1 then 1 else null end),count (case when float22=1 then 1 else null end),count (case when float22=0 then 1 else null end),count (case when float24=1 then 1 else null end),count (case when float24=0 then 1 else null end),count (case when float25>= 9 then 1 else null end),count (case when float25 in (8,7) then 1 else null end),count (case when float25<= 6 then 1 else null end)
from etl_temp.tmp_sfdc_case_b_OCPR a
where a.counter='survey_final'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30;

select a.counter as metric,
a.date2::date as date,
-- Case when datediff('d',a.date3::date,a.date2::date) < 0 then 'Anomaly'
--when datediff('d',a.date3::date,a.date2::date) between 0 and 7 then '0 to 7'
--when datediff('d',a.date3::date,a.date2::date) between 8 and 14 then '1 Week'
--when datediff('d',a.date3::date,a.date2::date) between 15 and 21 then '2 Weeks'
--when datediff('d',a.date3::date,a.date2::date) between 22 and 28 then '3 Weeks'
--when datediff('d',a.date3::date,a.date2::date) between 29 and 35 then '1 Month'
--when datediff('d',a.date3::date,a.date2::date) between 36 and 63 then '2 Months'
--when datediff('d',a.date3::date,a.date2::date) > 63 then '>2 Months' end as ticket_update_datediff,

--case when a.date1::date < '2012-07-25' then 'Pre-Segmentation'
-- when a.date1::date < '2012-09-10' then 'Segmentation 2.0'
-- when a.date1::date < '2012-09-27' then 'Segmentation 2.1'
-- else 'Social Support' end as release_schedule,
a.varchar1 as disposition,

--case
--when a.varchar17 = 'Forums' then 'Unpaid'
--when a.varchar17 = '0-4.99 Last 30' then 'Unpaid'
--when a.varchar17 = '0-4.99 Lifetime' then 'Unpaid'
--when a.varchar17 = '0-4.99 Lifetime_Legacy' then 'Unpaid'
--when a.varchar17 = '5-19 Last 30' then 'Paid'
--when a.varchar17 = '5-99 Last 60' then 'Paid'
--when a.varchar17 = '5-1499 Lifetime_Legacy' then 'Paid'
--when a.varchar17 = '5-1499 Lifetime' then 'Paid'
--when a.varchar17 = '20-49 Last 30' then 'Paid'
--when a.varchar17 = '20-99 Last 30' then 'Paid'
--when a.varchar17 = '50-249 Last 60' then 'Paid'
--when a.varchar17 = '100-499 Last 60' then 'Paid'
--when a.varchar17 = '100-1499 Lifetime' then 'Paid'
--when a.varchar17 = '250-1499 Lifetime' then 'Paid'
--when a.varchar17 = '250-999 Lifetime' then 'Paid'
--when a.varchar17 = '500-1499 Lifetime' then 'Paid'
--when a.varchar17 = '250-999 lifetime' then 'Paid'
--when a.varchar17 = '1000-9999 Lifetime' then 'Paid'
--when a.varchar17 = '1500-4999 Lifetime' then 'Paid'
--when a.varchar17 = '1500-9999 Lifetime_Legacy' then 'Paid' 
--when a.varchar17 = '1500-9999 Lifetime' then 'Paid' 
--when a.varchar17 = '5000-9999 Lifetime' then 'Paid'
--when a.varchar17 = '10000+ Lifetime' then 'Paid'
--when a.varchar17 = '10000+ Lifetime_Legacy' then 'Paid'
--when a.varchar17 = 'Special' then 'Paid'
--when a.varchar17 = 'VIP' then 'Paid'
--when a.varchar17 = 'ZIP' then 'Paid'
--else 'Unpaid' end as payer_summary,

case when a.varchar5 ilike 'unassigned' then 'Unassigned' 
when a.varchar5 in ('Zynga Poker Mobile','Zynga Poker')  then
case when a.varchar4 in ('IPad App','Android App','IPhone App','Blackberry App') then 'Zynga Poker Mobile' else 'Zynga Poker' End
else  a.varchar5 end as game,
a.varchar3 as social_network,
a.varchar4 as client,
case when a.varchar4 in ('IPad App','Android App','IPhone App','Blackberry App') then 'Mobile' Else 'Web' end as 'Platform',
a.varchar13 as ZRM_Category_Type,
a.varchar14 as ZRM_Category,
  split_part(a.varchar14,' - ',1) as 'Category Level 1'
  ,case when length(a.varchar14)-length(translate(a.varchar14,' - ','')) >= 3 then split_part(a.varchar14,' - ',2)
        when length(a.varchar14)-length(translate(a.varchar14,' - ','')) =  1 then split_part(a.varchar14,' - ',2)
        when length(a.varchar14)-length(translate(a.varchar14,' - ','')) <  1 then 'none'
        when regexp_like(split_part(a.varchar14,' - ',length(a.varchar14)-length(translate(a.varchar14,' - ',''))+1),'^[0-9]+$')
        then 'none'
   else 'none' end as 'Category Level 2'
  ,case when length(a.varchar14)-length(translate(a.varchar14,' - ','')) >= 4 then split_part(a.varchar14,' - ',3)
        when length(a.varchar14)-length(translate(a.varchar14,' - ','')) <= 1 then 'none'
        when regexp_like(split_part(a.varchar14,' - ',length(a.varchar14)-length(translate(a.varchar14,' - ',''))+1),'^[0-9]+$')
        then 'none'
   else 'none'
end as 'Category Level 3',
case when regexp_instr(a.varchar14,'[A-Z]+-[0-9]+$') > 0 then substr(a.varchar14,regexp_instr(a.varchar14,'[A-Z]+-[0-9]+$')) end as 'Jira',
a.varchar6 as 'Default Queue',

case 
when a.varchar10 ilike '%email%' then 'Email'
when a.varchar10 ilike '%phone%' then 'Phone'
when a.varchar10 ilike '%Chat%' then 'Chat' end as service_channel,

case
WHEN a.float8 = 1 then 'IOS'
WHEN a.float8 = 2 then 'Android'
WHEN a.float8 = 3 then 'Windows'
WHEN a.float8 = 4 then 'Mac'
WHEN a.float8 = 6 then 'Blackberry'
WHEN a.float8 = 5 then 'Linux'
WHEN a.float8 = 4 then 'Mac'
WHEN a.float8 = 7 then 'Unknown'
ELSE
'Unknown'
END as OS,
case when a.float9 =1 then 'Billing'
when a.float9=2 then 'Zynga Diamond'
when a.float9=3 then 'Special'
when a.float9=5 then 'ERT'
else 'Standard' end as 'Queue Divisions',
a.varchar7 as locale,
a.varchar8 as site,
Case When a.varchar8 ilike '%telus%' then 'Telus'
When a.varchar8 ilike '%Zynga%' then 'Zynga'
When a.varchar8 ilike '%unassigned%' then 'Queue'
Else a.varchar8 end as site_grouped,
a.varchar9 as game_origin,
a.varchar21 as ART_bracket,
case when a.varchar17 is null then '0-4.99 Last 30'
Else a.varchar17 end as segmentation,

case when a.varchar10 ilike '%chat%' then
 case when a.varchar10 ilike '%hour%' then 'CHAT' else a.varchar10 end
 when a.varchar10 ilike '%phone%' then
 case when a.varchar10 ilike '%hour%' then 'PHONE' else a.varchar10 end 
 else a.varchar10 end as channel_SLA,

case
when a.varchar17 = 'Forums' then 'Forums'
when a.varchar17 = '0-4.99 Last 30' then 'Unpaid'
when a.varchar17 = '0-4.99 Lifetime' then 'Unpaid'
when a.varchar17 = '0-4.99 Lifetime_Legacy' then 'Unpaid'
when a.varchar17 = '5-19 Last 30' then 'Paid'
when a.varchar17 = '5-99 Last 60' then 'Paid'
when a.varchar17 = '5-1499 Lifetime_Legacy' then 'Paid'
when a.varchar17 = '5-1499 Lifetime' then 'Paid'
when a.varchar17 = '20-49 Last 30' then 'Paid'
when a.varchar17 = '20-99 Last 30' then 'Paid'
when a.varchar17 = '50-249 Last 60' then 'Paid'
when a.varchar17 = '100-499 Last 60' then 'Paid'
when a.varchar17 = '100-1499 Lifetime' then 'Paid'
when a.varchar17 = '250-1499 Lifetime' then 'Paid'
when a.varchar17 = '500-1499 Lifetime' then 'Paid'
when a.varchar17 = '250-999 lifetime' then 'Paid'
when a.varchar17 = '1000-9999 Lifetime' then 'Paid'
when a.varchar17 = '1500-4999 Lifetime' then 'Platinum'
when a.varchar17 = '1500-9999 Lifetime_Legacy' then 'Platinum' 
when a.varchar17 = '5000-9999 Lifetime' then 'Platinum'
when a.varchar17 = '1500-9999 Lifetime' then 'Platinum' 
when a.varchar17 = '10000+ Lifetime' then 'Diamond'
when a.varchar17 = '10000+ Lifetime_Legacy' then 'Diamond'
when a.varchar17 = 'Special' then 'Special'
when a.varchar17 = 'VIP' then 'VIP'
when a.varchar17 = 'ZIP' then 'ZIP'
else 'Unpaid' end as ticket_paid_status,

a.float14 as response_count,
Case when a.float14=1 then 'Response - First' else 'Response - Subsequent' end as response_type,
--a.float13 as entry,
a.varchar18 as transaction_type,
case when varchar18 = 'New' then 1
when varchar18 = 'Updated' then 2
when varchar18 = 'Outbound' then 4
when varchar18 = 'Survey' then 5
else 1 end as transaction_ID,
--Case when a.float15 =1 then 'Include'
--When a.float15 =0 then 'Exclude'
--else 'Include' end as mass_edit_exclusion,
case when a.float12 = 1 then 'Standard Reply' when a.float12 =0 then 'Mass Reply' else 'Standard Reply' end as mass_reply,
a.float12,
--Case when a.float1 =1 then 'Include'
--When a.float1 =0 then 'Exclude'
--else 'Include' end as chat_exclusion,
Case when a.float2 =1 then 'Include'
When a.float2 =0 then 'Exclude'
else 'Include' end as queues_exclusion,
a.float33 as NumContactsToResolve,
--b.float1::varchar as 'Tiers',
--b.varchar1 as 'game_type',
case when a.varchar19 in ('Zynga Poker Mobile','Zynga Poker') then
case when a.varchar4 in ('IPad App','Android App','IPhone App','Blackberry App') then '2025' else '2025' End
else a.varchar5 end as 'Business Unit',
case when a.varchar19 ilike 'unassigned' then 'Unassigned' 
when a.varchar19 ilike '%poker%' then
case when a.varchar4 in ('IPad App','Android App','IPhone App','Blackberry App') then 'Zynga Poker Mobile' else 'Zynga Poker' End
else  a.varchar19 end as 'Reservation game',
--a.float17 as 'SLA Time',
a.varchar21 as 'SlA Status',
a.varchar20 as 'Status',
--datediff('day', a.date2::date, current_date-1) as 'datediff',
sum(a.float11) as tickets_count,
Avg(a.float18) as Average_response_time,
sum(a.float32) as detractors,
sum(a.float31) as passives,
sum(a.float30) as promoters,
sum(a.float26) as resolved,
sum(a.float27) as unresolved,
sum(a.float29) as unsatisfied,
sum(a.float28) as satisfied,
sum(a.float25) as AR_1,
sum(a.float24) as AR_2,
sum(a.float23) as AR_3,
sum(a.float22) as AR_4,
sum(a.float21) as AR_5,
(SUM(a.float30)+SUM(a.float31)+SUM(a.float32)) as 'Survey Count',
case when a.varchar21 = 'In SLA' and a.counter = 'agg_art' and a.varchar18 = 'Outbound' THEN Sum(a.float11) end as 'In SLA',
case when a.counter = 'agg_tix' and a.varchar18 = 'New' then Sum(a.float11) end as 'Volume - New',
case when a.counter = 'agg_tix' and a.varchar18 = 'New' and datediff('day', a.date2::date, current_date-1)<7 then Sum(a.float11) end as '7 day sum - Volume - New',
case when a.counter = 'agg_tix' and a.varchar18 = 'New' and datediff('day', a.date2::date, current_date-1)BETWEEN 7 and 13 then Sum(a.float11) end as 'Previous 7 day sum - Volume - New',
case when a.counter = 'agg_prod' and a.varchar18 = 'Outbound' then Sum(a.float11) end as 'Volume - Outbound',
case when a.counter = 'agg_prod' and a.float12 <> 0 and a.varchar18 = 'Outbound' then Sum(a.float11) end as 'Volume - Standard Replies',
case when a.counter = 'agg_prod' and a.float12 = 0 and a.varchar18 = 'Outbound' then Sum(a.float11) end as 'Volume - Mass Replies',
case when a.float14 = 1 and a.counter = 'agg_prod' and a.varchar18 = 'Outbound' then Sum(a.float11) end as 'First Responses',
case when a.float14 <> 1 and a.counter = 'agg_prod' and a.varchar18 = 'Outbound' then Sum(a.float11) end as 'Subsequent Responses',
case when a.counter = 'agg_prod' and a.varchar18 = 'Updated' then Sum(a.float11) end as 'Volume - Updated',
case when a.counter in ('agg_prod','agg_tix') and a.varchar18 in( 'Updated', 'New') then Sum(a.float11) end as 'Volume - Inbound',
case when a.counter in ('agg_prod','agg_tix') and a.varchar18 in( 'Updated', 'New','Outbound') then Sum(a.float11) end as 'Volume - Inbound/Outbound'
--,sum(a.float6) as UID_count,
--sum(a.float19) as contact_count,
--sum(a.float20) as agent_count
from etl_temp.tmp_sfdc_case_b_OCPR a
where a.counter ilike '%agg%'
and a.date2::date >= current_date-360
and a.date2::date < current_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37

--,38,39,40,41,42,43,44,45,46