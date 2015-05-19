INSERT /*+ direct */ into etl_temp.tmp_skb_OCPR (Metric5,date,sn_id,game_id,Client_id,Value)
Select 'DAU2',stat_date,sn_id,game_id,client_id,total_duu
from star_secure.a_game_day
where stat_date>= current_date-1800
and sn_id != -2 and client_id!= -2
;
INSERT /*+ direct */ into etl_temp.tmp_sfdc_join_OCPR (counter,join1,varchar1,varchar2,float1,date1)
Select 'tier',a.varchar2,a.varchar3,a.varchar4,a.value1,a.dateobj
from(Select a.counter,a.varchar2,a.varchar3,a.varchar4,a.value1,a.date1,b.dateobj,rank() over (partition by a.Varchar2,b.dateobj order by a.date1 desc) as rank
from report.a_team_datastore a
left join star.d_date b
on a.date1<= b.dateobj and b.dateobj between '2010-01-01' and current_date-1
where counter='tier') a
where a.rank=1
group by 1,2,3,4,5,6;

INSERT /*+ direct */ into etl_temp.tmp_skb_OCPR (Metric5,date,sn_id,game_id,Client_id,Value)
Select 'DAU2',stat_date,sn_id,-18,client_id,sum(total_duu)
from star_secure.a_game_day
where stat_date>= current_date-1800
and sn_id != -2 and client_id= 6
group by 1,2,3,4,5
;
INSERT /*+ direct */ into etl_temp.tmp_skb_OCPR(Metric5,metric2,game_id)
Select 'game_name',a.Varchar2,a.Game_id
from(select Varchar1,Varchar2,Game_id, date3,Rank() over (partition by Varchar2 order by date3 desc)as rank
from report.a_team_datastore
where counter='game_name')a
where a.rank=1
group by 1,2,3;

INSERT /*+ direct */ into etl_temp.tmp_skb_OCPR(Metric5,metric2,sn_id)
Select 'Social_network',a.Varchar2,a.sn_id
from(select Varchar1,Varchar2,sn_id, date3,Rank() over (partition by Varchar2 order by date3 desc)as rank
from report.a_team_datastore
where counter='Social_network')a
where a.rank=1
group by 1,2,3;

INSERT /*+ direct */ into etl_temp.tmp_skb_OCPR(Metric5,date,metric2,metric3,metric4,value)
Select
'DAU' as metric,
a.date::date as date,
case when e.metric2 is null then 'Facebook'
     else e.metric2 end as social_network,
case when a.client_id = 1 then 'Facebook.com'
     when a.client_id = 2 then 'iPhone'
     when a.client_id = 8 then 'iPad'
     when a.client_id = 3 then 'Android'
     when a.client_id = 16 then 'Google+'
     when a.client_id = 6 then 'Zynga.com'
     else 'Facebook.com' end as client,
case when d.metric2 is null then 'unassigned' else d.metric2 end as game,
sum(a.value) as DAU
from etl_temp.tmp_skb_OCPR a
left join etl_temp.tmp_skb_OCPR d
on a.game_id=d.game_id and d.Metric5='game_name'
left join etl_temp.tmp_skb_OCPR e
on a.sn_id = e.sn_id and e.Metric5='Social_network'
where a.metric5='DAU2'


group by 1,2,3,4,5
order by 1 desc
;

Select 
a.Metric5 as metric,
a.date as date,
a.metric2 as social_network,
a.metric3 as client,
case when a.metric3 in ('IPad App','Android App','IPhone App','Blackberry App') then 'Mobile' Else 'Web' end as 'Platform',
case when a.metric4 ilike 'unassigned' then 'unassigned' 
when a.metric4 ilike '%poker%' then
case when a.metric3 in ('IPad App','Android App','IPhone App','Blackberry App') then 'Zynga Poker Mobile' else 'Zynga Poker' End
else  a.metric4 end as game,
b.float1::varchar as 'Tiers',
b.varchar1 as 'game_type',
Case when b.varchar2::int is null then -1 
when a.metric4 ilike '%poker%' then
case when a.metric3 in ('IPad App','Android App','IPhone App','Blackberry App') then 2064 else 2025 End
else b.varchar2::int end as 'Business Unit',
sum(a.value) as DAU
from etl_temp.tmp_skb_OCPR a
inner join etl_temp.tmp_sfdc_join_OCPR b
on a.metric4=b.join1 and a.date::date=b.date1::date and b.counter='tier' and b.varchar2 is not null
where a.metric5='DAU'
group by 1,2,3,4,5,6,7,8,9