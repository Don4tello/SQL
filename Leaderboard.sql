commit;
set search_path = etl_temp,report,star,star_secure,hist,staging,lookups,logging,ztrack; 
set session characteristics as transaction isolation level read committed;
INSERT /*+ direct */ into tmp_table_ocpr (game_id,metric2,date,value,value2,metric)
select 
a.game_id,
case when 1=1 then d.game_name
            when d.game_name like '%Words with Friends%' then 'Words with Friends'
            when d.game_name like '%Words With Friends%' then 'Words with Friends'
            when d.game_name like '%Running with Friends%' then 'Running with Friends'
            when d.game_name like '%Hanging with Friends%' then 'Hanging with Friends'
            when d.game_name like '%Scramble%' then 'Scramble with Friends'
            when d.game_name like '%Matching With Friends%' then 'Matching With Friends'
            when d.game_name like '%Chess with Friends%' then 'Chess with Friends'
            when d.game_name like '%Gems with Friends%' then 'Gems with Friends'
            when d.game_name like '%Whats the Phrase%' then 'Whats the Phrase' 
            when d.game_name like '%Running%' then 'Running with Friends' 
       else d.game_name end as game,
a.stat_date,
count(distinct(case when internal_id is null then a.user_uid
    else internal_id end)) as DAU,
Sum(a.total_payment_amount) as rev
,'daurev'
from a_user_day a
join a_user b on b.sn_id=a.sn_id and b.game_id=a.game_id and b.client_id=a.client_id and b.user_uid=a.user_uid
and a.stat_date>=current_date-180
join l_client c on a.client_id=c.client_id
left join l_game d on a.game_id=d.game_id
group by 1,2,3
;


insert /*+direct*/ into tmp_table_ocpr (game_id,user_uid,date,metric2,metric3, metric)
select game_id
  ,case when length(translate(social_network_uid__c,'1234567890',''))>0 then null
        when length(social_network_uid__c) between 2 and 18 then social_network_uid__c::int
   else null end::int as user_uid
  ,createddate
  ,id as ticket_id
  ,code
  ,'ticket'
from (
    select a.id,game__c
      ,createddate
      ,social_network_uid__c
      ,category__c as code
      ,b.game_id
      ,row_number() over (partition by a.id order by systemmodstamp desc) as rownum
    from s_sfdc_case a
    left join a_team_datastore b on b.counter='game_name'
      and b.varchar1=a.game__c
    where category_type__c='Game Play'
    and a.last_queue_owner__c not in ('(SAD)','Bounces','CSIT','DxDiag','Email Demo','Awaiting Player Reply','Fake Queue','Gerardo Enrique Romero Rivera'
      ,'Kim Florence','Legacy No Answer','No Game Queue','ZZ - CS Product Only','Game Closure'
    )
) x
where rownum=1
and createddate::date>=current_date-180

;

insert /*+direct*/ into tmp_table_ocpr (metric3, metric, date2,date,metric2,value)
Select
metric3 as category_code,
'Jira',
max(date::date) as last_reported,
min(date::date) as first_reported,
case when max(date::date) >= current_date - 5 then 'Unresolved' else 'Resolved' end as 'Jira Status',
datediff('day',min(date::date),max(date::Date)) as 'Resolution Days'
from 
tmp_table_ocpr
where
metric = 'ticket'
group by 1,2
;

INSERT /*+ direct */ into tmp_table_ocpr(metric2,date,user_uid,value,value2,metric)
select metric2 as ticket,a.date,a.user_uid
  ,sum(case when date_trans::date<a.date::date then amount else 0 end) as pay_prior
  ,sum(case when date_trans::date>a.date::date then amount else 0 end) as pay_post
  ,'rev'
--; select *
from tmp_table_ocpr a
join v_payment b on b.game_id=a.game_id and b.user_uid=a.user_uid
  and date_trans::date between date::date-30 and date::date+30
where a.metric='ticket'
and date_trans::date>=current_date-180-30
--order by a.user_uid limit 9999;
group by 1,2,3
;

INSERT /*+ direct */ into tmp_table_ocpr(metric2,date,user_uid,value,value2,metric)
select metric2 as ticket,a.date,a.user_uid
  ,count(distinct case when first_timestamp::date<a.date::date then first_timestamp::date end) as dau_prior
  ,count(distinct case when first_timestamp::date>a.date::date then first_timestamp::date end) as dau_post
  ,'dau'
--; select *
from tmp_table_ocpr a
join s_zt_dau b on b.game_id=a.game_id and b.user_uid=a.user_uid
  and first_timestamp::date between date::date-30 and date::date+30
where a.metric='ticket'
and first_timestamp::date>=current_date-180-30
--order by a.user_uid limit 9999;
group by 1,2,3
;

INSERT /*+ direct */ into tmp_table_ocpr(metric2,date,user_uid,value,value2,value3,sn_id,metric)
select metric2 as ticket,a.date,a.user_uid
  ,Sum(distinct case when first_timestamp::date  between a.date and a.date+7 then 1 else 0 end) as '1week_retention'
  ,Sum(distinct case when first_timestamp::date  between a.date+8 and a.date+15 then 1 else 0 end) as '2week_retention'
  ,Sum(distinct case when first_timestamp::date  between a.date+16 and a.date+23 then 1 else 0 end) as '3week_retention'
  ,Sum(distinct case when first_timestamp::date  between a.date+24 and a.date+31 then 1 else 0 end) as '_retention'
  ,'retention'
--; select *
from tmp_table_ocpr a
join s_zt_dau b on b.game_id=a.game_id and b.user_uid=a.user_uid
  and first_timestamp::date between date::date-30 and date::date+30
where a.metric='ticket'
and first_timestamp::date>=current_date-180-30
--order by a.user_uid limit 9999;
group by 1,2,3
;



insert /*+direct*/ into tmp_table_ocpr (metric3,value,value2,value3,metric)
select category_id
--  ,count(1)
--  ,count(distinct user_uid)
  ,count(distinct isnull(user_uid::varchar,case_id)) as players
  ,sum(  --normalized post-ticket
    case when date::date<current_date-30 then dau_post-dau_pre
    else dau_post * 30 / datediff('day',date,current_date-1)-dau_pre
    end
  ) as sum_engagment_delta
  ,sum(  --normalized post-ticket
    case when date::date<current_date-30 then pay_post-pay_pre
    else pay_post * 30 / datediff('day',date,current_date-1)-pay_pre
    end
  ) as sum_payment_delta
  ,'category_delta'
from (
    select game_id,a.user_uid,date,a.metric2 as case_id,a.metric3 as category_id
      ,pay_pre, pay_post
      ,dau_pre, dau_post
    --  ,n.name
    from tmp_table_ocpr a
    left join (
        select user_uid,metric2
          ,sum(case when metric='rev' then value  else 0 end) as pay_pre
          ,sum(case when metric='rev' then value2 else 0 end) as pay_post
          ,sum(case when metric='dau' then value  else 0 end) as dau_pre
          ,sum(case when metric='dau' then value2 else 0 end) as dau_post
        from tmp_table_ocpr
        where metric in ('rev','dau')
        group by 1,2
    ) b on b.user_uid=a.user_uid and b.metric2=a.metric2
    where a.metric='ticket'
) x
where date::date<=current_date-3
group by 1
;

select 
   p.game_name
  ,p.category
  ,p.date
  ,p.players
  ,p.w1
  ,p.w2
  ,p.w3
  ,p.w4
  ,p.jira_status
  ,case when p.est_dau_delta_per_player is null then 0 else p.est_dau_delta_per_player end
  ,case when p.est_pay_delta_per_player is null then 0 else p.est_pay_delta_per_player end
  ,dau
  ,rev
  ,trim(case when length(category)-length(translate(category,'-','')) = 0 then 'none'
        when regexp_like(split_part(category,'-',length(category)-length(translate(category,'-',''))+1),'^[0-9]+$')
        then split_part(category,'-',length(category)-length(translate(category,'-',''))) ||'-'|| split_part(category,'-',length(category)-length(translate(category,'-',''))+1)
   else 'none'
   end) as 'Jira'
from (
    select 
        game_name,
        name as category,
        date::date,
      jira_status
      ,count(distinct isnull(user_uid::varchar,case_id)) as players
      ,avg(engagement_delta)*1./avg(players) as est_dau_delta_per_player
      ,avg(payment_delta)*1./avg(players) as est_pay_delta_per_player
      ,avg(w1) as w1
      ,avg(w2) as w2
      ,avg(w3) as w3
      ,avg(w4) as w4
    from (
        select a.game_id,a.user_uid,a.date,a.metric2 as case_id,a.metric3 as category_id,b.value as w1,b.value2 as w2,b.value3 as w3,b.sn_id as w4,c.metric2 as jira_status
        from tmp_table_ocpr a
        inner join tmp_table_ocpr b on a.metric2=b.metric2 and a.user_uid=b.user_uid and b.metric='retention'
        inner join tmp_table_ocpr c on a.metric3=c.metric3 and c.metric='Jira'
        where a.metric='ticket'
    ) x
    left join (
      select metric3 as category_id,value as players,value2 as engagement_delta,value3 as payment_delta
      from tmp_table_ocpr
      where metric='category_delta'
    ) y on y.category_id=x.category_id
    left join (
        select 'categories',a.ID, a.NAME
        from (
            select a.ID,a.NAME,a.systemmodstamp,rank()over(partition by a.ID order by a.systemmodstamp desc) as rank
            from staging.s_sfdc_category a
            group by 1,2,3
        ) a
        where a.rank=1 and a.name <> 'Other - MET Closure Duplicate Incident'
        group by 1,2,3
    ) n on n.ID=x.category_id
    left join l_game g on g.game_id=x.game_id
    group by 1,2,3,4
) p
left join (
    select metric2 as game_name,date::date,value as dau,value2 as rev
    from tmp_table_ocpr
    where metric='daurev'
) q on q.game_name=p.game_name and q.date=p.date
order by 1,2,3