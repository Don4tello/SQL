--Select 
--*
--from
--(
Select 
logdate,
--logtime,
publicationdate,
level1,
article_word_count,
article_id,
articles,
hour_int,
case 
when s_user_type = 'Anonymous' and servicegroup_title is not null then 'Active Subscriber' 
else s_user_type end as s_user_type,
r_user_type,
case 
when group_name is null and s_user_type = 'Anonymous' and accounttype is not null then accounttype
when group_name is null and s_user_type = 'Active Subscriber' then 'Unknown Subscribed'
when group_name is null and r_user_type <> 'Active Subscriber' then 'Unknown' else group_name end as group_name,
Sum(all_pageviews) as 'All Pageviews'
--,mpp_account_id
from
(
Select 
a.logdate::date as logdate,
--a.logtime,
b.publicationdate::date as publicationdate,
case when b.level1 is null then a.site_section else b.level1 end as level1,
b.article_word_count::int as article_word_count,
case when b.articleid is null then '1.'||a.article_id else '1.'||b.articleid end as 'Article_id',
case when b.headline is null then Replace(Replace(Replace(Replace(Replace(Replace(trim(Replace(RTRIM(RIGHT(cs_uri_stem,(char_length(cs_uri_stem) - INSTR(cs_uri_stem,'/', 1, REGEXP_COUNT(cs_uri_stem,'/')))),'1234567890.'),'-',' ')),'%C3%A9', 'e'),'%C3%A1','a'),'%C3%B3','o'),'%C3%A','i'),'%C3%BA','u'),'%C3%B6','oe') else b.headline end as 'Articles',
b.hour_int as hour_int,
case when a.user_type is null then 'Anonymous' else a.user_type end as r_user_type,
case when c.user_type is null then 'Anonymous'else c.user_type end as s_user_type,
c.group_name,
d.accounttype,
d.servicegroup_title,
Count(a.unique_id) as all_pageviews
--,mpp_account_id


From localuse.vw_weblogdetails_2015_uber_live a
left join -- POLOPOLY JOIN
(Select b.hour_int as hour_int,
b.headline as headline,
b.articleid as articleid,
b.publicationdate as publicationdate,
b.level1 as level1,
b.article_word_count as article_word_count
from
datalayer.tbl_articlepolopolydata b
where content_type in ('Gallery',
'Article with Video',
'Standard')  
--and publicationdate::date BETWEEN current_date - 7 and current_date - 1
) b
on a.article_id=b.articleid
left join -- SUBSCRIPTIONS JOIN TO SEE IF ACTIVE OR NOT AND WHAT GROUP THEY'RE IN
(Select 
accountid, 
logdate::date,
group_name as group_name,
case when subscriptionstatus = 'Active Subscription' then 'Active Subscriber'
else 'Inactive Subscriber'  end as user_type

from
(
SELECT logdate, 
subscriptionid, 
group_name,
subscriptioncreated, 
subscriptionstatus, 
accountid,
rank() over (partition by logdate,accountid order by logdate,subscriptioncreated desc) as rank
FROM datalayer.subscriptions a
left join -- JOIN FOR GROUP NAME EXCLUDING ALL and S+P for distinct grouping
(Select service_id_desc as group_name, service_id from datalayer.tbl_mst_serviceid_group group by 1,2) b
on a.serviceid=b.service_id
where logdate::date = current_date - 1
) a

where rank = 1) c
on a.mpp_account_id = c.accountid and a.logdate::date=c.logdate::date -- JOIN ON MPP & LOGDATE TO SUBSCRIBER TABLE

LEFT JOIN 

datalayer.tbl_engagement_tracker d -- JOIN ON MPP & LOGDATE AND ENTITLEMENT FOR ENGAGEMENT TABLE FOR CORPS AND STAFF
ON a.mpp_account_id = d.accountid AND a.logdate::DATE=d.logdate::DATE AND entitlement_name = 'Digital Edition'

WHERE a.logdate::Date = current_date - 1 and 
a.page_type = 'Article' 
group by 1,2,3,4,5,6,7,8,9,10,11,12
) a

group by 1,2,3
,4,5,6,7,8,9,10
--) a
--where group_name = 'STAFF'

;