Select 
logdate,
publicationdate,
level1,
article_word_count,
article_id,
articles,
hour_int,
case 
when s_user_type is null and servicegroup_title is not null then 'Eligible Subscriber'
when s_user_type is null and servicegroup_title is null then 'Anonymous'
else s_user_type end as s_user_type,
Sum(all_pageviews) as 'All Pageviews'

from
(
Select 
a.logdate::date as logdate,
b.publicationdate::date as publicationdate,
case when b.level1 is null then a.site_section else b.level1 end as level1,
b.article_word_count::int as article_word_count,
case when b.articleid is null then '1.'||a.article_id else '1.'||b.articleid end as 'Article_id',
case when b.headline is null then Replace(Replace(Replace(Replace(Replace(Replace(trim(Replace(RTRIM(RIGHT(cs_uri_stem,(char_length(cs_uri_stem) - INSTR(cs_uri_stem,'/', 1, REGEXP_COUNT(cs_uri_stem,'/')))),'1234567890.'),'-',' ')),'%C3%A9', 'e'),'%C3%A1','a'),'%C3%B3','o'),'%C3%A','i'),'%C3%BA','u'),'%C3%B6','oe') else b.headline end as 'Articles',
b.hour_int as hour_int,
c.user_type as s_user_type,
d.accounttype,
d.servicegroup_title,
Count(a.unique_id) as all_pageviews


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
case 
when subscriptionstatus = 'Active Subscription' and entitlement_name = 'Digital Edition' then 'Eligible Subscriber'
when subscriptionstatus = 'Active Subscription' and entitlement_name is null then 'Ineligible Subscriber'
when subscriptionstatus <> 'Active Subscription' then 'Inactive Subscriber'
else 'Unknown'  end as user_type

from
(
SELECT logdate, 
subscriptionid, 
entitlement_name,
subscriptioncreated, 
subscriptionstatus, 
accountid,
rank() over (partition by logdate,accountid order by logdate,subscriptioncreated desc) as rank
FROM datalayer.subscriptions a
left join -- JOIN FOR ENTITLEMENTS TO SEE IF ELIGIBLE FOR DIGITAL SUBSCRIPTION
(SELECT serviceid as service_id,entitlement_name 
FROM datalayer.tbl_entitlements
where entitlement_name = 'Digital Edition'
group by 1,2
) b
on a.serviceid=b.service_id
where logdate::date = current_date - 2
) a

where rank = 1) c
on a.mpp_account_id = c.accountid and a.logdate::date=c.logdate::date -- JOIN ON MPP & LOGDATE TO SUBSCRIBER TABLE

LEFT JOIN 

datalayer.tbl_engagement_tracker d -- JOIN ON MPP & LOGDATE AND ENTITLEMENT FOR ENGAGEMENT TABLE FOR CORPS AND STAFF
ON a.mpp_account_id = d.accountid AND a.logdate::DATE=d.logdate::DATE AND entitlement_name = 'Digital Edition'

WHERE a.logdate::Date = current_date - 2 and 
--left(a.logtime,2) = 13 and
a.page_type = 'Article' 
group by 1,2,3,4,5,6,7,8,9,10
) a

group by 1,2,3,4,5,6,7,8

;