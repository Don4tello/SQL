insert /*+direct*/ into localuse.tmp_generic_ocpr (date1,char1,char2,char3)
Select 
logdate,
case 
when s_user_type is null and servicegroup_title is not null then 'Eligible Subscriber'
when s_user_type is null and servicegroup_title is null and a.mpp_account_id is not null then 'Commentator'
when s_user_type is null and servicegroup_title is null and a.mpp_account_id is null then 'Anonymous'
else s_user_type end as s_user_type,
unique_id,
a.mpp_account_id

from
(
Select 
a.logdate::date as logdate,
c.user_type as s_user_type,
d.accounttype,
d.servicegroup_title,
a.unique_id,
a.mpp_account_id


From localuse.vw_weblogdetails_2015_uber_live a

left join -- SUBSCRIPTIONS JOIN TO SEE IF ACTIVE OR NOT AND WHAT GROUP THEY'RE IN
(Select 
accountid, 
logdate::date,
case 
when subscriptionstatus = 'Active Subscription' and entitlement_name = 'Web' then 'Eligible Subscriber'
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
where entitlement_name = 'Web'
group by 1,2
) b
on a.serviceid=b.service_id
where logdate::date BETWEEN current_date - 30 and current_date - 1
) a

where rank = 1) c
on a.mpp_account_id = c.accountid and a.logdate::date=c.logdate::date -- JOIN ON MPP & LOGDATE TO SUBSCRIBER TABLE

LEFT JOIN 

datalayer.tbl_engagement_tracker d -- JOIN ON MPP & LOGDATE AND ENTITLEMENT FOR ENGAGEMENT TABLE FOR CORPS AND STAFF
ON a.mpp_account_id = d.accountid AND a.logdate::DATE=d.logdate::DATE AND entitlement_name = 'Web'

WHERE a.logdate::Date BETWEEN current_date - 30 and current_date - 1 and page_type = 'Article'
--and a.mpp_account_id is not null
) a

;


--Select date1,char1,char2,char3
--from localuse.tmp_generic_ocpr
--group by 1,2,3,4
--;

--
--
Select logdate,user_type,Sum(pageviews) as Pageviews,Sum(Users) as Users, Sum(pageviews)/Sum(Users) as Pageviews_per_user

 from (

Select date1 as logdate,
char1 as user_type,
count(char2) as Pageviews,count(distinct char2) as Users
from localuse.tmp_generic_ocpr
group by 1,2
)a
group by 1,2
--;
--;
--Select logdate,Sum(pageviews),Sum(Users) from (
--
--Select date1 as logdate,
--count(char2) as Pageviews,count(distinct char2) as Users
--from localuse.tmp_generic_ocpr
--group by 1
--)a
--group by 1
--;

--
--
--@export on;
--@export set filename="C:\Users\mmazur\Documents\unique_id.csv" CsvColumnDelimiter="||";
--Select char1,char2 from localuse.tmp_generic_ocpr
--;

