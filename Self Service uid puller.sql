COMMIT;
set search_path = etl_temp,report,star,star_secure,hist,staging,lookups,logging,ztrack; 
set session characteristics as transaction isolation level read committed;
-- PULLS ALL TICKET INFO BASED ON CRITERIA
INSERT /*+ direct */ into etl_temp.tmp_gena (int2, char3, date2, date3,char4,char5,char6,char7,char8,char9,char10,char11,char12,char1)
Select 
case when length(a.social_network_uid__c)-length(translate(a.social_network_uid__c,'1234567890','')) between 1 and 18 then
         case when length(translate (lower(a.social_network_uid__c), '01234567890abcdefghijklmnopqrstuvwxyz=.,?+-*/:;&#@_<>%!|()[]{}­`~?? ',''))=0 then 
              translate (lower(a.social_network_uid__c), 'abcdefghijklmnopqrstuvwxyz=.,?+-*/:;&#@_<>%!|()[]{}­`~?? ','')::INT 
              else -999 end  
         else -999 end as user_uid,
a.Segmentation_Group__c,
a.CREATEDDATE,
a.systemmodstamp,
a.id,
a.Ticket_Paid_Status__c,
a.Payment_Likelihood_Score__c,
a.Payment_Last_30_Days__c,
a.Payment_Lifetime__c,
a.locale_name__c,
b.varchar2,
a.GAME__C,
a.social_network_uid__c,
a.CONTACTID
from (Select 
a.social_network_uid__c,
a.Segmentation_Group__c,
a.CREATEDDATE,
a.systemmodstamp,
rank() over (partition by a.social_network_uid__c order by a.systemmodstamp desc) as rank,
a.id,
a.Ticket_Paid_Status__c,
a.Payment_Likelihood_Score__c,
a.Payment_Last_30_Days__c,
a.Payment_Lifetime__c,
a.locale_name__c,
b.varchar2,
a.GAME__C,
a.CONTACTID

 from staging.s_sfdc_case a INNER JOIN
report.a_team_datastore b ON
a.GAME__C = b.varchar1 AND counter ='game_name'
AND LENGTH( a.SOCIAL_NETWORK_UID__C) <16 and LENGTH( a.SOCIAL_NETWORK_UID__C) > 4
WHERE DATE(a.CREATEDDATE) BETWEEN '${Mininum Ticket Date as yyyy-mm-dd}$' AND '${Maximum Ticket Date as yyyy-mm-dd}$' AND                         -- Last Ticket Time
a.Ticket_Paid_Status__c = '${Ticket Paid Status as Unpaid or Paid or Platinum or Diamond etc}$'                                                   -- Ticket Paid Status                                                                               -- Game Name

group by 1,2,3,4,6,7,8,9,10,11,12,13,14
) a INNER JOIN
report.a_team_datastore b ON
a.GAME__C = b.varchar1 AND counter ='game_name'
where a.rank = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
;
--PAYMENT INFO AND LAST LOGIN DATES
INSERT /*+ direct */ into etl_temp.tmp_genb (date1, int1, char1,date2,date3,int2)

Select date(a.lastdate), 
a.user_uid, 
c.varchar2,
date(a.last_payment_timestamp),
a.firstdate,
a.game_id
from 
(Select a.lastdate, 
a.user_uid, 
c.varchar2,
a.last_payment_timestamp,
a.firstdate,
a.game_id,
rank() over (partition by a.user_uid order by a.lastdate desc) as rank
from star.v_user a inner join etl_temp.tmp_gena b
on a.user_uid=b.int2
INNER JOIN
report.a_team_datastore c ON
a.game_id = c.game_id AND counter ='game_name'
group by 1,2,3,4,5,6) a
inner join etl_temp.tmp_gena b
on a.user_uid=b.int2
INNER JOIN
report.a_team_datastore c ON
a.game_id = c.game_id AND counter ='game_name'
WHERE DATE(a.lastdate) BETWEEN '${Mininum Login Date as yyyy-mm-dd}$' AND '${Maximum Login Date as yyyy-mm-dd}$' AND                     -- Last Login
DATE(a.last_payment_timestamp) BETWEEN '${Mininum Payment Date as yyyy-mm-dd}$' AND '${Maximum Payment Date as yyyy-mm-dd}$' AND         -- Last Payment
c.varchar2 = '${Game Name as CityVille or CastleVille etc}$'                         -- Game Name
and a.rank = 1
group by 1,2,3,4,5,6
;
--LAST NPS SCORE AND SURVEY DATE
INSERT /*+ direct */ into etl_temp.tmp_game_stats_detail_2 (metric2, stat_date, value2, value,metric,value3,metric3,metric4,metric5)
Select 
a.SURVEYTAKER__C,
a.CREATEDDATE,
a.RECOMMEND_SCORE__C,
a.DETRACTORS__C,
a.PASSIVES__C,
a.PROMOTERS__C,
a.questionid,
a.CASE__C,
a.surveyquestionid
from 
(Select 
a.SURVEYTAKER__C,
a.CREATEDDATE,
a.RECOMMEND_SCORE__C,
a.DETRACTORS__C,
a.PASSIVES__C,
a.PROMOTERS__C,
b.ID as 'questionid',
b.CASE__C,
a.id as 'surveyquestionid',
a.SURVEY_QUESTION__C,
rank() over (partition by a.id order by a.CREATEDDATE desc) as rank
from s_sfdc_survey_question_response a right join
s_sfdc_survey_taker b on
a.SURVEYTAKER__C=b.id
right join
etl_temp.tmp_gena c
on c.char4=b.CASE__C
inner join report.a_team_datastore d
on a.survey_question__c = d.varchar1 and d.counter='NPS'
) a
Where a.rank=1 
group by 1,2,3,4,5,6,7,8,9
;
INSERT /*+ direct */ into etl_temp.tmp_skb (metric, metric2)
select a.ID, a.Player_Attributes__c 
from (select a.ID, a.systemmodstamp, a.Player_Attributes__c,
rank() over (partition by a.ID order by a.systemmodstamp desc) as rank
from staging.s_sfdc_contact a
group by 1,2,3) a
where a.rank = 1
group by 1,2;

--LAST SELECT 

SELECT
    DATE(max(b.date1)) AS 'Last Login Date',
    DATE(min(b.date3)) AS 'First Login Date',
    date(max(d.stat_date)) AS 'Last Survey Date', 
    date(max(b.date2)) AS 'Last Payment Date',
    date(max(a.date2)) AS 'Last Ticket Date',
    datediff('day', date(max(b.date2)),date(max(b.date1))) as 'Difference of Login Date vs Payment Date',
    b.char1 as 'Game Name',
    b.int2 as 'Game ID',
    b.int1 AS 'UID',
    c.FIRSTNAME as 'First Name',
    c.LASTNAME as 'Last Name',
    a.char3 AS 'Segmentation Group',
    a.char5 AS 'Ticket Paid Status',
    a.char9 AS 'Language',
    c.EMAIL AS 'Email',
    (case when a.char6>=50 then 'High Likely' else 'Less Likely' end) as 'Grouped Likelihood to pay',
    d.value2 AS 'Recommended Score', 
    d.value as 'Detractor',
    d.metric as 'Passive',
    d.value3::int as 'Promoter',
    case when e.metric2 = 'Special' then 'Special'
         when e.metric2 = 'VIP' then 'VIP'
         when e.metric2 = 'ZIP' then 'ZIP'
else 'Normal' end as 'Player Attribute'
FROM
    etl_temp.tmp_gena a
INNER JOIN etl_temp.tmp_genb b
ON
    a.int2=b.int1
INNER JOIN staging.s_sfdc_contact c
ON
    a.char1=c.ID
LEFT JOIN etl_temp.tmp_game_stats_detail_2 d
ON
a.char4=d.metric4
LEFT JOIN etl_temp.tmp_skb e
on
a.char1=e.metric

GROUP BY
    7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
ORDER BY
    8
limit 500000