Select 
yearkey||'-'||weeknumber as week,
Sum(election_articles) as brexit_articles
from
(
Select 
a.logdate::date as logdate,
count(a.unique_id) as election_articles

From localuse.vw_weblogdetails_2015_uber_live a
left join
datalayer.tbl_articlepolopolydata b
on a.article_id=b.articleid and content_type in ('Gallery',
'Article with Video',
'Standard')  
WHERE logdate::Date BETWEEN  '2015-09-30' AND current_date - 1  and page_type = 'Article' AND  b.topic ilike  '%Brexit%'
group by 1
) a
inner join
localuse.d_date b
on a.logdate=b.d_date::Date
group by 1