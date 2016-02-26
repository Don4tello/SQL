
--@export on;
--@export set filename="C:\Users\mmazur\Documents\all_video_articles.csv" CsvColumnDelimiter="||";
Select 
--logdate,
week,
case when video_duration >= 120 then '+2 Minutes' else 'Less then 2 Minutes' end as Length_grouping,
--video_duration,
--publicationdate,
--level1,
--'www.irishtimes.com'||cs_uri_stem,
--article_word_count,
--article_id,
--articles,
--hour_int as hour_published,
--referrer_page_type,
--rank,
Sum(all_pageviews)
from
(
Select 
--a.logdate::date as logdate,
yearkey||'-'||weeknumber as week,
--
--b.publicationdate::date as publicationdate,
--cs_uri_stem,
--case when b.level1 is null then a.site_section else b.level1 end as level1,
----b.article_word_count::int as article_word_count,
--case when b.articleid is null then '1.'||a.article_id else '1.'||b.articleid end as 'Article_id',
--b.articleid,
d.video_duration,
--b.headline as articles,
--b.hour_int as hour_int,
--case when user_type is null then 'Anonymous' else user_type end as user_type,
--case when a.referer_page_type  ilike '%news.google%' then 'Google News'
--when a.referer_page_type  ilike '%google%' then 'Google'
--when a.referer_page_type  ilike '%facebook%' then 'Facebook'
--when a.referer_page_type  ilike '%reddit%' then 'Reddit'
--when a.referer_page_type  ilike '%t.co%' then 'Twitter'
--when a.referer_page_type  ilike '%twitter%' then 'Twitter'
--when a.referer_page_type  ilike '%dlvr.it%' then 'Twitter'
--when a.referer_page_type  ilike '%digest%' then 'Digest'
--when a.referer_page_type  =  '(direct)' then 'Direct'
--else a.referer_page_type  end as referrer_page_type,
--row_number() over (partition by b.articleid order by char_length(cs_uri_stem) asc) as rank,
Count(a.unique_id) as all_pageviews

From localuse.vw_weblogdetails_2015_uber_live a
inner join
datalayer.tbl_articlepolopolydata b
on a.article_id=b.articleid and content_type = 'Video'
inner join
localuse.d_date c
on c.d_date::Date=a.logdate::date
left join 
(select video_id, video_duration, ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY video_duration ASC) AS RANK 
from datalayer.tbl_video_brightcove group by 1,2) d
on b.video_id=d.video_id and d.rank = 1
--in ('Gallery',
--'Article with Video',
--'Standard')  
WHERE  page_type = 'Article' and
a.logdate::date < current_Date
--logdate::Date between '2016-01-01'  AND '2016-01-31'-- LOGDATE
--and b.level1 = 'Sport'
--and a.article_id = '2496682' -- ARTICLE_ID 
group by 1,2
--,2,3,4,5,6,7,8
--,6,7,8,9
) a
group by 1,2
--,2,3,4,5,6,7,8
--,7,8
