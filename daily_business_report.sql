Select 
logdate,
publicationdate,
level1,
article_word_count,
article_id,
articles,
hour_int,
Sum(all_pageviews) as 'All Pageviews',
Sum(homepage) as 'Homepage',
Sum(facebook) as 'Facebook',
Sum(google) as 'Google',
Sum(article)  as 'Article',
Sum(indexed) as 'Index',
Sum(twitter) as 'Twitter',
Sum(digest) as 'Digest',
Sum(Linkedin) as 'Linkedin'
from
(
Select 
a.logdate::date as logdate,
b.publicationdate::date as publicationdate,
case when b.level2 is null then a.site_section else b.level2 end as level1,
b.article_word_count::int as article_word_count,
case when b.articleid is null then '1.'||a.article_id else '1.'||b.articleid end as 'Article_id',
case when b.headline is null then Replace(Replace(Replace(Replace(Replace(Replace(trim(Replace(RTRIM(RIGHT(cs_uri_stem,(char_length(cs_uri_stem) - INSTR(cs_uri_stem,'/', 1, REGEXP_COUNT(cs_uri_stem,'/')))),'1234567890.'),'-',' ')),'%C3%A9', 'e'),'%C3%A1','a'),'%C3%B3','o'),'%C3%A','i'),'%C3%BA','u'),'%C3%B6','oe') else b.headline end as 'Articles',
b.hour_int as hour_int,
case when user_type is null then 'Anonymous' else user_type end as user_type,
count(a.unique_id) as all_pageviews,
Sum(case when a.referer_page_type = 'Home' then 1 else null end) as homepage,
Sum(case when a.referer_page_type ilike '%Facebook%' then 1 else null end) as facebook,
Sum(case when a.referer_page_type ilike '%google%' then 1 else null end) as google,
Sum(case when a.referer_page_type = 'Article' then 1 else null end) as article,
Sum(case when a.referer_page_type = 'Index' then 1 else null end) as indexed,
Sum
(case  when a.referer_page_type  = 'http://t.co' then 1
when a.referer_page_type  = 't.co' then 1
when a.referer_page_type = 'dlvr.it' then 1 
when a.referer_page_type = 'twitterfeed' then 1 
when a.referer_page_type = 'twitter.com' then 1 
when a.referer_page_type = 'https://t.co' then 1 
when a.referer_page_type = 'https://twitter.com' then 1 
when a.referer_page_type = 'twitterrific.com' then 1 
when a.referer_page_type = 'tweetdeck.twitter.com' then 1 
when a.referer_page_type = 'https://tweetdeck.twitter.com' then 1 
 when a.referer_page_type = 'twitter' then 1 else null end) as twitter,
Sum(case when a.referer_page_type ilike '%Digest%' then 1 else null end) as digest,
Sum(case when a.referer_page_type = 'http://lnkd.in' then 1
when a.referer_page_type ilike '%linkedin.com' then 1 else null end) as linkedin


From localuse.vw_weblogdetails_2015_uber_live a
left join
datalayer.tbl_articlepolopolydata b
on a.article_id=b.articleid and content_type in ('Gallery',
'Article with Video',
'Standard')  and b.level1 = 'Business'
WHERE logdate::Date = current_date - 1 and page_type = 'Article' 
group by 1,2,3,4,5,6,7,8
) a
group by 1,2,3,4,5,6,7
;