SELECT 
--(video_published_date)::date as 'date',
yearkey||'-'||weeknumber as week,
case when video_duration >= 120 then '+2 Minutes' else 'Less then 2 Minutes' end as Length_grouping,
Sum(video_view) as 'Videoviews'

FROM (Select 
video_id,
video_name,
video_percent_viewed,
video_view,
video_impression,
video_seconds_viewed,
video_duration,
video_published_date,
ROW_NUMBER() OVER (PARTITION BY video_id,video_published_date ORDER BY video_view DESC) AS RANK
from
datalayer.tbl_video_brightcove )a
left join 
datalayer.tbl_articlepolopolydata b
on a.video_id = b.video_id
inner join
localuse.d_date c
on (a.video_published_date)::date=c.d_date::Date
where video_published_date::date BETWEEN '2015-09-30' AND '2016-02-17'
AND video_view > 0
and video_impression > 0
and a.rank = 1
group by 1,2