select 
date(a.created_at) as created_at,
b.user_id as user_id, 
b.name as user_name, 
c.name as event, 
d.workbook_id as workbook_id, 
d.name as workbook, 
case when g.url_namespace <> '' then concat_ws ('/', 't', g.url_namespace, 'workbooks', d.repository_url) 
	 else concat_ws ('/', 'workbooks', d.repository_url) end as workbook_repository_url, 
e.view_id as view_id, 
e.name as view_name,
case when g.url_namespace <> '' then concat_ws ('/', 't', g.url_namespace, 'views', (replace(e.repository_url,'/sheets/', '/'))) 
	 else concat_ws ('/', 'views', replace(e.repository_url,'/sheets/', '/')) end as view_repository_url,
f.project_id as project_id, 
case when f.name = 'Customer Service' then 'Player Advocacy & Service' else f.name end as project, 
case when g.site_id = 1 then 'Zynga' 
	when g.site_id = 2 then 'Telus'
	when g.site_id = 3 then 'Service Management' end as site_name

from historical_events a

join hist_users b
	on a.hist_actor_user_id = b.id
	
join historical_event_types c
	on a.historical_event_type_id = c.type_id

join hist_workbooks d
	on a. hist_workbook_id = d.id
	
join hist_views e
	on a.hist_view_id = e.id
	
join hist_projects f
	on a.hist_project_id = f.id
	
join hist_sites g
	on a.hist_target_site_id = g.id
	
where date(created_at) >= date(current_date -185)  	
and a.historical_event_type_id = 84