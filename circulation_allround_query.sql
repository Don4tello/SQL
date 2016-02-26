Select 
--a.start_date,
--a.stop_date,
--b.round_id,
c.subs_id,
c.addrlinesingle,
c.firstname,
c.surname,
c.perscontact4,
c.areacode,
c.contactnumber,
j.title_name,
z.rounddesc,
z.roundid,
d.sord_id,
d.sord_startdate,
d.sord_stopdate,
d.sord_pointer,
d.serviceid,
e.iss_date,
e.QTY
--e.iss_date,
--g.comp_type,
--g.comp_date,
--g.comp_issue,
--g.comp_id,
--g.timestamp,
--Sum(g.Complaint_qty) as complaint_quantity_in_papers,
--count(distinct comp_date) as number_of_complaints

from 
subscription d
left join
rptvw_ps_address c on d.subs_pointer=c.subs_pointer
left join
subscription_orders e on d.sord_pointer=e.sord_pointer
--left join
--rnddist a on d.sord_pointer=a.sord_pointer
--left join
--round b on b.round_pointer=a.round_pointer
--left join
--subscriber f on d.subs_pointer=f.subs_pointer
--left join 
--complaints g on d.sord_pointer=g.sord_pointer and COMP_ACTION = 'C/ALLOW'
--left join
--compaction h on g.comp_action=h.compact_id
--left join
--issue i on g.comp_issue=i.iss_id
left join
RATEHEAD_ID_RENAME j on d.rate_head_id = j.ratehead_id
left join
(Select z.sordid,z.rounddesc,z.roundid from rptvw_roundcards_1 z group by z.sordid,z.rounddesc,z.roundid) z
 on z.sordid=d.sord_id 




where 
--e.iss_date < sysdate+1
--and round_id <> 'CORP' 
--and sord_stopdate is null
--and 
--start_date BETWEEN sysdate-40 AND sysdate
--and sord_startdate BETWEEN sysdate-40 AND sysdate
--and 
--e.iss_date BETWEEN sysdate-40 AND sysdate and 
--comp_date BETWEEN sysdate-40 AND sysdate
--and d.RATE_HEAD_ID NOT IN ('DDELOTHER', 'IT DEL DD', 'IT DEL EX', 'OTHER DEL')
--RATE_HEAD_ID  IN (
--'IDM MTH DD',
--'IDM 6 MTHS',
--'IDM 3MTHR',
--'IDM 3 MTHS',
--'IDM 12MTHR',
--'IDM 12 MTH',
--'FOC IDM')
--j.title_name = 'Irish Daily Mail'
--AND (E.ISS_DATE>=TO_DATE ('21-02-2016 00:00:00', 'DD-MM-YYYY HH24:MI:SS') AND E.ISS_DATE<TO_DATE ('27-02-2016 00:00:01', 'DD-MM-YYYY HH24:MI:SS'))
c.subs_id = 21409
--and stop_date is null
--group by
--a.start_date,
----a.stop_date,
------b.round_id,
--c.subs_id,
--c.addrlinesingle,
--c.firstname,
--c.surname,
--c.perscontact4,
--c.areacode,
--c.contactnumber,
--j.title_name,
--d.sord_id,
--d.sord_startdate,
--d.sord_stopdate,
--d.sord_pointer,
--d.serviceid,
--e.iss_date
--,
--e.QTY
--e.iss_date,
--g.comp_type
--g.comp_date
--g.comp_issue,
--g.comp_id,
--g.timestamp


