CREATE TABLE CAMPAIGN_TABLE (
         
         start_date   DATE DEFAULT (sysdate)
         end_date   DATE DEFAULT (sysdate)
         currency        VARCHAR2(10),
         brand_name      VARCHAR2(100),
         country         VARCHAR2(100),
         entrystatus     VARCHAR2(20),
         profilestatus   VARCHAR2(20)
         );

INSERT INTO CAMPAIGN_TABLE (create_date,start_date,end_date,currency,brand_name,country,entrystatus,profilestatus)
Select
create_date,
start_date,
end_date,
currency,
brand_name,
country,
entrystatus,
Case when entrystatus = 'ENTRY COMPLETE' and profile_first = 1 then 'ENTRY PROFILED' else 'ENTRY NOT PROFILED' end as profilestatus

from

(Select
create_date,
start_date,
end_date,
currency,
brand_name,
country,
profile_id,
entry_status,
ROW_NUMBER() OVER (PARTITION BY profile_id,campaign_id ORDER BY create_date ASC) AS profile_first
from
(Select
a.create_date,
b.START_DATE,
b.END_DATE,
b.CURRENCY,
c.BRAND_NAME,
c.COUNTRY,
a.profile_id,
a.campaign_id,
a.update_date,
a.entry_id,
case when a.entry_status = 0 and a.error_code = 0 then 'ENTRY COMPLETE' else 'ENTRY IMCOMPLETE' end as entrystatus,
ROW_NUMBER() OVER (PARTITION BY a.entry_id ORDER BY a.update_date DESC) AS rank 


from T_ENTRIES a
inner join T_CAMPAIGNS b on a.CAMPAIGN_ID=b.CAMPAIGN_ID 
inner join T_BRANDS c on a.brand_id=b.brand_id
where update_date = sysdate ) a
where rank = 1) a

;
