CREATE VIEW mm_CORPORATE_MONTH_END_matrix (company_group,subs_id,supply,gross_amount) as
Select 
company_group,
SUBS_ID,
Sum(SUPPLY),
Sum(GROSS_AMOUNT)
from
(
Select
company_group,
SUBS_ID,
rrp,
Sum(Finalquantity-complaintquantity) as SUPPLY,
Sum(Finalquantity-complaintquantity)*rrp as GROSS_AMOUNT,
Sum(complaintquantity) as Complaints_quantity,
Sum(finalquantity) as Finalquantity
from
(Select
company_group,
title_grouping,
case when title_grouping = 'The Irish Times' then 0.7 else 0.8 end as vat_p,
SUBS_ID,
SORD_ID,
rrp,
TITLE_NAME,
PRICE_START_DATE,
PRICE_END_DATE,
ADDRLINESINGLE,
QTY as Quantity,
COMPLAINT_QTY as complaintquantity,
QTY as finalquantity
from
(Select 
company_group,
FROMDATE,
TODATE,
SUBS_POINTER,
subs_id,
RATEHEAD_ID,
SAT_RRP,
SUN_RRP,
DEFAULT_RRP,
PRICE_START_DATE,
PRICE_END_DATE,
ISS_DATE,
dayinweek,
QTY,
COMP_DATE,
COMPLAINT_QTY,
SORD_ID,
title_grouping,
TITLE_NAME,
ADDRLINESINGLE,
CASE 
when PRICE_END_DATE is null 
and ISS_DATE >= PRICE_START_DATE 
AND SAT_RRP is null 
and SUN_RRP is null 
and dayinweek in (6,7)
then DEFAULT_RRP
when PRICE_END_DATE is null and ISS_DATE >= PRICE_START_DATE AND dayinweek = 7 THEN SUN_RRP
when PRICE_END_DATE is null and ISS_DATE >= PRICE_START_DATE AND dayinweek = 6 THEN SAT_RRP
ELSE DEFAULT_RRP end as rrp
from
(Select company_group,
ADDRLINESINGLE,
a.FROMDATE,
a.TODATE,
a.SUBS_POINTER,
a.subs_id,
b.QTY,
b.RATEHEAD_ID,
b.SAT_RRP,
b.SUN_RRP,
b.DEFAULT_RRP,
b.PRICE_START_DATE,
b.PRICE_END_DATE,
b.ISS_DATE,
b.COMP_DATE,
TITLE_NAME,
CASE WHEN TITLE_NAME LIKE '%Irish Times%' THEN 'The Irish Times' ELSE 'Other' END AS title_grouping,
case when b.COMPLAINT_QTY is null then 0 else b.COMPLAINT_QTY end as COMPLAINT_QTY,
b.SORD_ID,
EXTRACT (MONTH FROM b.ISS_DATE) AS month_corp,
EXTRACT (MONTH FROM SYSDATE)    AS current_month,
EXTRACT (YEAR FROM b.ISS_DATE) AS year_corp,
EXTRACT (YEAR FROM SYSDATE)    AS current_year,
EXTRACT (MONTH FROM b.COMP_DATE) AS month_complaints,
EXTRACT (YEAR FROM b.COMP_DATE) AS year_complaints
from
(
Select
CASE 
WHEN upper(a.ADDRLINESINGLE) like '%ACC BANK%' then 'ACC BANK'
WHEN upper(a.ADDRLINESINGLE) like '%AIB%' then 'AIB'
WHEN upper(a.ADDRLINESINGLE) like '%IRISH TIMES%' then 'The Irish Times'
WHEN upper(a.ADDRLINESINGLE) like '%KPMG%' then 'KPMG'
WHEN upper(a.ADDRLINESINGLE) like '%INVESTEC%' then 'INVESTEC'
WHEN upper(a.ADDRLINESINGLE) like '%EVERSHEDS%' then 'Eversheds'
WHEN upper(a.ADDRLINESINGLE) like '%IBEC%' then 'IBEC'
WHEN upper(a.ADDRLINESINGLE) like '%DEPARTMENT OF HEALTH%' then 'Dept of Health'
WHEN (a.ADDRLINESINGLE) like '%Forfás%' then 'Forfás'
WHEN b.subs_id = '16964' then 'FMS Wertmanagement'
WHEN b.subs_id = '18562' then 'IPA'
WHEN b.subs_id in('18709','18710','18711','19789','22634') then 'DEPT FOREIGN AFFAIRS'
WHEN b.subs_id = '18815' then 'Russian Embassy'
WHEN b.subs_id = '21725' then 'Royal St. George'
WHEN b.subs_id = '21600' then 'Board Bia'
WHEN b.subs_id = '21599' then 'MKC Communications'
WHEN b.subs_id = '21598' then 'GSOC'
WHEN b.subs_id = '20618' then 'BNP Paribas'
WHEN b.subs_id in ('21894','21895') then 'Beechwood Partners'
WHEN b.subs_id in ('21896','21897','21940') then 'Irish Stock Exchange'
WHEN b.subs_id = '21988' then 'FLAC'
WHEN b.subs_id in ('21985','21986','21987') then 'Irish Arts Council'
WHEN b.subs_id in ('22242',  '22243', '22244', '22264', '22265', '22266') then 'Murray Consultants'
WHEN b.subs_id = '22359' then 'BearingPoint IRL'
WHEN b.subs_id = '22269' then 'Housing Agency'
WHEN b.subs_id in('22466','22467') then 'Aviva'
WHEN b.subs_id = '22514' then 'Mason Hayes  Curran'
WHEN b.subs_id = '22034' then 'RSM Farrell'
WHEN b.subs_id = '22205' then 'Smurfit Kappa'
WHEN b.subs_id = '22802' then 'Citizens Information Board'
WHEN b.subs_id in ('22803','22820','22805') then 'Courts Service'
WHEN b.subs_id in('23025','23026','23027') then 'INIS / RAT'
WHEN b.subs_id in('23314', '23315', '23316','23353','23354','23355','23356','23357','23167','23360','23358','23476','23359','23560','23361','23161','24080') then 'Dept of Justice' -- was missing 23161,24080
WHEN b.subs_id in('23396','23397','23398','23399','23400') then 'NTMA'
WHEN b.subs_id = '23312' then 'Reception Integration'
WHEN b.subs_id in ('22895','442') then 'Office of DPP'                  
WHEN b.subs_id in ('23089','23090') then 'Embassy of Slovakia'
WHEN b.subs_id = '17648' then 'DHKN'                                    
WHEN b.subs_id = '18030' then 'Merit Medical Irl Ltd'                   
WHEN b.subs_id = '18078' then 'Enterprise Ireland'                      
WHEN b.subs_id = '21989' then 'ING Commercial Banking'                  
WHEN b.subs_id = '22590' then 'Irish Family Planning'                   
else b.subs_id end as company_group,
a.ADDRLINESINGLE,
a.FROMDATE,
a.TODATE,
SUBS_POINTER,
b.subs_id
from
CORPORATE_ADDS a
left join
subscriber b
on a.subs_id=b.subs_id
-- OPTIONAL where a.subs_id = '12465' -- OPTIONAL
)a
inner join
(Select
RATEHEAD_ID,
SAT_RRP,
SUN_RRP,
SORD_ID,
DEFAULT_RRP,
PRICE_START_DATE,
PRICE_END_DATE,
ISS_DATE,
SUBS_POINTER,
e.TITLE_NAME,
c.QTY,
d.COMP_DATE,
d.COMPLAINT_QTY
from
SUBSCRIPTION a
left join
PS_CORP_RND_PRICES b
on a.RATE_HEAD_ID=b.RATEHEAD_ID
left join
SUBSCRIPTION_ORDERS c
on a.SORD_POINTER=c.SORD_POINTER
left join 
COMPLAINTS d
on a.SORD_POINTER=d.SORD_POINTER and iss_date=comp_date and d.COMP_ACTION = 'C/ALLOW'
left join
RATEHEAD_ID_RENAME e
on b.RATEHEAD_ID=e.RATEHEAD_ID
where PRICE_END_DATE is null and ISS_DATE >= PRICE_START_DATE
) b
on a.subs_pointer=b.subs_pointer
) a
left join
d_date b
on a.ISS_DATE=d_date
where month_corp=current_month-1 and year_corp=current_year
)a
where 
--rrp is not null and 
ISS_DATE>=FROMDATE and (TODATE is null or ISS_DATE < TODATE)
--OPTIONAL and company_group = 'FMS' -- OPTIONAL
) a
group by company_group,rrp,SUBS_ID) a
group by company_group,SUBS_ID
;





