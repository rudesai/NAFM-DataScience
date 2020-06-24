-- STEP 1: creating a table with all the leads in the NAFM database starting from their created date since 2018
drop table na_field_marketing.temp_nafm_leads;
create table na_field_marketing.temp_nafm_leads as (  -- 493,680 rows
select   distinct lead_id , lead_date_created, l.converted_date,l.is_converted, p."eloqua persona", l.converted_opportunity_id, o.opportunity_forecast_category, o.opportunity_stage_name, o.opportunity_amount
from na_field_marketing.src_leads_global_sfdc l
join 
(
select distinct "lead contact id", p."eloqua persona"
from na_field_marketing.prod_nafm_mt p
where  "email (person)" != '%ibm%' 
) p
on left(l.lead_id,15) = "lead contact id"
left join na_field_marketing.src_opprtnty_sfdc o on l.converted_opportunity_id = o.opportunity_id
where lead_date_created >= '2018-01-01' 
);



-- STEP 2: joining with LMH data
drop table na_field_marketing.temp_nafm_lmh;
create table na_field_marketing.temp_nafm_lmh as (
select  l.lead_stage_from,l.lead_stage_to ,t.*
from na_field_marketing.src_lmh_sfdc l
join na_field_marketing.temp_nafm_leads t
on l.lead_id = t.lead_id
);