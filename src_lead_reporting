import datascience as ds
import pandas as pd


## GETTING THE LEADS SPECIFC TO A PERSONA AND A YEAR

        ## CREATE A FUNCTION HERE WHICH TAKES IN PARAMETERS FOR HOW YOU WANT TO CREATE THE INITIAL LEADS DATASET
try:
    query='''
            create table na_field_marketing.temp_leads_ite_2018 as (
            select   distinct lead_id 
            from na_field_marketing.src_leads_sfdc l
            join 
            (
            select distinct "lead contact id"
            from na_field_marketing.prod_nafm_mt pnm
            where "eloqua persona" = 'IT Executive' and "email (person)" != '%ibm%'
            ) p
            on left(l.lead_id,15) = "lead contact id"
            where lead_date_created >= '2018-01-01' and lead_date_created <= '2018-12-31'
            );
            '''
    ds.query_RS(query,option= 'execute')
except:
    query='drop table na_field_marketing.temp_leads_ite_2018;'+query
    ds.query_RS(query,option= 'execute')    

print('--------- Lead Report for IT Executive for the year 2018 ---------')    
    

       ## CREATE A FUNCTION FOR TOTAL LEADS
## CALCULTION OF TOTAL LEADS
query='''
select count(distinct l.lead_id) as leads
from na_field_marketing.src_lmh_sfdc l
join 
na_field_marketing.temp_leads_ite_2018 t
on l.lead_id = t.lead_id
;
    '''
df=ds.query_RS(query)
print('Total number of leads: {}'.format(df['leads'][0]))


       ## CREATE A FUNCTION FOR MALS SALS SQLS              
## CALC FOR MALS SALS SQLS

lead_stages=['Marketing Accepted','Sales Accepted','Sales Qualified']
for lead_stage in lead_stages:
    query='''
        select  count(distinct l.lead_id) as leads
        from na_field_marketing.src_lmh_sfdc l
        join 
        na_field_marketing.temp_leads_ite_2018 t
        on l.lead_id = t.lead_id
        where lead_stage_to='{}'
        ;
    '''.format(lead_stage)
    df=ds.query_RS(query)
    print('{} Leads: {}'.format(lead_stage,df['leads'][0]))  
    
    
## CALC FOR CONVERTED LEADS

query='''
        select count (distinct t.lead_id) as leads
        from na_field_marketing.temp_leads_ite_2018 t 
        join na_field_marketing.src_leads_sfdc s on s.lead_id = t.lead_id
        where s.is_converted is true;
    '''
df=ds.query_RS(query)

print('Total number of Converted Leads for IT Executive for the year 2018 are {}'.format(df['leads'][0]))



    
    


