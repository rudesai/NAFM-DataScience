import datascience as ds
import pandas as pd
import datetime as dt


## GETTING THE LEADS SPECIFC TO A PERSONA AND A YEAR

        ## CREATE A FUNCTION HERE WHICH TAKES IN PARAMETERS FOR HOW YOU WANT TO CREATE THE INITIAL LEADS DATASET

class lead_reporting_persona:
    
    ## CREATING TABLE
    def create_table(self, start_year=2019):
        query='''
               select distinct "eloqua persona"
               from na_field_marketing.prod_nafm_mt
              '''
        df=ds.query_RS(query)

        ## CREATING PERSONA LIST OUT OF THE DATAFRAME
        series=df['eloqua persona']
        personas=[]
        for item in series:
            item=item.replace(' ','_')
            personas.append(item)
        

        ## CREATING YEAR LIST 
        year_list=[]
        curr_year=dt.datetime.today().year
        for year in range(start_year,curr_year+1):
            year_list.append(year)
        
        for persona in personas: 
            persona_filter_value=persona.replace('_',' ')
            for year in year_list:
                try:
                    query='''
                            create table na_field_marketing.temp_leads_{}_{} as (
                            select   distinct lead_id 
                            from na_field_marketing.src_leads_global_sfdc l
                            join 
                            (
                            select distinct "lead contact id"
                            from na_field_marketing.prod_nafm_mt pnm
                            where "eloqua persona" = '{}' and "email (person)" != '%ibm%'
                            ) p
                            on left(l.lead_id,15) = "lead contact id"
                            where lead_date_created >= '{}-01-01' and lead_date_created <= '{}-12-31'
                            );
                            '''.format(persona,year,persona_filter_value,year,year)
                    ds.query_RS(query,option= 'execute',instance='DS')
                    print('--------- Leads table created for {} for the year {} ---------'.format(persona,year))  
                except:
                    query='drop table na_field_marketing.temp_leads_{}_{};'.format(persona,year)+query
                    ds.query_RS(query,option= 'execute',instance='DS')    
                    print('--------- Leads table created for {} for the year {} ---------'.format(persona,year))
                    
    
    ### CALCULATIONS 
    
    def calculations(self,start_year=2019):
        query='''
               select distinct "eloqua persona"
               from na_field_marketing.prod_nafm_mt
              '''
        df=ds.query_RS(query)

        ## CREATING PERSONA LIST OUT OF THE DATAFRAME
        series=df['eloqua persona']
        personas=[]
        for item in series:
            item=item.replace(' ','_')
            personas.append(item)

        ## CREATING YEAR LIST 
        year_list=[]
        curr_year=dt.datetime.today().year
        for year in range(start_year,curr_year+1):
            year_list.append(year)
        
        ## CREATING A BLANK DATAFRAME WHICH WILL STORE ALL THE RESULTS FROM THE BELOW CALCULATED DATAFRAMES
        grand_master_df=pd.DataFrame()
        
        ## ACTUAL CALCULATIONS PER EACH PERSONA AND PER EACH YEAR HAPPENS BELOW. 
        for persona in personas:     
            for year in year_list:
                ### Print the Persona and the Year the data is about
                
                print('--------- Lead Report for {} for the year {} ---------'.format(persona,year)) 
                
                ### CALCULATION FOR TOTAL NUMBER OF LEADS
                
                query='''
                select count(distinct l.lead_id) as leads
                from na_field_marketing.src_lmh_sfdc l
                join 
                na_field_marketing.temp_leads_{}_{} t
                on l.lead_id = t.lead_id
                ;
                    '''.format(persona,year)
                master_df=ds.query_RS(query)
                
                ## ATTACHING THE METADATA OF THE CALCULATION
                master_df['Persona']=persona
                master_df['Year']=year
                master_df['Stage']='Total Leads'
                
                ## APPENDING THE RECENT ABOVE CALCULATION TO THE GRAND MASTER DATAFRAME
                grand_master_df=grand_master_df.append(master_df)
                print('Total number of leads: {}'.format(master_df['leads'][0]))
                
                ### CALCULATION FOR MAL, SAL, SQL

                lead_stages=['Marketing Accepted','Sales Accepted','Sales Qualified']
                for lead_stage in lead_stages:
                    query='''
                        select  count(distinct l.lead_id) as leads
                        from na_field_marketing.src_lmh_sfdc l
                        join 
                        na_field_marketing.temp_leads_{}_{} t
                        on l.lead_id = t.lead_id
                        where lead_stage_to='{}'
                        ;
                    '''.format(persona,year,lead_stage)
                    df=ds.query_RS(query)
                    
                    ## ATTACHING THE METADATA OF THE CALCULATION
                    df['Persona']=persona
                    df['Year']=year
                    df['Stage']=lead_stage
                    
                    ## APPENDING THE RECENT ABOVE CALCULATION TO THE GRAND MASTER DATAFRAME
                    grand_master_df=grand_master_df.append(df)
                    print('{} Leads: {}'.format(lead_stage,df['leads'][0])) 
                    
                ### CALCULATION FOR CONVERTED LEADS
                
                query='''
                        select count (distinct t.lead_id) as leads
                        from na_field_marketing.temp_leads_{}_{} t 
                        join na_field_marketing.src_leads_global_sfdc s on s.lead_id = t.lead_id
                        where s.is_converted is true;
                    '''.format(persona,year)
                df=ds.query_RS(query)
                
                ## ATTACHING THE METADATA OF THE CALCULATION
                df['Persona']=persona
                df['Year']=year
                df['Stage']='Converted Leads'
                
                ## APPENDING THE RECENT ABOVE CALCULATION TO THE GRAND MASTER DATAFRAME
                grand_master_df=grand_master_df.append(df)

                print('Total number of Converted Leads: {}'.format(df['leads'][0]))
        print('\n')
        return grand_master_df
