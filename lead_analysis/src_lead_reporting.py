import datascience as ds
import pandas as pd
import datetime as dt

class lead_reporting_persona:
    def create_NA_Contacts_wo_CLS_VDM_pull(self):
        '''
        This function requires no parameter. 
        1. It pulls all the NA contacts without any CLS filters.
        2. Creates a custom Persona logic using only the Job Roles
        3. Pushes the transformed dataset to RS
        '''
        ## querying to extract NA contacts data w/o CLS tags from VDM
        query='''
            select distinct c_emailaddress, c_job_role11
        from APL_VDB_ELOQUA.eloqua_cntcts ec
        where c_super_region1 = 'NA' AND status_flag = 1 AND stg_del_flg = 'false' ;
        '''
        df=ds.query_JDV(query,vdm='MARKETING_VDM_DYNAMIC')

        # Create function that creates the custom persona logic
        def custom_persona(x):
            if x in ('Analyst','Consultant','Specialist/Staff','Representative/Specialist'):
                return 'Business Analyst'
            elif x in ('Manager','Product Manager','Director'):
                return 'IT Manager'
            elif x in ('CEO', 'Chairman', 'Chief Security/Compliance Officer', 'CIO', 'COO', 'CTO', 'General Manager', 'Owner', 'Partner/Principal', 'President', 'Vice President'):
                return 'IT Executive'   
            elif x in ('Engineer', 'Engineer/Specialist', 'Programmer/Developer'):
                return 'Lead Developer' 
            elif x in ('Database Administrator', 'Network Administrator', 'System Administrator', 'Webmaster'):
                return 'System Administrator'    
            elif x in ('Architect', 'Chief Architect', 'Chief Architect/Chief Scientist'):
                return 'Architect'    
            else: 
                return 'Other'

        # Apply function to transform the extracted contacts data 
        df['persona'] = df['c_job_role11'].apply(custom_persona)

        # Pushing the transformed dataframe to redshift
        try:
            df.to_redshift('na_eloqua_cncts_wo_cls', 'na_field_marketing',if_exists='replace')
        except:
            ds.query_RS('drop table na_field_marketing.na_eloqua_cncts_wo_cls', option='execute')
            df.to_redshift('na_eloqua_cncts_wo_cls', 'na_field_marketing',if_exists='replace')
    
    
    

## GETTING THE LEADS SPECIFC TO A PERSONA AND A YEAR

        ## CREATE A FUNCTION HERE WHICH TAKES IN PARAMETERS FOR HOW YOU WANT TO CREATE THE INITIAL LEADS DATASET


    
    ## CREATING THE MASTER TABLE
    def create_leads_table_RS(self):
        '''
        This function requires no parameters. It joins the global leads dataset with eloqua contacts, lead management
        table and opportunity table.
        
        This table can be used for any Lead analysis. 
        '''
        query='''
                CREATE TABLE na_field_marketing.persona_leads_analysis as(
                select distinct c.persona, l.lead_id,email, is_converted, l.converted_date, o.opportunity_id,o.opportunity_amount,o.opportunity_forecast_category, h.lead_stage_to, h.rejection_reason, h.comments, l.lead_date_created 
                from na_field_marketing.src_leads_global_sfdc l 
                join na_field_marketing.na_eloqua_cncts_wo_cls c  on lower(l.email)=lower(c.c_emailaddress)   
                join na_field_marketing.src_lmh_sfdc h on h.lead_id=l.lead_id
                left join na_field_marketing.src_opprtnty_sfdc o on l.converted_opportunity_id = o.opportunity_id
                );
              '''
        try:
            ds.query_RS(query, option='execute')
        except:
            ds.query_RS('drop table na_field_marketing.persona_leads_analysis', option='execute')
            ds.query_RS(query, option='execute')
        
        print('The Main persona lead table is created: na_field_marketing.persona_leads_analysis ')

                    
    
    ### CALCULATIONS 
    
    def calculations(self,start_year=2010, push_to_RS=False, table_name='temp_calc_table_leads_personas'):
        '''
        Parameters:
        --------------
        start_year: Determines from which year you want to create the calcs. Default value is 2010
        push_to_RS: If set True, the returned dataframe will be pushed to Redshift database.
        table_name: Sets the table name you want the pushed dataframe to have. 
        
        This function does the calculations for total leads, accepted leads and the converted leads for different personas and
        returns a dataframe. If push_to_RS is setted to True, then the df will be pushed to RS and a new table will be created
        with the name mentioned in table_name parameter. 
        '''
        ## creating a list of personas to loop thorugh
        personas=['Business Analyst','IT Manager','IT Executive','Lead Developer', 'System Administrator',
                 'Architect','Other']

        ## CREATING YEAR LIST 
        year_list=[]
        curr_year=dt.datetime.today().year
        for year in range(start_year,curr_year+1):
            year_list.append(year)
        
        ## CREATING A BLANK DATAFRAME WHICH WILL STORE ALL THE RESULTS FROM THE BELOW CALCULATED DATAFRAMES
        master_df=pd.DataFrame()
        
        ## ACTUAL CALCULATIONS PER EACH PERSONA AND PER EACH YEAR HAPPENS BELOW. 
        for persona in personas:     
            for year in year_list:
                ### Print the Persona and the Year the data is about
                
                print('--------- Lead Report for {} for the year {} ---------'.format(persona,year)) 
                
                ### CALCULATION FOR TOTAL NUMBER OF LEADS
                
                query='''
                    select count(distinct lead_id) as leads 
                    from na_field_marketing.persona_leads_analysis 
                    where persona='{}' and DATE_PART(year,lead_date_created)={}
                ;
                    '''.format(persona,year)
                df=ds.query_RS(query)
                
                ## ATTACHING THE METADATA OF THE CALCULATION
                df['Persona']=persona
                df['Year']=year
                df['Stage']='Total Leads'
                
                ## APPENDING THE RECENT ABOVE CALCULATION TO THE GRAND MASTER DATAFRAME
                master_df=master_df.append(df)
                print('Total number of leads: {}'.format(df['leads'][0]))
                
                ### CALCULATION FOR MAL, SAL, SQL

        ### CALCULATION FOR Accepted Leads
                
                query='''
                    select count(distinct lead_id)  as leads  
                    from na_field_marketing.persona_leads_analysis 
                    where persona='{}' and DATE_PART(year,lead_date_created)={} 
                    and lead_stage_to in ('Marketing Accepted','Sales Accepted')
                ;
                    '''.format(persona,year)
                df=ds.query_RS(query)
                
                ## ATTACHING THE METADATA OF THE CALCULATION
                df['Persona']=persona
                df['Year']=year
                df['Stage']='Accepted Leads'
                
                ## APPENDING THE RECENT ABOVE CALCULATION TO THE GRAND MASTER DATAFRAME
                master_df=master_df.append(df)
                print('Accepted Leads: {}'.format(df['leads'][0]))
                    
                ### CALCULATION FOR CONVERTED LEADS
                
                query='''
                    select count(distinct lead_id)  as leads  -- 53
                    from na_field_marketing.persona_leads_analysis 
                    where persona='{}' and DATE_PART(year,lead_date_created)={}
                    and is_converted is true;
                ;
                    '''.format(persona,year)
                df=ds.query_RS(query)
                
                ## ATTACHING THE METADATA OF THE CALCULATION
                df['Persona']=persona
                df['Year']=year
                df['Stage']='Converted Leads'
                
                ## APPENDING THE RECENT ABOVE CALCULATION TO THE GRAND MASTER DATAFRAME
                master_df=master_df.append(df)
                print('Converted Leads: {}'.format(df['leads'][0]))
                
            print('\n')
        if push_to_RS==False:
            print('A dataframe with all the calculation is returned. The df is not pushed to RS')
            return master_df  
        if push_to_RS==True:
            try:
                master_df.to_redshift(table_name, 'na_field_marketing',if_exists='replace')
            except:
                master_df.query_RS('drop table na_field_marketing.{}'.format(table_name), option='execute')
                master_df.to_redshift(table_name, 'na_field_marketing',if_exists='replace')
            print('\n')
            print('\n')
            print('The dataframe is returned as well as push to RS named: na_field_marketing.{}'.format(table_name))
            return master_df    
