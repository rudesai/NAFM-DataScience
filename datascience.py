from pandas import DataFrame as _DataFrame
from functools import wraps as _wraps
import os

def query_JDV(queryString, vdm='sales_vdm', big_query=False):
    """Query JDV and return DataFrame

    Keyword arguments:
    queryString -- SQL query
    vdm -- sales_vdm, eng_enterprise_vdm, cee_vdm_dynamic
    big_query -- True/False
    """

    import pandas.io.sql as psql
    import jaydebeapi

    username = os.environ['JDV_USER']
    password = os.environ['JDV_PASS']
    # dir_path = os.path.dirname(os.path.realpath(__file__))

    if big_query:
        print("Requesting Big Query Connection")
        connection_path = '@mms://bqjdv.prod.a4.vary.redhat.com:31000'
    else:
        connection_path = '@mms://jdv.prod.a4.vary.redhat.com:31000'

    conn = jaydebeapi.connect(jclassname='org.teiid.jdbc.TeiidDriver',
                              url='jdbc:teiid:' + vdm + connection_path,
                              # add new service account for domino ?
                              driver_args=[username, password],
                              jars='/tmp/teiid-10.2.1-jdbc.jar')

    df = psql.read_sql(sql=queryString, con=conn)
    conn.close()
    return df

def get_encoded_aws_keys(module_folder='datascience'):
    """
    Retreives AWS access keys used for S3
    
    Parameters:
    module_folder (string): folder where datascience module is held
    
    Returns: AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY
    """
    import configparser
    config = configparser.ConfigParser()
    path = '/home/ubuntu/.local/bin/aws.ini'
    config.read(path)
    
    sections = config.sections()
    username = config['aws_access_key']['aws_access_key_id']
    password = config['aws_access_key']['aws_secret_access_key']
    
    return username, password

def decode(str):
    """
    Decodes credentials
    
    Parameters:
    str (string): encoded string
    
    Returns:
    decoded string
    """
    import base64
    return base64.b64decode(str).decode("utf-8")

def list_s3():
    """List all files in S3. Sorted by last modified date.

    Keyword arguments:
    N/A
    """
    import pandas as pd
    import boto3
    import base64
    files = []
    
    username, password = get_encoded_aws_keys()

    session = boto3.Session(
        aws_access_key_id=decode(username),
        aws_secret_access_key=decode(password))
    s3 = session.resource('s3')
    bucket = s3.Bucket('rhdatasci')
    for obj in bucket.objects.all():
        files.append({'file_name': obj.key, 'size_MB': obj.size, 'last_modified': obj.last_modified})

    files = pd.DataFrame(files)
    files['size_MB'] = (files['size_MB']/1000000)
    files['last_modified'] = pd.to_datetime(files['last_modified'].astype(str).str[0:10] + ' ' + files['last_modified'].astype(str).str[11:19])
    files = files.sort_values('last_modified',ascending=False)
    return files

def read_s3(key,file_type='csv',clean_names=True):
    """Read file from S3 and return DataFrame

    Keyword arguments:
    key -- file name key (e.g. ops/cases_RS)
    file_type -- csv or excel (default='csv')
    clean_names -- removes spaces and replaces with underscores
    """
    import pandas as pd
    import boto3
    import io
    
    # Create S3 session
    username, password = get_encoded_aws_keys()
    session = boto3.Session(
        aws_access_key_id=decode(username),
        aws_secret_access_key=decode(password))
 
    s3 = session.resource('s3')
    obj = s3.Object(bucket_name='rhdatasci', key=key)
    response = obj.get()
    if file_type=='csv':
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
    elif file_type=='excel':
        df = pd.read_excel(io.BytesIO(response['Body'].read()))
    if clean_names:
        df.clean_names()
    return df
#changed the default
def connect_RS(instance='DS'):
    """Connect to RedShift
 
    Keyword arguments:
    instance -- DS or QA or DAVE or CEEANRS instance (default='DS')
    """
    import psycopg2
    if instance=='DAVE':
        username = os.environ['DAVE_RS_USER']
        password  = os.environ['DAVE_RS_PASS']
        host='rsdscirhprod.cpe1poooghvl.us-west-2.redshift.amazonaws.com'
        database='rsdscirh'
    elif instance=='CEEANRS':
        username = os.environ['DS_RS_USER']
        password  = os.environ['DS_RS_PASS']
        host='rhdsrsprod.cpe1poooghvl.us-west-2.redshift.amazonaws.com'
        database='ceeanrs'
    elif instance=='DS':
        username = os.environ['DS_RS_USER']
        password  = os.environ['DS_RS_PASS']
        host='rhdsrsprod.cpe1poooghvl.us-west-2.redshift.amazonaws.com'
        database='rhdsrs'
    elif instance=='QA':
        username = os.environ['DS_RS_USER']
        password  = os.environ['DS_RS_PASS']
        host='rhdwrsqa.cmnodbxtjyuq.us-east-1.redshift.amazonaws.com'
        database='dwrsdev'
        
    conn_string = "dbname={3} port='5439' user='{0}' password='{1}' \
                   host={2}".format(username,password,host,database)
    conn = psycopg2.connect(conn_string)
 
    return conn


def query_madb(query_string, madb_db_host='dwdb2.corp.redhat.com',
                 db_name='marketing_analytics'):
    """Query marketing_analytics db and return DataFrame
 
    Keyword arguments:
    queryString -- SQL query
    madb_db_host -- mysql host url
    db_name -- name of db
    """
 
    import pandas.io.sql as psql
    from sqlalchemy import create_engine
    from sqlalchemy import text
    
    try:
        import MySQLdb
    except ImportError as e:
        print(f"{e}...: Try pip installing mysqlclient")
 
    engine = create_engine(
        f"mysql+mysqldb://{os.environ['AREN_MKTANALYTICS_USER']}:{os.environ['AREN_MKTANALYTICS_PASS']}@{madb_db_host}:3306/{db_name}", 
        connect_args={'read_timeout': 28800, 'write_timeout': 28800, 'compress': True}, pool_recycle=7200)
 
    df = psql.read_sql(sql=text(query_string), con=engine)
    engine.dispose()
    return df
    
    
def query_phenom(query_string, phenom_db_host='mysql01.dw.prod.int.phx2.redhat.com',
                 db_name='PhenomDB', no_use_on=1):
    """Query Phenom db and return DataFrame
 
    Keyword arguments:
    queryString -- SQL query
    phenom_db_host -- mysql host url
    db_name -- name of db
    no_use_on -- specify day of week (M=1, Sun=7) that the db is being reloaded, thus don't query then.
    """
    import datetime
    import pandas.io.sql as psql
    from sqlalchemy import create_engine
    from sqlalchemy import text
    
    now = datetime.datetime.now()
    if now.isoweekday() == no_use_on:
        raise Exception(f"{db_name} is being reloaded today and cannot be queried - please try your query tomorrow")

    try:
        import MySQLdb
    except ImportError as e:
        print(f"{e}...: Try pip installing mysqlclient")
    engine = create_engine(
        f"mysql+mysqldb://{os.environ['AREN_PHENOMDB_USER']}:{os.environ['AREN_PHENOMDB_PASS']}@{phenom_db_host}:3306/{db_name}",
        connect_args={'read_timeout': 28800, 'write_timeout': 28800, 'compress': True}, pool_recycle=7200)
 
    df = psql.read_sql(sql=text(query_string), con=engine)
    engine.dispose()
    return df
    
    #default to DS
def query_RS(queryString,option='read',instance='DS'):
    """Query RedShift and return DataFrame

    Keyword arguments:
    queryString -- SQL query
    option -- use 'read' when reading data and 'execute' when creating tables, granting permissions, etc. (default='read')
    instance -- redshift instance name: DAVE or DS or QA (default='DS')
    """
    
    import pandas.io.sql as psql

    conn = connect_RS(instance=instance)
    if option=='read':
        df = psql.read_sql(sql=queryString, con=conn);
        conn.close()
        return df
    elif option=='execute':
        cur = conn.cursor()
        cur.execute(queryString)
        conn.commit()
        print('Success')
    conn.close()
    return

def kill_connection(table_name,instance='DAVE'):
    
    """Kill all active connection to a table in Redshift

    Keyword arguments:
    tablename -- Table name
    instance -- redshift instance name: DAVE or DS or QA (default='DAVE')
    """
    
    process_query = """SELECT 
    l.pid
    FROM pg_catalog.pg_locks l
    JOIN pg_catalog.pg_class c ON c.oid = l.relation
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_stat_activity a ON a.procpid = l.pid
    where TRIM(c.relname) not like 'pg%'
    and c.relname = '{0}';
                  """.format(table_name)
    
    pids = ds.query_RS(queryString=process_query,
                       instance=instance)
    for i in pids['pid']:
        try:
            kill_query = """SELECT pg_terminate_backend({0});""".format(i)
            ds.query_RS(queryString=kill_query,
                        instance=instance)
        except Exception as e:
            print(str(e))
            return 'Failed'
    return 'Sucess'

def log_progress(sequence, every=None, size=None, name='Items'):
    
    from ipywidgets import IntProgress, HTML, VBox
    from IPython.display import display
    
    is_iterator = False
    if size is None:
        try:
            size = len(sequence)
        except TypeError:
            is_iterator = True
    if size is not None:
        if every is None:
            if size <= 200:
                every = 1
            else:
                every = int(size / 200)     # every 0.5%
    else:
        assert every is not None, 'sequence is iterator, set every'

    if is_iterator:
        progress = IntProgress(min=0, max=1, value=1)
        progress.bar_style = 'info'
    else:
        progress = IntProgress(min=0, max=size, value=0)
    label = HTML()
    box = VBox(children=[label, progress])
    display(box)

    index = 0
    try:
        for index, record in enumerate(sequence, 1):
            if index == 1 or index % every == 0:
                if is_iterator:
                    label.value = '{name}: {index} / ?'.format(
                        name=name,
                        index=index
                    )
                else:
                    progress.value = index
                    label.value = u'{name}: {index} / {size}'.format(
                        name=name,
                        index=index,
                        size=size
                    )
            yield record
    except:
        progress.bar_style = 'danger'
        raise
    else:
        progress.bar_style = 'success'
        progress.value = index
        label.value = "{name}: {index}".format(
            name=name,
            index=str(index or '?')
        )

def _monkeypatch_method(cls):

    """
    Creates a decoration for monkey-patching a class
    Recipe from: https://mail.python.org/pipermail/python-dev/2008-January/076194.html
    Args:
        cls:
    Returns:
    """
    @_wraps(cls)
    def decorator(func):
        setattr(cls, func.__name__, func)
        return func
    return decorator

def _resolve_qualname(table_name, schema=None):
    name = '.'.join([schema, table_name]) if schema is not None else table_name
    return name


@_monkeypatch_method(_DataFrame)
def to_redshift(self,table_name,schema,group='default',index=False,
               if_exists='fail',truncate=False,compress=True, aws_access_key_id=None, aws_secret_access_key=None,
               null_as=None, emptyasnull=True, grant=None,new_file=True,instance='DS',parallel=0):
    
    
  

#    Inserts dataframe to Redshift by creating a file in S3
#    Args:
#        self: Panda' dataframe to insert into Redshift
#        table_name: Name of the table to insert dataframe
#        group: RedShift group to share the table with, use None for not sharing
#        index: bool; whether to include the DataFrame's index
#        engine: An SQL alchemy engine object
#        bucket: S3 bucket name
#        keypath: Keypath in s3 (without bucket name)
#        schema: Redshift schema
#        if_exists: {'fail', 'append', 'replace'}
#        compress: Compresses data before uploading it to S3
#        aws_access_key_id: from ~./boto by default
#        aws_secret_access_key: from ~./boto by default
#        null_as: treat these values as null (not tested)
#        emptyasnull: bool; whether '' is inserted as null
#        grant: List of the following privileges {'SELECT','INSERT','UPDATE','DELETE'}. Default 'ALL'
#    Returns:

    import gzip
    import boto3
    from sqlalchemy import MetaData
    from pandas import DataFrame
    from pandas.io.sql import SQLTable, pandasSQL_builder
    import psycopg2
    import codecs
    from io import BytesIO
    from sqlalchemy import create_engine,VARCHAR

    if instance=='DAVE':
        username = os.environ['DAVE_RS_USER']
        password  = os.environ['DAVE_RS_PASS']
        aws_access_key_id = os.environ['S3_ID']
        aws_secret_access_key = os.environ['S3_KEY']
        key_path = os.environ['S3_KEY_PATH']
        bucket = os.environ['S3_BUCKET']
        host = 'rsdscirhprod.cpe1poooghvl.us-west-2.redshift.amazonaws.com'
        database = 'rsdscirh'
        s3_keypath = '{}/{}'.format(key_path, table_name)
    else:
        username = os.environ['DS_RS_USER']
        password  = os.environ['DS_RS_PASS']
        encoded_username, encoded_password = get_encoded_aws_keys()
        aws_access_key_id = decode(encoded_username)
        aws_secret_access_key = decode(encoded_password)
        bucket='rhdatasci'
        host = 'rhdsrsprod.cpe1poooghvl.us-west-2.redshift.amazonaws.com'
        database = 'rhdsrs'
        s3_keypath = 'ops/{0}'.format(table_name)
    port = '5439'
   
    conn_string = 'postgresql+psycopg2://' + username + ':' + password + '@' + host + ':' + port + "/" + database
    engine = create_engine(conn_string)
    
    # Determine output URL:
    keypath = "{filename}.{ext}".format(filename=s3_keypath, ext="gzip" if compress else "csv")
    url = bucket + '/' + keypath
    print('Full S3 url: {}'.format(url))
    
    #Adds file to S3
    # executes only if there is one file and not more
    if parallel==0:
        #Create S3 session and determine URL of file to delete
        s3_bucket, s3_obj = create_s3_session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, bucket=bucket, keypath=keypath, compress=compress)
        self.to_s3(s3_obj=s3_obj, table_name=table_name, index=index, compress=compress, new_file=new_file)

    qualname = _resolve_qualname(table_name, schema)
    #modified to be able to create a table only if u r pulling one file at a time
    if parallel==0:
        obj_cols = self.select_dtypes(include=[object]).columns.values.tolist()

        varchar_cols = dict((col,self[col].str.len().max()*4) for col in obj_cols if self[col].astype(str).str.\
                        contains(r'[^a-zA-Z0-9 -_]').any())
        table = SQLTable(table_name, pandasSQL_builder(engine, schema=schema),
                 self, if_exists=if_exists, index=False,dtype={c: VARCHAR(int(varchar_cols[c])) for c in varchar_cols})    

        if truncate:
            print('Updating table {}'.format(qualname))
        else:
            print("Creating table {}".format(qualname))
        if table.exists():
            if if_exists == 'fail':
                raise ValueError("Table Exists")
            elif if_exists == 'append':
                if truncate:
                    queue = ['truncate table {};'.format(qualname)]
                else:
                    queue = []
            elif if_exists == 'replace':
                queue = ['drop table {};'.format(qualname), table.sql_schema() + ";"]
            else:
                raise ValueError("Bad option for `if_exists`")

        else:
            queue = [table.sql_schema() + ";"]

    else:
        queue = []
        
    stmt = ("COPY {qualname}\n"
           "FROM 's3://{keypath}' \n"
           "CREDENTIALS 'aws_access_key_id={key};aws_secret_access_key={secret}' "
           "{gzip} "
           "{null_as} "
           #"{emptyasnull}"
           "delimiter '~' CSV IGNOREHEADER 1 maxerror 5 region 'us-west-2';"
           "commit;"

           
                                        ).format(qualname=qualname,
                                                 keypath=url,
                                                 key=aws_access_key_id,
                                                 secret=aws_secret_access_key,
                                                 gzip="GZIP " if compress else " ",
                                                 null_as="NULL AS '{}'".format(null_as) if null_as is not None else "",
                                                 emptyasnull="EMPTYASNULLL " if emptyasnull else " ")
    
    
 
        
    if instance == 'DS':
        query='''select groname from pg_group pg 
                join pg_namespace ns 
                on left(groname,len(groname)-3) =nspname
                where nspname='{0}'
                order by nspname'''.format(schema)
        rp_cols = query_RS(query,instance='DS')['groname'].tolist()
    else:
        schema_group_dict = {'rsdsci_eng':['rsdsci_eng_rw'],
                             'rsdsci_cee':['rsdsci_cee_rw'],
                             'rsdsci_people':['rsdsci_people_rw'],
                             'rsdsci_sales':['rsdsci_sales_rw'],
                             'rsdsci_mkt':['rsdsci_mkt_rw'],
                            }
        rp_cols = schema_group_dict[schema]
    if group:
        if type(grant) == list:
            privileges = ",".join(grant)
        else:
            privileges = 'SELECT'
        if group == 'default':
            for i in rp_cols: 
                stmt+= ("grant {privileges} on {qualname}\n"
                "to group {group}; commit;").format( privileges=privileges,
                                                    qualname=qualname,
                                                    group=i)
        else:
            stmt += ("grant {privileges} on {qualname}\n"
                "to group {group}; commit;").format(privileges=privileges,
                                                    qualname=qualname,
                                                    group=group)
      

    queue.append(stmt)

    print("Querying Redshift...")
    with engine.begin() as con:
        for stmt in queue:
            if 'COPY' in stmt:
                print(stmt.split('commit;', 1)[1])
            else:
                print(stmt)
            con.execute(stmt)
            
    #After tables copied from S3 to Redshift, delete that file in S3
    #Create S3 session and determine URL of file to delete
    s3_bucket, s3_obj = create_s3_session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, bucket=bucket, keypath=keypath, compress=compress)
    delete_from_s3(s3_bucket_obj=s3_bucket, keypath=keypath)
        

def create_s3_session(aws_access_key_id, aws_secret_access_key, bucket, keypath, fy='', compress=True):
    """
    Connects to S3
    
    Parameters:
    aws_access_key_id (string): Access key ID
    aws_secret_access_key (string): Access key secret
    bucket (string): Name of S3 bucket 
    keypath (string): Name of file
    fy (string): appends fiscal year to end of S3 keypath, if included
    compress (boolean): idicates if S3 object should be compressed as .gzip
    """
    import boto3
    
    session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)   
    
    s3 = session.resource('s3')
    mybucket = s3.Bucket(bucket)
    obj = mybucket.Object(bucket_name=bucket, key=keypath+ '{}'.format(fy))
    
    return mybucket, obj
    
        
def delete_from_s3(s3_bucket_obj, keypath):
    """
    Deletes a file from S3, meant to be called after table is written Redshift
  
    Parameters: 
    s3_obj (string) : S3 object to be removed
    keypath (boolean) : prefix of keypath of files in S3 to delete
  
    Returns: 
    boolean: True if file removed from S3, False otherwise
    """
    import boto3
            
    for obj in s3_bucket_obj.objects.filter(Prefix=keypath):
        print('bucket where file is to be removed: {}'.format(s3_bucket_obj.name))
        print('keypath of file to be removed:{}'.format(obj.key))
        obj.delete()


@_monkeypatch_method(_DataFrame)
def to_s3(self, s3_obj, index, table_name, new_file, fy='', compress=True):
    
    
    import gzip
    import boto3
    from sqlalchemy import MetaData
    from pandas import DataFrame
    from pandas.io.sql import SQLTable, pandasSQL_builder
    import psycopg2
    import codecs
    import os
    from io import BytesIO
    
    """
    Writes the data frame to S3
    Args:
        self: Dataframe to upload
        s3_obj (Object): S3 object dataframe upload location
        index (string): indicates index of file
        new_file (boolean): indicates if a new file must be made before sending to S3
        fy (string): fiscal year to be appended at end of file name locally, but NOT S3
        compress (boolean): indicates if file is to be compressed as gzip
    Returns: The S3 URL of the file, and the credentials used to upload it
    """
    
    print('bucket where dataframe is to be copied: {}'.format(s3_obj.bucket_name))
    print('keypath of file to be copied:{}'.format(s3_obj.key))
    print("Writing to disk...")

    # Compress
    if new_file:
        if compress:
            self.to_csv(('./{filename}_{fy}.csv').format(filename=table_name, fy=fy), index=index,sep='~',compression='gzip')
        else:
            self.to_csv(('./{filename}_{fy}.csv').format(filename=table_name, fy=fy), index=index,sep='~')

        def percent_cb(complete, total):
            sys.stdout.write('.')
            sys.stdout.flush()

        s3_obj.upload_file(('./{filename}_{fy}.csv').format(filename=table_name, fy=fy),ExtraArgs={'ServerSideEncryption':'AES256'})
        os.remove(('./{filename}_{fy}.csv').format(filename=table_name, fy=fy))
        return True
        
    return False

# Newly added when to_s3 function needed to be called individually
def to_s3_custom(self,table_name,instance='DS',fy=''):
    username, password = get_encoded_aws_keys()
    aws_access_key_id = decode(username)
    aws_secret_access_key = decode(password)
    if instance =='DS':
        bucket='rhdatasci'
        host = 'rhdsrsprod.cpe1poooghvl.us-west-2.redshift.amazonaws.com'
        database = 'rhdsrs'
        s3_keypath = 'ops/{0}'.format(table_name)
    else:
        username = os.environ['DS_RS_USER']
        password  = os.environ['DS_RS_PASS']
        bucket='rhdatasci'
        host = 'rhdsrsprod.cpe1poooghvl.us-west-2.redshift.amazonaws.com'
        database = 'rhdsrs'
        s3_keypath = 'ops/{0}'.format(table_name)
        
    # Determine output URL:
    keypath = "{filename}.{ext}".format(filename=s3_keypath, ext="gzip")
    # url = bucket + '/' + keypath
    # print('Full S3 url: {}'.format(url))
    
    #Create S3 session and determine URL of file to delete
    bucket, obj = create_s3_session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, bucket=bucket, keypath=keypath, fy=fy, compress=True)
    return to_s3(self, s3_obj=obj, table_name=table_name, index=False, compress=True, new_file=True, fy=fy)

def expandgrid(*itrs):
    """Creates dataframe of all unique pairs given multiple lists
    Keyword arguments:
    *itrs -- iterable objects
    """
    import itertools
    from pandas import DataFrame
    product = list(itertools.product(*itrs))
    return DataFrame({'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))})

@_monkeypatch_method(_DataFrame)
def clean_names(self):
    self.columns=self.columns.str.replace(' ',"_").str.replace(' ',"_").str.replace('/',"_").str.\
    replace('(',"_").str.replace(')',"_").str.lower().tolist()
    

@_monkeypatch_method(_DataFrame)
def clean_str_cols(self):
    """
    Remove blanks and special characters from string columns 
    """
    str_cols=self.select_dtypes(include=['object']).columns.tolist()
    self[str_cols]=self[str_cols].apply(lambda x: x.str.replace(r"\r\n",'').str.\
                                      replace('[^\w\s\-\_]','').str.replace('\n','').str.replace('~',''))

#2020-03-20 - Added a new function with the current definiton of current_fyq

def current_RHfyq(shift = 0):
    import fiscalyear
    import numpy as np

    fiscalyear.START_MONTH = 3
    fiscal_date = fiscalyear.FiscalDate.today()
    if fiscal_date.month > 2:
        fiscal_year = fiscal_date.year + 1
    else:
        fiscal_year = fiscal_date.year
    fiscal_qtr = fiscal_date.quarter
    if shift != 0:
        fiscal_year = int(fiscal_year + np.floor((fiscal_qtr+shift-1)/4))
        fiscal_qtr = (fiscal_qtr+shift)%4
        if fiscal_qtr == 0:
            fiscal_qtr = 4
    
    fyq = f'FY{fiscal_year}-Q{fiscal_qtr}'
    
    return(fyq)

# 2020-03-20 - Modified the current_fyq function to calculate the fyq based on IBM calendar
def current_fyq(shift = 0):
    import fiscalyear
    import numpy as np
    fiscalyear.START_MONTH = 1
    fiscal_date = fiscalyear.FiscalDate.today()
    fiscal_year = fiscal_date.year
    fiscal_qtr = fiscal_date.quarter
    if shift != 0:
        fiscal_year = int(fiscal_year + np.floor((fiscal_qtr+shift-1)/4))
        fiscal_qtr = (fiscal_qtr+shift)%4
        if fiscal_qtr == 0:
            fiscal_qtr = 4
    fyq = f'FY{fiscal_year}-Q{fiscal_qtr}'
    return(fyq)
    
def sfdc_to_pd(reportid, filters=dict()):
    """Read SFDC report and return DataFrame

    Keyword arguments:
    reportid -- SFDC report ID (e.g. '00O60000004kdes')
    filters -- dictionary of filters (e.g. {'pv0':'filter0','pv1':'filter1'})
    """
    
    import pandas as pd
    import csv
    import requests
    from simple_salesforce import Salesforce

    sfdc_username = os.environ['SFDC_EMAIL']
    sfdc_password  = os.environ['SFDC_PASS']
    security_token  = os.environ['SFDC_TOKEN']
    sf = Salesforce(username=sfdc_username, password=sfdc_password, security_token=security_token)

    all_filters = str()
    for key in filters:
        all_filters += key+'='+str(filters[key])+'&'

    with requests.session() as s:
        d = s.get("https://redhat.my.salesforce.com/{}?{}export=1&enc=UTF-8&xf=csv".format(reportid, all_filters),
                  headers=sf.headers,
                  cookies={'sid': sf.session_id})
        lines = d.content.decode('utf-8').splitlines()
        reader = csv.reader(lines)
        data = list(reader)
        data = data[:-7]
        df = pd.DataFrame(data)
        df.columns = df.iloc[0]
        df = df.drop(0)
        return df
        
def reduce_mem_usage(df):
    """
    Reduces the memory size of a dataframe by looking at min and max value
    of each numeric field and reducing the data type when possible
    """
    
    import numpy as np
    
    start_mem_usg = df.memory_usage(deep=True).sum() / 1024**3 
    print("Memory usage of dataframe is :",round(start_mem_usg,2)," GB")
    NAlist = [] # Keeps track of columns that have missing values filled in. 
    
    for col in df.columns:
        if df[col].dtype not in [object,np.dtype('datetime64[ns]')] :  # Exclude strings and dates
            
            # make variables for Int, max and min
            IsInt = False
            mx = df[col].max()
            mn = df[col].min()
            
            # Integer does not support NA, therefore, NA needs to be filled
            if not np.isfinite(df[col]).all(): 
                NAlist.append(col)
                continue
                   
            # test if column can be converted to an integer
            asint = df[col].fillna(0).astype(np.int64)
            result = (df[col] - asint)
            result = result.sum()
            if result > -0.01 and result < 0.01:
                IsInt = True
 
            
            # Make Integer/unsigned Integer datatypes
            if IsInt:
                if mn >= 0:
                    if mx < 255:
                        if df[col].dtype != np.dtype('uint8'):
                            print("******************************")
                            print("Column: ",col)
                            print(df[col].dtype,"-->",'uint8')
                        df[col] = df[col].astype(np.uint8)
                    elif mx < 65535:
                        if df[col].dtype != np.dtype('uint16'):
                            print("******************************")
                            print("Column: ",col)
                            print(df[col].dtype,"-->",'uint16')
                        df[col] = df[col].astype(np.uint16)
                    elif mx < 4294967295:
                        if df[col].dtype != np.dtype('uint32'):
                            print("******************************")
                            print("Column: ",col)
                            print(df[col].dtype,"-->",'uint32')
                        df[col] = df[col].astype(np.uint32)
                    else:
                        if df[col].dtype != np.dtype('uint64'):
                            print("******************************")
                            print("Column: ",col)
                            print(df[col].dtype,"-->",'uint64')
                        df[col] = df[col].astype(np.uint64)
                else:
                    if mn > np.iinfo(np.int8).min and mx < np.iinfo(np.int8).max:
                        if df[col].dtype != np.dtype('int8'):
                            print("******************************")
                            print("Column: ",col)
                            print(df[col].dtype,"-->",'int8')
                        df[col] = df[col].astype(np.int8)
                    elif mn > np.iinfo(np.int16).min and mx < np.iinfo(np.int16).max:
                        if df[col].dtype != np.dtype('int16'):
                            print("******************************")
                            print("Column: ",col)
                            print(df[col].dtype,"-->",'int16')
                        df[col] = df[col].astype(np.int16)
                    elif mn > np.iinfo(np.int32).min and mx < np.iinfo(np.int32).max:
                        if df[col].dtype != np.dtype('int32'):
                            print("******************************")
                            print("Column: ",col)
                            print(df[col].dtype,"-->",'int32')
                        df[col] = df[col].astype(np.int32)
                    elif mn > np.iinfo(np.int64).min and mx < np.iinfo(np.int64).max:
                        if df[col].dtype != np.dtype('int64'):
                            print("******************************")
                            print("Column: ",col)
                            print(df[col].dtype,"-->",'int64')
                        df[col] = df[col].astype(np.int64)    
            
            # Make float datatypes 32 bit
            else:
                if df[col].dtype != np.dtype('float32'):
                    print("******************************")
                    print("Column: ",col)
                    print(df[col].dtype,"-->",'float32')
                df[col] = df[col].astype(np.float32)
    
    # Print final result
    print("******************************")
    print("___MEMORY USAGE AFTER COMPLETION:___")
    mem_usg = df.memory_usage(deep=True).sum() / 1024**3 
    print("Memory usage is: ",round(mem_usg,2)," GB")
    print("This is a",round(100*(1-(mem_usg/start_mem_usg)),2),"% reduction in size")
    if len(NAlist) != 0:
        print("The following columns contain non-finite values and can be further optimized")
        print(NAlist)
    return df

def plotly_line(df,x_col,y_col,title,group_col = None,y_title = None,x_title = None,y2_col = None,y2_title = None,
                layout=None,return_fig = False):
    
    import plotly
    from plotly import __version__
    from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
    import plotly.plotly as py
    import plotly.graph_objs as go
    import collections
    import os
    import json
    
    init_notebook_mode(connected=True)
    
    def update(d, u):
        for k, v in u.items():
            if isinstance(v, collections.Mapping):
                d[k] = update(d.get(k, {}), v)
            else:
                d[k] = v
        return d
    
    plot_data = df
    if y_title == None:
        y_title = y_col
    if y2_title == None:
        y2_title = y2_col
    if x_title == None:
        x_title = x_col
        
    default_layout = dict(
        title = title,
        paper_bgcolor='rgb(240,240,240)',  # set paper (outside plot) 
        plot_bgcolor='rgb(240,240,240)', #   and plot color to grey
        xaxis = dict(title = x_title),
        yaxis = dict(title = y_title),
        colorway = ['#CC0000','#004153','#A3DBE8','#4C4C4C','#DCDCDC','#007A87','#80A0A9']
    )
        
    if layout != None:
        layout = update(default_layout,layout)
    else:
        layout = default_layout
                  
                  
    # Make Layout object

    

    data =[]
    if group_col != None:
        for group in plot_data[group_col].unique():
            data.append(
                go.Scatter(x=plot_data[plot_data[group_col] == group].sort_values(x_col)[x_col],
                           y=plot_data[plot_data[group_col] == group].sort_values(x_col)[y_col],
                           name = group)
                )
    elif y2_col != None:
        data.append(
            go.Scatter(x=plot_data[x_col],
                       y=plot_data[y_col],
                       name = y_col,
                      )
        )
        data.append(
            go.Scatter(x=plot_data[x_col],
                       y=plot_data[y2_col],
                       name = y2_col,
                       yaxis = 'y2'
                      )
        )
        layout['yaxis2'] = dict(title = y2_title,
                                overlaying='y',
                                side='right')
    else:
        data.append(
            go.Scatter(x=plot_data[x_col],
                       y=plot_data[y_col]
                      )
        )

    # Make Figure object
    fig = dict(data=data, layout=layout)
    
    if return_fig:
        return(fig)
    else:
        return(iplot(fig, filename=title))

def plotly_bar(df,x_col,y_col,title,group_col = None,y_title = None,x_title = None,barmode='stack',orientation='v',
              layout=None,return_fig=False,user=None):
    import plotly
    from plotly import __version__
    from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
    import plotly.plotly as py
    import plotly.graph_objs as go
#     plotly.tools.set_config_file(plotly_domain='http://plotly01-rhel7.rdu.redhat.com')
    import collections
    from ipywidgets import widgets
    import json
            

    init_notebook_mode(connected=True)
    def update(d, u):
        for k, v in u.items():
            if isinstance(v, collections.Mapping):
                d[k] = update(d.get(k, {}), v)
            else:
                d[k] = v
        return d
    plot_data = df
    if y_title == None:
        y_title = y_col
    if x_title == None:
        x_title = x_col
        
    # Make Layout object
    default_layout = dict(
        barmode=barmode,
        title = title,
        paper_bgcolor='rgb(240,240,240)',  # set paper (outside plot) 
        plot_bgcolor='rgb(240,240,240)', #   and plot color to grey
        xaxis = dict(title = x_title),
        yaxis = dict(title = y_title),
        colorway = ['#CC0000','#004153','#A3DBE8','#4C4C4C','#DCDCDC','#007A87','#80A0A9']
    )
        
    if layout != None:
        layout = update(default_layout,layout)
    else:
        layout = default_layout
        
        
    data =[]
    if group_col != None:
        for group in plot_data[group_col].unique():
            data.append(
                go.Bar(
                    x=plot_data[plot_data[group_col] == group][x_col],
                    y=plot_data[plot_data[group_col] == group][y_col],
                    orientation=orientation,
                    name = group)
                )
    else:
        data.append(
            go.Bar(
                x=plot_data[x_col],
                y=plot_data[y_col],
                orientation=orientation
            )
        )

    # Make Figure object
    fig = dict(data=data, layout=layout)
    
    if return_fig:
        return(fig)
    else:
        return(iplot(fig, filename=title))
