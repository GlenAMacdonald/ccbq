'''
Created on Feb 19, 2018

@author: Spare
'''
from google.cloud import bigquery
import pandas as pd
import pandas_gbq as gbq
import Queue
import threading
import logging
import time
import datetime
import coinmarketcap as cmc
import schedule

market = cmc.Market()
    
def get500t(q,qout):
    args = q.get()
    # Reference API call: market.ticker('?start=0&limit=100')
    data = market.ticker('?start={0}&limit={1}'.format(args[0],args[1]))
    qout.put(data)
    q.task_done()
    return
    
def get500q():
    q500 = Queue.Queue()
    q500out = Queue.Queue()
    last = 499
    first = 0
    step = 100
    for i in range(first, last, step):
        start = i
        end = start + step - 1
        q500.put((start,end))
    for i in range(q500.qsize()):
        t = threading.Thread(target = get500t, args = (q500,q500out))
        t.start()
    q500.join()
    framelist = [pd.DataFrame(q500out.get())]
    for i in range(q500out.qsize()):
        framelist.append(pd.DataFrame(q500out.get()))
    dataFrame = pd.concat(framelist, ignore_index = True)
    return dataFrame
            
def trim500(fullDataFrame):
    #trim the dataFrame to contain on the columns wanted and rename the long ones
    # wanted columns: 24h_volume_usd, market_cap_usd, percent_change_24h, price_btc, price_usd, symbol
    trimmedDataFrame = fullDataFrame[['24h_volume_usd','market_cap_usd','price_btc','price_usd','percent_change_24h','symbol','name']].copy()
    trimmedDataFrame.columns = ['vol_24h', 'mkt_cap', 'price_btc','price_usd','change_24h','symbol','name']
    trimmedDataFrame[['vol_24h', 'mkt_cap', 'price_btc','price_usd','change_24h']] = trimmedDataFrame[['vol_24h', 'mkt_cap', 'price_btc','price_usd','change_24h']].apply(pd.to_numeric)
    #set the symbol row to be the index, commented out for the duplicate symbol check update
    #trimmedDataFrame.set_index('symbol', inplace = True)
    #give the dataframe a column for the current time (to later become the row key in bigquery)
    currenttimestr = "{:%Y-%m-%d %H:%M}".format(datetime.datetime.now())
    trimmedDataFrame['timestamp'] = pd.Series(currenttimestr,index=trimmedDataFrame.index)
    trimmedDataFrame.set_index('name',inplace = True)
    return trimmedDataFrame

def initiate_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler('/home/Spare/CC/cmc2bq/log/cmc2bq-{0}.log'.format(datetime.date.today()))
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def setloggerfilename():
    global logger
    handler = logging.FileHandler('/home/Spare/CC/cmc2bq/log/cmc2bq-{0}.log'.format(datetime.date.today()))
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.handlers = [handler]
    return

def define_table(bigquery_client, dataset_ref, table_id):
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)
    table.schema = (
        bigquery.SchemaField('vol_24h', 'float64'),
        bigquery.SchemaField('mkt_cap', 'float64'),
        bigquery.SchemaField('price_btc', 'float64'),
        bigquery.SchemaField('price_usd', 'float64'),
        bigquery.SchemaField('change_24h', 'float64'),
        bigquery.SchemaField('timestamp', 'timestamp'))
    return [table_ref, table]

def tlist_table_objects(tlist_DF, bigquery_client, dataset_ref):
    TableIDs = tlist_DF['TableID']
    tabledf = pd.DataFrame(index = TableIDs, columns = ['table_ref', 'table'])
    for i in range(len(TableIDs)):
        table_id = TableIDs[i]
        [table_ref, table] = define_table(bigquery_client, dataset_ref, table_id)
        tabledf.loc[table_id,'table_ref'] = table_ref
        tabledf.loc[table_id,'table'] = table
    tabledf.index = tlist_DF.index
    tlist_DF['table_ref'] = tabledf['table_ref']
    tlist_DF['table'] = tabledf['table']
    return tlist_DF

def goc_table_list(bigquery_client, dataset_ref, projectid): # Get or create table list - Table list that will store all tables..
    #Try and get the table list
    table_ref = dataset_ref.table('Table_List')
    try:
        tlist_BQ = bigquery_client.get_table(table_ref)
        tlist_DF = gbq.read_gbq('SELECT * FROM cmcdataset.Table_List', project_id = projectid)
        tlist_DF.set_index('name',inplace=True)
        tlist_DF = tlist_table_objects(tlist_DF, bigquery_client, dataset_ref)
    except Exception as e:
        if e.code == 404: # if the table is not found, then create it
            #define the table
            tlist_BQ = bigquery.Table(table_ref)
            tlist_BQ.schema = (
            bigquery.SchemaField('TableID', 'string'),
            bigquery.SchemaField('name', 'string'),
            bigquery.SchemaField('symbol', 'string'),
            bigquery.SchemaField('created', 'timestamp'))
            #create the table in BigQuery
            errors = bigquery_client.create_table(tlist_BQ)
            #create a local empty dataframe
            tlist_DF = pd.DataFrame(columns = ['TableID','name','symbol','created'])
            #set the index as name so that it doesn't change type to "Series" when a row is added.
            tlist_DF.set_index('name', inplace = True)
    return tlist_DF, tlist_BQ

def addrowtotlist(bigquery_client, ulq, logger):
    args = ulq.get()
    row = args[0]
    table = args[1]
    try:
        errors = bigquery_client.insert_rows(table,row)
    except Exception as e:
        print e
        logger.error('Error inserting row in Table_List for currency: {0}'.format(args[0].name))
        logger.error(e)
    ulq.task_done()
    return

def uploadtlistloop(bigquery_client, ulq, logger):
    while ulq.qsize() > 0:
        addrowtotlist(bigquery_client, ulq, logger)
        #print ulq.qsize()
    return

def uploadtlistq(bigquery_client, ulq, logger, numthreads):
    threads = []
    for i in range(numthreads):
        t = threading.Thread(target = uploadtlistloop, args = (bigquery_client,ulq, logger))
        threads.append(t)
    for i in threads:
        i.start()
    for i in threads:
        i.join()
    return

def align500(trimmedDF, tlist_DF,bigquery_client, tlist_BQ, dataset_ref, logger):
    ulq = Queue.Queue()
    trimmedDataFrame = trimmedDF.copy()
    trimmedDataFrame['TableID'] = pd.Series([''],index=trimmedDataFrame.index)
    for i in range(len(trimmedDataFrame)):
        #store the current index to make the code a bit neater - the index of the trimmed dataframe is the name of the currency
        currentindex = trimmedDataFrame.index.values[i]
        currentsym = trimmedDataFrame.iloc[i]['symbol']
        #print i
        #print currentindex
        try:
            #try to access the coin name in tlist_DF
            trimmedDataFrame.at[currentindex,'TableID'] = tlist_DF.at[currentindex,'TableID'] 
        except Exception as e:         
            #if it fails, then it doesn't exist, and needs to be added
            currenttimestr = "{:%Y-%m-%d %H:%M}".format(datetime.datetime.now())
            #Check if there's a TableID of the symbol for the current row.
            if not(currentsym in (tlist_DF.TableID).tolist()):  #There isn't a TableID with the same symbol - so write one.
                newrow = pd.DataFrame({'TableID' : currentsym, 'name' : currentindex,'symbol' : currentsym,'created':currenttimestr}, columns = ['TableID', 'name', 'symbol', 'created'], index = [0]) 
                ulq.put((newrow.values.tolist(),tlist_BQ))
                newrow.set_index('name', inplace = True) 
                [table_ref, table] = define_table(bigquery_client, dataset_ref, currentsym) #create the bigquery objects to store the tables
                newrow['table'] = table
                newrow['table_ref'] = table_ref
                tlist_DF = tlist_DF.append(newrow)
                trimmedDataFrame.at[currentindex,'TableID'] = tlist_DF.at[currentindex,'TableID']
            else: #there IS another TableID in tlist_DF with the same symbol.
                #Get entries where the TableID contains the symbol
                existingTables = (tlist_DF[tlist_DF.TableID.str.contains(currentsym)].TableID).tolist()
                #Sort them by symbol descending, get the first one (largest number) and increment it by one
                sortedIDs = sorted(existingTables, reverse = True)
                largestTableID = sortedIDs[0]
                num = largestTableID.replace(currentsym, '')
                try: # if the string is now empty there was only 1 entry, 
                    num = int(num) + 1
                except: # so the new tableID will be currentsym +1
                    num = 1
                #add the new number to the base symbol
                print 'Duplicate Symbol is ',currentsym
                newTableID = currentsym + str(num)
                #create dataframe and add to tlist_DF
                newrow = pd.DataFrame({'TableID' : currentsym, 'name' : currentindex,'symbol' : currentsym,'created':currenttimestr}, columns = ['TableID', 'name', 'symbol', 'created'], index = [0]) 
                #add to the queue of new entries for tlist and list of new tables to be created.
                ulq.put((newrow.values.tolist(),tlist_BQ))
                newrow.set_index('name', inplace = True)
                [table_ref, table] = define_table(bigquery_client, dataset_ref, newTableID)
                newrow['table'] = table
                newrow['table_ref'] = table_ref
                tlist_DF = tlist_DF.append(newrow)
                trimmedDataFrame.at[currentindex,'TableID'] = tlist_DF.at[currentindex,'TableID']
    if ulq.qsize() > 0:
        print 'Storing ',ulq.qsize(),' Table-IDs ',currenttimestr
    if ulq.qsize() > 10:
        if ulq.qsize() > 100:
            numthreads = 20
        else:
            numthreads = 5  
        uploadtlistq(bigquery_client,ulq,logger,numthreads)
    else:
        uploadtlistloop(bigquery_client, ulq, logger)
    return tlist_DF, trimmedDataFrame

def define_table_objects(data, bigquery_client, dataset_ref):
    TableIDs = data['TableID']
    tabledf = pd.DataFrame(index = TableIDs, columns = ['table_ref', 'table'])
    for i in range(len(TableIDs)):
        table_id = TableIDs[i]
        [table_ref, table] = define_table(bigquery_client, dataset_ref, table_id)
        tabledf.loc[table_id,'table_ref'] = table_ref
        tabledf.loc[table_id,'table'] = table
    return tabledf
    
def create1_table(bigquery_client, table, logger):
    errors = bigquery_client.create_table(table)
    return
    
def create_table(bigquery_client, table, args, logger):
    try: 
        table = bigquery_client.create_table(table)
        logger.info('Created table for symbol: {0}'.format(args[0]))
    except Exception as e:
        logger.exception(e)
        logger.info('{0}'.format(args))
    return

def uploadbq1(bigquery_client,ulq, logger):
    args = ulq.get()
    table = args[1]
    row = args[2]
    try:
        errors = bigquery_client.insert_rows(table,row)
    except Exception as e:
        logger.error('Problem inserting row for symbol: {0}'.format(args[0]))
        create_table(bigquery_client, table, args, logger)
    ulq.task_done()
    return

def uploadbqloop(bigquery_client, ulq, logger):
    while ulq.qsize() > 0:
        uploadbq1(bigquery_client, ulq, logger)
        #print ulq.qsize()
    return

def uploaddata_q(bigquery_client,data,tlist_DF,numthreads, logger):
    TableIDs = data['TableID'].copy()
    syms = data['symbol'].copy()
    data.drop(['TableID','symbol'], axis=1, inplace=True)
    ulq = Queue.Queue()
    length = data.shape[0]
    for i in range(length):
        row = data.iloc[[i]]
        TableName = row.index.values[0]
        TableID = TableIDs[TableName]
        table = tlist_DF.loc[TableName,'table']
        ulq.put((TableID,table,row.values.tolist()))
    currenttimestr = "{:%Y-%m-%d %H:%M}".format(datetime.datetime.now())
    print 'Storing ',ulq.qsize(),' coins', currenttimestr
    for i in range(numthreads):
        t = threading.Thread(target = uploadbqloop, args = (bigquery_client,ulq, logger))
        t.start()
    ulq.join()
    return

def runonce(bigquery_client, projectid, dataset_ref, tlist_DF, tlist_BQ, logger):
    df = get500q()
    df2 = trim500(df)
    [tlist_DF, df3] = align500(df2, tlist_DF, bigquery_client, tlist_BQ, dataset_ref, logger)
    numthreads = 20
    uploaddata_q(bigquery_client,df3,tlist_DF,numthreads,logger)
    return tlist_DF

def g_tick(period):
    t = time.time()
    count = 0
    while True:
        count += 1
        yield max(t+count*period - time.time(),0)

def runfor(bigquery_client, projectid, dataset_ref, logger, runs, xmin):
    [tlist_DF, tlist_BQ] = goc_table_list(bigquery_client, dataset_ref, projectid)
    setloggerfilename()
    g = g_tick(xmin*60)
    while runs > 0:
        tlist_DF = runonce(bigquery_client, projectid, dataset_ref, tlist_DF, tlist_BQ, logger)
        time.sleep(g.next())
        runs -= 1
    return

def schedulethread():
    while True:
        schedule.run_pending()
        time.sleep(3600)
    return

def runforever(bigquery_client, projectid, dataset_ref, logger, xmin):
    [tlist_DF, tlist_BQ] = goc_table_list(bigquery_client, dataset_ref, projectid)
    g = g_tick(xmin*60)
    schedule.every().monday.do(setloggerfilename)
    t=threading.Thread(target=schedulethread)
    while True:
        tlist_DF = runonce(bigquery_client, projectid, dataset_ref, tlist_DF, tlist_BQ, logger)
        time.sleep(g.next())
    return

#Initiate variables
projectid = "pythonproject-190701"
dataset_id = 'cmcdataset'
bigquery_client = bigquery.Client(project=projectid)
dataset_ref = bigquery_client.dataset(dataset_id)
#numthreads = 20
#initiate the Logger
logger = initiate_logger()
runforever(bigquery_client, projectid, dataset_ref, logger, 1)

