import xml.dom.minidom as MDOM
import MySQLdb
import httplib
import urllib2
import os

def IfExist(JobID,host,user,passwd):       
        conn = MySQLdb.connect(host,user,passwd,"jobhistory")
        cur = conn.cursor()
        sql = "select avgMapTime from jobcount where id = '%s'" % (JobID)
        cur.execute(sql)
        results = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        if(results):
            return 1
        else: 
            return 0

def get_nodevalue(node,index= 0):
        return node.childNodes[index].nodeValue if node else ''

def get_xmlnode(node,name):
        return node.getElementsByTagName(name) if node else []

def GetCountData(host,port,JobID):
        headers = {"Accept":"application/xml"}
        httpClient = httplib.HTTPConnection(host,port,timeout = 30)
        httpClient.request('GET',"/ws/v1/history/mapreduce/jobs/"+JobID+"/counters","",headers)
        response = httpClient.getresponse()
        value = response.read()
       # print value
        responsexml=MDOM.parseString(value)
        httpClient.close()
        user_nodes = get_xmlnode(responsexml,'counter')
        user_list = []
        list = []
        for node in user_nodes:
            node_name=get_xmlnode(node,'name')
            node_total_value=get_xmlnode(node,'totalCounterValue')
            user_name=(get_nodevalue(node_name[0])).encode("utf-8","ignore")
            user_total_value=int(get_nodevalue(node_total_value[0]))
            if not (user_name in list):
                list.append(user_name)
                user={}
                user['name'],user['totalCounterValue']= (
                    user_name,user_total_value
                )
                user_list.append(user)
        return user_list

def GetConfData(host,port,JobID):
        headers = {"Accept":"application/xml"}
        httpClient = httplib.HTTPConnection(host,port,timeout = 30)
        httpClient.request('GET',"/ws/v1/history/mapreduce/jobs/"+JobID+"/conf","",headers)
        response = httpClient.getresponse()
        value = response.read()
       # print value
        responsexml=MDOM.parseString(value)
        httpClient.close()
        user_nodes = get_xmlnode(responsexml,'property')
        user_list = []
        list = ReadTxt("col.txt")
        for node in user_nodes:
            node_name=get_xmlnode(node,'name')
            node_value=get_xmlnode(node,'value')
            user_name=(get_nodevalue(node_name[0])).encode("utf-8","ignore").replace('.','_').replace('-','_')
            user_value=(get_nodevalue(node_value[0])).encode("utf-8","ignore")
            if(user_name in list):
                user={}
                user['name'],user['value']= (
                    user_name,user_value
                )
                user_list.append(user)
        return user_list

def GetWebData(host,port,JobID,txt):
        headers = {"Accept":"application/xml"}
        httpClient = httplib.HTTPConnection(host,port,timeout = 30)
        httpClient.request('GET',"/ws/v1/history/mapreduce/jobs/"+JobID,"",headers)
        response = httpClient.getresponse()
        value = response.read()
        responsexml=MDOM.parseString(value)
        httpClient.close()
        
        nNode = get_xmlnode(responsexml,'name')
        startNode = get_xmlnode(responsexml,'startTime')
        finishNode = get_xmlnode(responsexml,'finishTime')
        mapNode = get_xmlnode(responsexml,'avgMapTime')
        reduceNode = get_xmlnode(responsexml,'avgReduceTime')
        shuffleNode = get_xmlnode(responsexml,'avgShuffleTime')
        mergeNode = get_xmlnode(responsexml,'avgMergeTime')
        statNode = get_xmlnode(responsexml,'state')

        name = get_nodevalue(nNode[0])
        startTime = get_nodevalue(startNode[0])
        finishTime = get_nodevalue(finishNode[0])
        elapsed = (long(finishTime) - long(startTime))/1000
        avgMapTime = long(get_nodevalue(mapNode[0]))/1000
        avgReduceTime = long(get_nodevalue(reduceNode[0]))/1000
        avgShuffleTime = long(get_nodevalue(shuffleNode[0]))/1000
        avgMergeTime = long(get_nodevalue(mergeNode[0]))/1000
        stat = get_nodevalue(statNode[0])
           
        if(str(stat) == 'SUCCEEDED'):
            colValue = "'"+JobID +"','"+str(name)+"',"+str(elapsed)+","+str(avgMapTime)+","+str(avgReduceTime)+","+str(avgShuffleTime)+","+str(avgMergeTime)
            colName = "id,name,Elapsed,avgMapTime,avgReduceTime,avgShuffleTime,avgMergeTime"
            count_list =  GetCountData(host,port,JobID)
            conf_list = GetConfData(host,port,JobID)
            col_count = []
            list0 = ReadTxt(txt)
            for user in count_list:
                if (user['name'] in list0):
                    colValue += ","+str(user['totalCounterValue'])
                    colName  += ","+user['name']
            col_count.append(colName)
            col_count.append(colValue)      
        
            colValue="'"+JobID+"'"
            colName="id"
            col_conf = []
            list1 = ReadTxt("col.txt")
            for user in conf_list:
                if(user['name'].replace('.','_'.replace('-','_')) in list1):
                    colName += ","+user['name']
                    colValue += ",'"+str(user['value'])+"'"
            col_conf.append(colName)
            col_conf.append(colValue)
        
            col ={}
            col['count'],col['conf']=(
                col_count,col_conf
            )
 
            return col
        else:
            return 0

def GetJobsID(host,port):
    AllJobsID=[]
    headers={"Accept":"application/xml"}
    httpClient=httplib.HTTPConnection(host,port,timeout=30)
    httpClient.request('GET','/ws/v1/history/mapreduce/jobs',"",headers)
    response=httpClient.getresponse()
    value=response.read()
    responsexml=MDOM.parseString(value)
    ApplicationID=get_xmlnode(responsexml,'id')
    httpClient.close()
    for member in ApplicationID:
        mm = get_nodevalue(member)
        AllJobsID.append(mm)
    return AllJobsID

def ReadTxt(txt):
    list =[]
    for line in open(txt):
       list.append(line.strip('\n').rstrip().replace('.','_').replace('-','_'))
    return list

def CreSql(txt):
    creSql = "create table jobcount(id varchar(30),name varchar(200),Elapsed int,avgMapTime int,avgReduceTime int,avgShuffleTime int,avgMergeTime int"
    list0 = ReadTxt(txt)
    for obj in list0: 
       creSql += "," + obj +" bigint"

    creSql +=");create table jobconf(id varchar(30)"
    list1 = ReadTxt("col.txt")
    for obj in list1:
        creSql += ","+obj+" varchar(30)"
    creSql += ")"
    return creSql

def inTxt(host,port,JobID):
    user_list = GetCountData(host,port,JobID)
    unit=""        
    for user in user_list:
        unit+=str(user['name'])+"\n"
    file_object = open('config.txt','w')
    file_object.write(unit)
    file_object.close()

def CreateDB(host,user,passwd,txt):
    try:
        conn = MySQLdb.connect(host,user,passwd)
        cur = conn.cursor()

        sql = "create database if not exists jobhistory"
        cur.execute(sql)
        conn.select_db("jobhistory")

        check="show tables"
        cur.execute(check)
        results = cur.fetchall()
        if not (results):
            creSql = CreSql(txt).split(';')
            cur.execute(creSql[0])
            cur.execute(creSql[1])
    except MySQLdb.Error, e:
            print "Mysql Error %d: %s" %(e.args[0],e.args[1])
    finally:
        conn.commit()
        cur.close()
        conn.close()
        

def colSql():
    user_list = GetCountData(host,port,JobID)

def InsertDB(host,user,passwd,col):
        try:
            conn = MySQLdb.connect(host,user,passwd)
            cur = conn.cursor()
            conn.select_db("jobhistory")
            inSql = "insert into jobcount ("+col["count"][0]+") VALUES ("+ col["count"][1] +")"
            cur.execute(inSql)
            inSql = "insert into jobconf ("+col["conf"][0]+") VALUES ("+ col["conf"][1] +")"
            cur.execute(inSql)
        except MySQLdb.Error, e:
            print "Mysql Error %d: %s" %(e.args[0],e.args[1])
           
        finally:
            print "insert successfully"
            conn.commit()
            cur.close()
            conn.close()
 

def main():
    jobHost = "node0"
    port = 19888
    sqlHost = "127.0.0.1"
    user = "root"
    passwd = ""
    txt = "config.txt"    
       
    if not (os.path.isfile(txt)):
        inTxt(jobHost,port,"job_1474670277840_0001")    
    else:
        print "txt exist"

    AllJobsID = GetJobsID(jobHost,port)     
    CreateDB(sqlHost,user,passwd,txt)

    for member in AllJobsID:
        if not IfExist(member,sqlHost,user,passwd):
            print member+" "+"No Exist"
            col =  GetWebData(jobHost,port,member,txt)
            if(col): 
                InsertDB(sqlHost,user,passwd,col)
            else:
                print 'the job failed'

if __name__ == '__main__':
    main()
