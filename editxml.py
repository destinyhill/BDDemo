from xml.etree import ElementTree as ET
from decimal import *
import os
import math
import time
import sys

def ReadTxt(txt):
    ls =[]
    for line in open(txt):
        if line:
            ls.append(line.strip('\n').rstrip())               
    return ls

def CreateNode(tag,content):
    element = ET.Element(tag)
    element.text =content+"\n"
    return element 

def SaveOrig(path,file):
    print 'save the original %s now'%(file)
    all = path+"/"+file
    str_time = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    if not (os.path.isfile(all+".old")):
        cmd = "cp %s %s" %(all,all+".old")        
        os.system(cmd)
    elif not (os.path.isfile(all+"."+str_time)): 
        cmd = "cp %s %s" %(all,all+"."+str_time)
        os.system(cmd)
    else:
        print "file has saved\n"

def WriteXML(hadoop_conf,hasFile,has):
    tStr = time.strftime('%Y-%m-%d %H:%M:%S\n',time.localtime(time.time()))
    OpenTxt("CHANGE CONFIG:"+tStr)
    print has
    for key in hasFile:
        file = key
        dir = hadoop_conf+"/"+ file
        print dir
        try:
            tree = ET.parse(dir)
        except Exception, e:
            print "Error: Cannot parse file " + file
        finally:          
            tree = ET.parse(dir)
            nodes = tree.findall("property")
            s = file+'\n'
            OpenTxt(s)
            for key in has:
                if(key in hasFile[file]):
                    sr = key+':'+str(has[key])+'\n' 
                    OpenTxt(sr)
                    check = 0
                    for node in nodes:
                        if node.getchildren()[0].text == str(key) :
                            check = 1
                            node.getchildren()[1].text = str(has[key])
                    if not (check):
                        sNode = ET.Element("property")
                        sNode.text = "\n             "
                        child0 = ET.SubElement(sNode,'name')
                        child0.text = str(key)
                        child0.tail = "\n             "
                        child1 = ET.SubElement(sNode,'value')
                        child1.text = str(has[key])
                        child1.tail = "\n             "
                        describ = ET.SubElement(sNode,'describ')
                        describ.text = "create time:"+time.strftime('%Y-%m-%d',time.localtime(time.time()))
                        describ.tail = "\n             " 
                        sNode.tail = "\n             "
                        tree.getroot().append(sNode)
            tree.write(dir)
            ScpFile(dir,hadoop_conf)
    OpenTxt('\n')

def StopAll():
    hadoop_home = os.environ.get("HADOOP_HOME")
    print "Hi hadoop is closing"
    os.system(hadoop_home+"/sbin/stop-all.sh")

def StartAll():
    hadoop_home = os.environ.get("HADOOP_HOME")
    print "Hi hadoop is starting"
    os.system(hadoop_home+"/sbin/start-all.sh")

#def RefreshClu():
#    hadoop_home = os.environ.get("HADOOP_HOME")
#    print "Hi hadoop is refreshing"
#    os.system(hadoop_home+"/sbin/")

        
def ScpFile(dir,hadoop_conf):
    nodes = ReadTxt(hadoop_conf+"/slaves")
    for node in nodes:
        smd = "scp %s %s:%s/" % (dir,node,hadoop_conf)
        print smd 
        os.system(smd)
              
def RunDemo(demo,ch,wcDir,ioDir,maps,reduces):
    if(demo == "1"):
        result = OperaWordCount(ch,wcDir,reduces)
        if result:
            OpenTxt("WordCount failed\n")
        else:
            OpenTxt("WordCount successed\n") 
    if(demo == "2"):
        result = OperaTeraSort(ch,wcDir,maps,reduces)
        if result:
            OpenTxt("Terasort failed\n") 
        else:
            OpenTxt("Terasort successed\n")
    if(demo == "3"):
        result = OperaTeraSort(ch,wcDir,maps,reduces)
        if result:
            OpenTxt("Terasort failed\n")
        else:
            OpenTxt("Terasort successed\n")
            result = OperaWordCount(ch,wcDir,reduces)
            if result:
                OpenTxt("WordCount failed\n")
            else:
                OpenTxt("WordCount successed\n")
        
def GetFile(ls):
    has = {}
    name = ''
    for obj in ls:
        arry = []
        unit = obj.split()
        if(len(unit) == 1 and unit):
            arry = []
            has[unit[0]] = arry
            name = unit[0]
        elif(len(unit) >1 and unit):
            un = unit[0].split(',')
            for u in un:
               has[name].append(u)
    return has

def GetData(ls):
    has = {}
    for obj in ls:
        arry = []
        unit = obj.split()
        if(len(unit) > 3):
            try:
                un1 = int(unit[1])
                un2 = int(unit[2])
                un3 = int(unit[3])
            except ValueError:
                un1 = Decimal(unit[1])
                un2 = Decimal(unit[2])
                un3 = Decimal(unit[3])
            finally:
                nu = un1
                while(nu < un2):
                    arry.append(nu)
                    nu += un3
                arry.append(un2)
        elif(len(unit) == 3):
            arry.append(unit[1])
            arry.append(unit[2])
        elif(len(unit) == 2):
            arry.append(unit[1])

        if(len(unit) != 1 and unit):
            has[unit[0]] = arry
    return has
     

def Sort(has):
    total = 1
    for key in has:
        total = total * len(has[key])
    mul = total 
    ls = []
    ha = {}
    for key in has:
        mul = mul/len(has[key])
        block = []
        for obj in has[key]:
            for i in range(mul):
                block.append(obj)
        ha[key] = block
   
    for i in range(total):
        sa = {}
        j = i+1
        for key in ha:
            num = j % len(ha[key])
            if(num == 0):
                num = len(ha[key])
            k = key.split(',')
            for obj in k:
               sa[obj] = ha[key][num-1]
        ls.append(sa)
    return ls

def RandomTextWriter(ch,wcDir):
    print "Randomtextwriter is writing\n"
    f_size=2060
    fz = int(f_size)*1000000
    result = 1
    cTxtWrite = "%s jar %s randomtextwriter -D mapreduce.randomtextwriter.totalbytes=%d -D mapreduce.output.fileoutputformat.compress=false /randomtextwriter/input" %(ch,wcDir,fz)
    print cTxtWrite
    result = os.system(cTxtWrite)
    if result:
        print "There is an error in Randomtextwriter"
    return result
   
def TeraGen(ch,wcDir,maps):
    print "Teragen is writting\n"
    size = 21474840   # amount of the data is 2G 
    result = 0
    for map in maps:
        cTeragen = "%s jar %s teragen -D mapreduce.job.maps=%d %d /teragen/input/input%s"%(ch,wcDir,map,size,str(map))    
        print cTeragen
        result = os.system(cTeragen)
        if result:
            print "There is an error in Teragen"
            break
    return result

def CleanData(ch,demo):
    if(demo =="1"):
        comd = "%s fs -rmr /randomtextwriter"%(ch)
        os.system(comd)
        comd = "%s fs -rmr /wordcount"%(ch)
        os.system(comd)
    elif(demo =="2"):
        comd = "%s fs -rmr /teragen"%(ch)    
        os.system(comd)
        comd = "%s fs -rmr /terasort"%(ch)
        os.system(comd)
    elif(demo == "3"):
        comd = "%s fs -rmr /randomtextwriter"%(ch)
        os.system(comd)
        comd = "%s fs -rmr /wordcount"%(ch)
        os.system(comd)
        comd = "%s fs -rmr /teragen"%(ch)
        os.system(comd)
        comd = "%s fs -rmr /terasort"%(ch)
        os.system(comd)
    
def OperaProcess(hadoop_conf,demo,ch,wcDir,ioDir,hasFile,ha,maps,reduces):
    CleanData(ch,demo)
    la = Sort(ha)
    judge = 0
    #generate data
    if(demo =="1"):
        judge = RandomTextWriter(ch,wcDir)
    elif(demo == "2"):
        judge = TeraGen(ch,wcDir,maps)    
    elif(demo == "3"):
        judge = RandomTextWriter(ch,wcDir)
        if not judge:
            judge = TeraGen(ch,wcDir,maps)
     
    if not judge:
    #save original xml ,edit xml and run demo
        for key in hasFile:
            SaveOrig(hadoop_conf,key) 
        for ha in la:
            WriteXML(hadoop_conf,hasFile,ha)
            RunDemo(demo,ch,wcDir,ioDir,maps,reduces) 

def OperaTeraSort(ch,wcDir,maps,reduces):
    size = 21474840
    OpenTxt("Demo:Terasort ---- size:2GB")
    print "TeraSort is running\n"
    result = 1
    for map in maps:
        for reduce in reduces:
            print "Terasort is running\n"
            cTerasort = "%s jar %s terasort -D mapreduce.job.reduces=%d /teragen/input/input%s /terasort/output/output%s_%s"%(ch,wcDir,reduce,str(map),str(map),str(reduce))
            result = os.system(cTerasort)
    return result

def OperaWordCount(ch,wcDir,judge,reduces):
    f_size = 1024 #raw_input("input integer size(MB) of txt:\n")
    OpenTxt("Demo:WordCount ---- size:%sMB\n"%(f_size))    
    print "WordCount is running\n"
    result =1
    for reduce in reduces:
        cWordCount = "%s jar %s wordcount -D mapreduce.job.reduces=%d /randomtextwriter/input /wordcount/output%s" %(ch,wcDir,reduce,str(reduce))
        print cWordCount
        result = os.system(cWordCount)
    return result
    
def OperaDFSIOE(ch,ioDir):
    file_num = 10 #raw_input("input num of files:\n")
    file_size = 5 #raw_input("input size(GB) of per file:\n")
    if (file_num):
        #os.system (ch+ " fs -rmr /benchmarks/TestDFSIO/io_data")
        OpenTxt("Demo:DFSIO---- file num:%s     per file size:%sGB(write),%sGB(read)\n"%(file_num,file_size,file_size))
        wcomd = "%s jar %s TestDFSIO -write -nrFiles %s -size %sGB" %(ch,ioDir,file_num,"5")
        print wcomd
        os.system(wcomd)
        rcomd ="%s jar %s TestDFSIO -read -nrFiles %s -size %sGB" %(ch,ioDir,file_num,file_size)
        print rcomd
        return os.system (rcomd)
        
    else:
        os.system (ch+ " fs -rmr /benchmarks/TestDFSIO/io_data")
        OpenTxt("Demo:DFSIO---- file size:%sGB"%(file_size))
        wcomd = "%s jar %s TestDFSIO -write -size %sGB" %(ch,ioDir,file_size)
        print wcomd
        os.system(wcomd)
        rcomd = "%s jar %s TestDFSIO -read -size %sGB" %(ch,ioDir,file_size)
        print rcomd
        return os.system(rcomd)
     
def OpenTxt(content):
    f=open(r'log.txt','a')
    f.write(content)
    f.close()

def main():
    check = 0
    hadoop_home = os.environ.get("HADOOP_HOME")
    hadoop_conf = hadoop_home+"/etc/hadoop"
    wcDir = hadoop_home + "/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar"
    ioDir = hadoop_home + "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar"
    ch = hadoop_home+"/bin/hadoop --config "+hadoop_conf
   
    while(check == 0):
        demo =raw_input( "choose the demo: 1.wordcount 2.terasort 3.all 4.exit:  ") 
        maps =[]
        reduces =[]   
        ls = ReadTxt("data.txt")
        hasFile = GetFile(ls)
        ha = GetData(ls)
        if ha.has_key('mapreduce.job.maps'):
           maps = ha.pop('mapreduce.job.maps')
        if ha.has_key('mapreduce.job.reduces'):
           reduces = ha.pop('mapreduce.job.reduces')
        
        if not maps:
            maps=[4,8,12]
        if not reduces:    
            reduces=[6,8,12,16]
        print ha
        print maps
        print reduces
     
        if not (cmp(demo,"1")):
            check = 1
            OperaProcess(hadoop_conf,demo,ch,wcDir,ioDir,hasFile,ha,maps,reduces)
        if not (cmp(demo,"2")):
            check = 1
            OperaProcess(hadoop_conf,demo,ch,wcDir,ioDir,hasFile,ha,maps,reduces)
        if not (cmp(demo,"3")):
            check = 1
            OperaProcess(hadoop_conf,demo,ch,wcDir,ioDir,hasFile,ha,maps,reduces)
        if not (cmp(demo,"4")):
            sys.exit()
     
if __name__ == '__main__':
    main()
