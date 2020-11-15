# _*_ coding: utf-8 _*_

import sqlite3
import json
import re

DATAFILE="/data/baike/baike_triples.txt"
DBFILE="/data/baike/baike.db"
conn =sqlite3.connect(DBFILE)
c = conn.cursor()
try:
    c.execute("drop table baike")
except:
    ...

c.execute("CREATE TABLE baike (	term TEXT NOT NULL,	json TEXT);")
c.execute("CREATE INDEX baike_term_IDX ON baike (term);")


pattern = re.compile(r'<[^>]+>',re.S)

all = list()
with  open(DATAFILE,"r") as f:
#    for i in range(100):
#        all.append(f.readline())
    all=f.readlines()






allitem={}
bNew=False
key=""
item={}
value={}
TAG=[]
for line in all:
    #print(line)
    a=line.split()
    
    
    curKey=a[0].strip()
   
    
    if( key != curKey ):
        if( key.strip() != "" ):
            if(TAG):
                value["BaiduTAG"] = TAG

            str=json.dumps(value) 
            allitem[key]=str
                     
            item={}
            value={}
            TAG=[]
        key = curKey

    subkey=a[1]

    result=pattern.sub('', a[2])
    if(subkey == "BaiduTAG"):
        TAG.append(result)
    else:
        value[subkey]=result


# store the last one

if( key.strip() != "" ):
    if(TAG):
        value["BaiduTAG"] = TAG
    
    item[key]=value
    str=json.dumps(value) 
    allitem[key]=str
                     
    item={}
    value={}
    TAG=[]
    


#  term json
Items=allitem.items()
for key, value in Items:
    data=(key,value)
    
    sql ="insert into baike ( term,json) values(?,?)"
    c.execute(sql,data)
'''
cursor= c.execute("select * from baike")

for row in cursor:
    print(row[0])
'''



conn.commit()
conn.close()






