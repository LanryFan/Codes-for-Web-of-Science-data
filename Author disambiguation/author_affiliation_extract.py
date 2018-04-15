# -*- coding: utf-8 -*-
"""
Created on Sun Dec 10 09:46:14 2017

@author: Lanry Fan
"""

import pymysql

db=pymysql.connect('localhost','user','psw','dbname')
cursor=db.cursor()
sql='select articleID,AF,C1 from article_diff_init'
data=[]
try:
    cursor.execute(sql)
    results=cursor.fetchall()
    for row in results:
        articleID=row[0]
        authors=[name.strip() for name in row[1].split('||')]
        addrs=row[2]
        if '[' in addrs:
            info=['['+info.strip() for info in addrs.split('[')[1:]]
            for metaInfo in info:
                names,addr=metaInfo.split(']')
                names,addr=names.replace('||',' ').strip(),addr.replace('||',' ').strip()
                names=names[1:].strip()
                namelist=[i.strip() for i in names.split(';')]
                for name in namelist:
                    data.append([articleID,name,addr])
        elif '[' not in addrs:
            addrlist=[i.strip()+'.' for i in addrs.split('.')[:-1]]
            for author in authors:
                for addr in addrlist:
                    data.append([articleID,author,addr])
except Exception as e:
    print(e)

print('data length is ',str(len(data)))

try:
    for row in data:
        cursor_insert=db.cursor()
        sql_insert='insert into author_init\
            (articleID,author,affiliation)\
            values(%d,"%s","%s")' % (row[0],row[1],row[2]) 
        # 执行sql语句
        cursor_insert.execute(sql_insert)
        # 提交到数据库执行
        db.commit()
except Exception as e:
    print(e)

db.close()
        
