# -*- coding: utf-8 -*-
"""
Created on Sun Dec 10 13:09:44 2017

@author: Lanry Fan
"""

import pymysql

db=pymysql.connect('localhost','user','psw','dbname')
cursor=db.cursor()
sql='select articleID,AU,AF,C1,PY from article_diff_init'
data=[]
something=[]#记录地址中出现的名字没有包括在作者名字中记录的articleID(一般为团体作者，少数情况)
try:
    cursor.execute(sql)
    results=cursor.fetchall()
    for row in results:
        articleID=row[0]
        authors=[name.strip() for name in row[1].split('||')]
        authors_fullName=[name.strip() for name in row[2].split('||')]
        authorNum=max(len(authors),len(authors_fullName))
        addrs=row[3]
        pubYear=row[4]
        print('正在处理第',articleID,'篇文章...')
        if '[' in addrs:
            info=['['+info.strip() for info in addrs.split('[')[1:]]
            for metaInfo in info:
               names,addr=metaInfo.split(']')
               names,addr=names.replace('||',' ').strip(),addr.replace('||',' ').strip()
               names=names[1:].strip()
               namelist=[i.strip() for i in names.split(';')]
               for fullName in namelist:                       
                   if fullName in authors_fullName:
                       index=authors_fullName.index(fullName)
                       name=authors[index]
                       data.append([articleID,authorNum,name,fullName,index+1,addr,pubYear])
                   else:
                       something.append(articleID)
        elif '[' not in addrs:       
            addrlist=[i.strip()+'.' for i in addrs.split('.')[:-1]]
            for i in range(authorNum):
                rank=i+1
                for addr in addrlist:
                    data.append([articleID,authorNum,authors[i],authors_fullName[i],rank,addr,pubYear])
except Exception as e:
    print(e)

print('data length is ',str(len(data)))

try:
    counter=0
    for row in data:
        cursor_insert=db.cursor()
        print('正在插入第',str(counter+1),'条数据...')
        sql_insert='insert into author_init\
            (articleID,authorNum,author,author_fullName,rank,affiliation,pubYear)\
            values(%d,%d,"%s","%s",%d,"%s",%d)' % (row[0],row[1],row[2],row[3],row[4],row[5],row[6]) 
        # 执行sql语句
        cursor_insert.execute(sql_insert)
        # 提交到数据库执行
        db.commit()
        counter+=1
except Exception as e:
    print(e)

print('数据插入完成...')
db.close()
print('数据库连接关闭...')        
