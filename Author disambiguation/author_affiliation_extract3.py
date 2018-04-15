# -*- coding: utf-8 -*-
"""
Created on Thur Dec 28 09:16:14 2017
@Description:Extracting the missing record of [author,author_fullName] from [article_diff_init]
into [author_init2]

@author: Lanry Fan
"""

import pymysql
db=pymysql.connect('localhost','user','psw','dbname')
#Step-1:Cheating [author_init] to find the missing author record
cursor_article=db.cursor()
sql_article='select count(aid) AS tempnumber, articleID, authorNum,\
    (authorNum-count(aid))as miss  from author_init2 GROUP BY articleID \
    having tempnumber<authorNum'
data_article=''
try:
    print('正在查找[author_init]中出现缺失的文章记录...')
    cursor_article.execute(sql_article)
    rs=cursor_article.fetchall()
    for row in rs:
        articleID=row[1]
        data_article+=(str(articleID)+',')
except Exception as e:
    print(e)
print('缺失文章查找完毕，等待处理...')

#Step-2:Cheating [article_diff_init] to find the missing article record
cursor_article2=db.cursor()
sql_article2='select articleID from article_diff_init where articleID not in(\
    select distinct(articleID) from author_init2)'
try:
    print('正在查找[article_diff_init]中出现缺失的文章记录...')
    cursor_article2.execute(sql_article2)
    rs=cursor_article2.fetchall()
    for row in rs:
        articleID=row[0]
        data_article+=(str(articleID)+',')
    data_article=data_article[:-1]
except Exception as e:
    print(e)

#Step-3:Extracting the author info
cursor=db.cursor()
sql='select articleID,AU,AF,C1,PY from article_diff_init where articleID in('\
    +data_article+')'
data=[]
something=[]#记录地址中出现的名字没有包括在作者名字中记录的articleID(一般为团体作者，少数情况)
counter1=0
try:
    cursor.execute(sql)
    results=cursor.fetchall()
    for row in results:
        articleID=row[0]
        author_raw=row[1]
        authors=[name.strip() for name in author_raw.split('||')]
        authors_fullName=[name.strip() for name in row[2].split('||')]
        authorNum=max(len(authors),len(authors_fullName))
        addrs=row[3]
        pubYear=row[4]
        print('正在处理第',counter1+1,'篇文章...')
        #如果地址不是空的，且有作者
        if author_raw!='' and addrs!='':
            #如果有对应所有作者的地址
            if addrs.strip()[0]!='[':
                if '[' in addrs:
                    info=[info.strip() for info in addrs.strip().split('[')[:1]]
                    for addrInfo in info:
                       addr=addrInfo.replace('||',' ').strip()
                       for fullName in authors_fullName:
                           index=authors_fullName.index(fullName)
                           name=authors[index]
                           data.append([articleID,authorNum,name,fullName,index+1,addr,pubYear])
            else:
                info=['['+info.strip() for info in addrs.split('[')[1:]]
                nameset=set()
                for metaInfo in info:
                   names,addr=metaInfo.split(']')
                   names,addr=names.replace('||',' ').strip(),addr.replace('||',' ').strip()
                   names=names[1:].strip()
                   namelist=[i.strip() for i in names.split(';')]
                   for name in namelist:
                       nameset.add(name)
                for fullName in authors_fullName:                       
                    if fullName not in namelist:
                       index=authors_fullName.index(fullName)
                       name=authors[index]
                       data.append([articleID,authorNum,name,fullName,index+1,'',pubYear])
        #如果地址是空的，且有作者
        elif author_raw!='' and addrs=='':
            for fullName in authors_fullName:
                index=authors_fullName.index(fullName)
                name=authors[index]
                data.append([articleID,authorNum,name,fullName,index+1,'',pubYear])
        counter1+=1
except Exception as e:
    print(e)

print('data length is ',str(len(data)))

try:
    counter=0
    for row in data:
        cursor_insert=db.cursor()
        print('正在插入第',str(counter+1),'条数据...')
        sql_insert='insert into author_init2\
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
