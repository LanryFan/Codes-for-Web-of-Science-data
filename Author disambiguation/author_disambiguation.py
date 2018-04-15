# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 16:24:32 2017
@description:
Update [author_init] set [aid_raw] for authors.
For the same author in an article,share the same aid_raw.

@author: Lanry Fan
"""

import pymysql

db=pymysql.connect('localhost','user','psw','dbname')
#step-1:Get articleID list from [author_init]
cursor_article=db.cursor()
sql_article='select distinct(articleID) from author_init order by articleID'

aid=0
try:
    cursor_article.execute(sql_article)
    rs1=cursor_article.fetchall()
    for row in rs1:
        articleID=row[0]
        print('正在处理第',articleID,'篇文章...')
        #step-2:Get information by articleID from [author_init]
        cursor_info=db.cursor()
        sql_info='select xuhao,rank from author_init where articleID=%d' % (articleID)
        cursor_info.execute(sql_info)
        rs2=cursor_info.fetchall()
        xuhao_rank=[]
        for row1 in rs2:
            xuhao=row1[0]
            rank=row1[1]
            xuhao_rank.append([xuhao,rank,0])#List of [xuhao,rank,aid]
        max_rank=max(x[1] for x in xuhao_rank)
        for i in range(len(xuhao_rank)):
            aid_temp=aid+xuhao_rank[i][1]
            xuhao_rank[i][2]=aid_temp
            #step-3:Update table[author_init] set [aid_raw]
            cursor_update=db.cursor()
            sql_update='update author_init set aid_raw=%d where xuhao=%d' % (xuhao_rank[i][2],xuhao_rank[i][0])
            try:
                cursor_update.execute(sql_update)
                db.commit()
            except Exception as e1:
                print(e1)
                db.rollback()
        #Update aid
        aid+=max_rank
        
except Exception as e:
    print(e)

print('第一轮处理完成！')
db.close()
