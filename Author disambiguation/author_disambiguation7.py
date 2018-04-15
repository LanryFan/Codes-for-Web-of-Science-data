# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 16:24:32 2017
@description:
Author disambiguation:
Set [uniaid] used [aid_raw11]

@author: Lanry Fan
"""

import pymysql

db=pymysql.connect('localhost','user','psw','dbname')
#step-1:Get [aid_raw11] list from [author_init2]
cursor=db.cursor()
sql='select distinct(aid_raw11) from author_init5 order by aid_raw11'
#Step-2:Set [uniaid] used [aid_raw11]
aid=0
try:
    cursor.execute(sql)
    rs1=cursor.fetchall()
    for row in rs1:
        print('正在处理第',aid+1,'条aid_raw11数据...')
        aid_raw=row[0]
        
        cursor_aid=db.cursor()
        sql_aid='update author_init5 set uniaid=%d\
            where aid_raw11=%d' %(aid+1,aid_raw)
        try:
            cursor_aid.execute(sql_aid)
            db.commit()        
        except Exception as e1:
            print('e1:',e1)
            db.rollback()
        aid+=1
except Exception as e:
    print('e:',e)

print('作者消歧完成！uniaid赋值完成！')
db.close()
