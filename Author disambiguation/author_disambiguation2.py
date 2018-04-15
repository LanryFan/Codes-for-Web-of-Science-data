# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 16:24:32 2017
@description:
Author disambiguation use the name and the affiliation of authors.

@author: Lanry Fan
"""

import re
import pymysql

db=pymysql.connect('localhost','user','psw','dbname')
#step-1:Get information list from [author_init]
cursor=db.cursor()
sql='select xuhao,author,author_fullName,affiliation from author_init\
    order by xuhao'

data=[]
errorid=[]
try:
    cursor.execute(sql)
    rs1=cursor.fetchall()
    for row in rs1:
        try:
            
            xuhao=row[0]        
            print('正在处理第',xuhao,'条记录...')      
            author=row[1]
            author_fullName=row[2]
            affiliation=row[3]
            
            lname,fmname_ini=author.split(',')
            lname=lname.strip()
            fmname_ini=fmname_ini.strip()
            fname_ini=''
            mname_ini=''
            fname=''
            mname=''
            index_lname=0
            #Only first name initial
            if len(fmname_ini)==1:
                fname_ini=fmname_ini
            else:
                fname_ini=fmname_ini[0]
                mname_ini=fmname_ini[1:]
            #author_fullName.replace('.','').strip()
            name=re.split('[, ]',author_fullName.replace('.','').strip())
            try:
                for i in range(len(name)):
                    name.remove('')#去掉所有空格
            except Exception as e1:
                pass
            try:
                index_lname=name.index(lname)
                if index_lname==0:
                    fname=name[1]
                    if len(name)>2:
                        for i in range(2,len(name)):
                            mname+=name[i]
                    else:
                        pass
                elif index_lname>0:
                    if len(name)>index_lname+1:
                        for i in range(0,index_lname):
                            mname+=name[i]
                        fname=name[index_lname+1:]
                    elif len(name)==index_lname+1 and len(name)>=3:
                        fname=name[0]
                        for i in range(1,index_lname):
                            mname=name[i]
                    elif len(name)==index_lname+1 and len(name)<3:
                        fname=name[0]
            except Exception as e2:
                print('e2:',e2)
                try:
                    name1=[n1.strip() for n1 in author_fullName.replace('.','').split(',')]
                    index_lname=name1.index(lname)
                    if index_lname==0:
                        name2=name1[1].split(' ')
                        fname=name2[0]
                        if len(name2)==1:
                            pass
                        else:
                            for i in range(1,len(name2)):
                                mname+=name2[i]
                    else:
                        name2=name1[0].split(' ')
                        fname=name2[0]
                        if len(name2)==1:
                            pass
                        else:
                            for i in range(1,len(name2)):
                                mname+=name2[i] 
                except Exception as e3:
                    print('e3:',e3)
                    try:
                        name3=[n3.strip() for n3 in author_fullName.replace('.','').split(',')]
                        fname=name3[1]
                    except Exception as e4:
                        print('e4:',e4)
                        errorid.append(xuhao)
                        pass
            firstini_last_name=fname_ini+'_'+lname
            country=[affi.strip() for affi in affiliation.split(',')][-1]
            data.append([xuhao,fname,mname,lname,fname_ini,mname_ini,firstini_last_name,country])
        except Exception as e5:
            print('e5:',e5)
            errorid.append(xuhao)
            pass
except Exception as e:
    print('e:',e)
    errorid.append(xuhao)
print('数据处理完成，等待写入数据库...')

try:
    for row in data:
        cursor_update=db.cursor()
        sql_update='update author_init set fname="%s",mname="%s",lname="%s",\
            fname_initial="%s",mname_initial="%s",firstinilastname="%s",country="%s"\
            where xuhao=%d' % (row[1],row[2],row[3],row[4],row[5],row[6],\
            row[7],row[0])
        try:
            print('正在插入第',row[0],'条记录...')
            cursor_update.execute(sql_update)
            db.commit()
        except Exception as e1:
            db.rollback()
            print('e1:',e1)
except Exception as e:
    print('e:',e)

print('第二轮处理完成！')
db.close()

print('开始写入出错数据...')
path = 'E:/EyeTracking/Data/20171214/errordata.txt'
length=len(errorid)
with open(path, 'w', encoding='utf-8', errors='ignore') as f:
    try:
        print('共有',length,'条错误记录...')
        errortext=''
        for i in range(length):
            errortext+=(str(errorid[i])+'\n')
        f.write(errortext) 
    except Exception as e:
        print('e',':',e)            

print('出错数据写入完成！')
