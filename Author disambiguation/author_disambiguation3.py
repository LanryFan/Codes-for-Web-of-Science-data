# -*- coding: utf-8 -*-
"""
Created on Thur Dec 14 22:24:32 2017
@description:
Author disambiguation use the name and the affiliation of authors.

@author: Lanry Fan
"""

import pymysql

db=pymysql.connect('localhost','user','psw','dbname')
'''
#Step-1:Initial [aid_raw2] use [aid_raw]
cursor_init=db.cursor()
sql_init='update author_init set aid_raw2=aid_raw'
try:
    cursor_init.execute(sql_init)
    db.commit()
except Exception as e0:
    print('e0:',e0)
    db.rollback()
'''
#step-2:Get lname list from [author_init]
cursor=db.cursor()
sql='select count(xuhao),lname from author_init group by lname'
#用[fname,mname,fname_initial,mname_initial,affiliation]精确匹配
data=[]
counter=0
try:
    cursor.execute(sql)
    rs1=cursor.fetchall()
    for row in rs1:
        print('正在处理第',counter+1,'条姓名数据...')
        number=row[0]
        lname=row[1]
        #Step-3:Get the name information list use the lname
        cursor_disab=db.cursor()
        sql_disab='select xuhao,aid_raw2,fname,mname,fname_initial,mname_initial,\
            affiliation from author_init\
            where lname="%s"' %(lname)
        try:
            cursor_disab.execute(sql_disab)
            rs2=cursor_disab.fetchall()
            lname=lname.replace('||','')
            data_disab=[]
            xuhao=0;aid=0;fname='';mname=''
            fname_ini='';mname_ini=''
            affiliation=''
            for row2 in rs2:
                xuhao=row2[0]
                aid=row2[1]
                fname=row2[2]
                mname=row2[3]
                fname_ini=row2[4]
                mname_ini=row2[5]
                affiliation=row2[6]
                if fname is None:
                    fname=''
                if mname is None:
                    mname=''
                if fname_ini is None:
                    fname_ini=''
                if mname_ini is None:
                    mname_ini=''
                if affiliation is None:
                    affiliation=''
                removestr=['||','/','\\','[',']','"',"'"]
                for rmstr in removestr:
                    fname=fname.replace(rmstr,'')
                    mname=mname.replace(rmstr,'')
                    fname_ini=fname_ini.replace(rmstr,'')
                    mname_ini=mname_ini.replace(rmstr,'')
                fname=fname.strip()
                mname=mname.strip()
                fname_ini=fname_ini.strip()
                mname_ini=mname_ini.strip()
                affiliation=affiliation.replace('.','').replace('&',' ')
                affiliation=affiliation.replace('||',' ').strip()
                affiliation=' '.join(affiliation.split())
                affiliation=affiliation.lower()
                data_disab.append([xuhao,aid,fname,mname,fname_ini,mname_ini,affiliation])
            length=len(data_disab)
            #Step-4:Author disambiguation
            #More than one record
            if length>1:
                for i in range(length-1):
                    for j in range(i+1,length):
                        #Only if the two have different aid,then decide disambiguation
                        if data_disab[i][1]!=data_disab[j][1]:
                            #If the two both have the [fname]
                            if data_disab[i][2]!='' and data_disab[j][2]!='':
                                #If the two both have the [mname],use[fname,mname]
                                if data_disab[i][3]!='' and data_disab[j][3]!='':
                                    #If two [fname,mname] are the same
                                    if data_disab[i][2]==data_disab[j][2] and data_disab[i][3]==data_disab[j][3]:
                                        #IF two [affiliation] are the same,decide them as one author                                    
                                        if data_disab[i][6]==data_disab[j][6]:
                                            aid1=data_disab[i][1]
                                            aid2=data_disab[j][1]
                                            minaid=min(aid1,aid2)
                                            for item in data_disab:
                                                if item[1]==aid1 or item[1]==aid2:
                                                    item[1]=minaid
                                        else:
                                            pass
                                #Either A or B haven't the [mname],use[fname]
                                else:
                                    #If two [fname] are the same
                                    if data_disab[i][2]==data_disab[j][2]:
                                        #IF two [affiliation] are the same,decide them as one author                                    
                                        if data_disab[i][6]==data_disab[j][6]:
                                            aid1=data_disab[i][1]
                                            aid2=data_disab[j][1]
                                            minaid=min(aid1,aid2)
                                            for item in data_disab:
                                                if item[1]==aid1 or item[1]==aid2:
                                                    item[1]=minaid
                                        else:
                                            pass
                            #Either A or B haven't the [fname]
                            else:
                                #If the two both have the [fname_ini]
                                if data_disab[i][4]!='' and data_disab[j][4]!='':
                                    #If the two both have the [mname_ini],use[fname_ini,mname_ini]
                                    if data_disab[i][5]!='' and data_disab[j][5]!='':
                                        #If two [fname_ini,mname_ini] are the same
                                        if data_disab[i][4]==data_disab[j][4] and data_disab[i][5]==data_disab[j][5]:
                                            #IF two [affiliation] are the same,decide them as one author                                    
                                            if data_disab[i][6]==data_disab[j][6]:
                                                aid1=data_disab[i][1]
                                                aid2=data_disab[j][1]
                                                minaid=min(aid1,aid2)
                                                for item in data_disab:
                                                    if item[1]==aid1 or item[1]==aid2:
                                                        item[1]=minaid
                                            else:
                                                pass
                                    #Either A or B haven't the [mname_ini],use[fname_ini]
                                    else:
                                        #If two [fname_ini] are the same
                                        if data_disab[i][4]==data_disab[j][4]:
                                            #IF two [affiliation] are the same,decide them as one author                                    
                                            if data_disab[i][6]==data_disab[j][6]:
                                                aid1=data_disab[i][1]
                                                aid2=data_disab[j][1]
                                                minaid=min(aid1,aid2)
                                                for item in data_disab:
                                                    if item[1]==aid1 or item[1]==aid2:
                                                        item[1]=minaid
                                            else:
                                                pass
                                #Either A or B haven't the [fname_ini],decide them as two individual authors
                                else:
                                    pass        
                for row3 in data_disab:
                    data.append([row3[0],row3[1],row3[2],row3[3],row3[4],row3[5],row3[6]])
            #Only one record
            else:
                #data.append([xuhao,aid,fname,mname,fname_ini,mname_ini,affiliation])
                pass
        except Exception as e1:
            print('e1:',e1)
        counter+=1
except Exception as e:
    print('e:',e)

print('数据处理完成，等待写入数据库...')
length_data=len(data)
counter1=0
try:
    for row in data:
        cursor_update=db.cursor()
        sql_update='update author_init set aid_raw2=%d\
            where xuhao=%d' % (row[1],row[0])
        try:
            print('共',length_data,'条记录，正在插入第',counter1+1,'条记录...')
            cursor_update.execute(sql_update)
            db.commit()
        except Exception as e2:
            db.rollback()
            print('e2:',e2)
        counter1+=1
except Exception as e0:
    print('e0:',e0)

print('第三轮匹配完成！')
db.close()
