# -*- coding: utf-8 -*-
"""
Created on Sat Dec 16 09:24:10 2017
@Description:This class used to see if the two authors have the same co_author

@author: Lanry Fan
"""

import pymysql

class co_author(object):
    def __init__(self):
        self.aid1=0
        self.aid2=0
    def has_coauthor(self,aid1,aid2):
        self.aid1=aid1
        self.aid2=aid2
        db=pymysql.connect('localhost','user','psw','dbname')
        cursor1=db.cursor()
        cursor2=db.cursor()
        sql1='select distinct(articleID) from author_init where\
            aid_raw8=%d' % (aid1)
        sql2='select distinct(articleID) from author_init where\
            aid_raw8=%d' % (aid2)
        try:
            cursor1.execute(sql1)
            cursor2.execute(sql2)
            rs1=cursor1.fetchall()
            rs2=cursor2.fetchall()
            articleID1=[]
            articleID2=[]
            aidset1=set()
            aidset2=set()
            for row1 in rs1:
                articleID1.append(row1)
                cursor_aid1=db.cursor()
                sql_aid1='select distinct(aid_raw8) from author_init\
                    where articleID=%d' % (row1)
                try:
                    cursor_aid1.execute(sql_aid1)
                    rs_aid1=cursor_aid1.fetchall()
                    for row_aid1 in rs_aid1:
                        if row_aid1!=self.aid1:
                            aidset1.add(row_aid1)
                except Exception as e1:
                    print('Error in getting aid through aid1',e1)
            for row2 in rs2:
                articleID2.append(row2)
                cursor_aid2=db.cursor()
                sql_aid2='select distinct(aid_raw8) from author_init\
                    where articleID=%d' % (row2)
                try:
                    cursor_aid2.execute(sql_aid2)
                    rs_aid2=cursor_aid2.fetchall()
                    for row_aid2 in rs_aid2:
                        if row_aid2!=self.aid2:
                            aidset2.add(row_aid2)
                except Exception as e2:
                    print('Error in getting aid through aid2',e2)
            tag=False
            for aid in aidset1:
                if aid in aidset2:
                    tag=True
                    break
            db.close()
            return tag
        except Exception as e:
            print('Matching error:',e)
                
            
