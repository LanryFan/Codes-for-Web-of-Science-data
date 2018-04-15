# -*- coding: utf-8 -*-
"""
Created on Sat Dec 16 14:56:43 2017

@author: Lanry Fan
"""

import pymysql
class co_citation(object):
    def __init__(self):
        self.aid1=0
        self.aid2=0
    def is_co_citation(self,aid1,aid2):
        self.aid1=aid1
        self.aid2=aid2
        article1={}#doi dict that aid1 publish,like{articleID:doi}
        article2={}#doi dict that aid2 publish,like{articleID:doi}
        pubset1=set()#paperr doi set that aid1 publish
        pubset2=set()#paperr doi set that aid2 publish
        citedset1=set()#doi set of the paper that aid1 cited
        citedset2=set()#doi set of the paper that aid2 cited
        causet1=set()#author set of the paper that aid1 cited
        causet2=set()#author set of the paper that aid2 cited
        citeddic1={}#doi dict of the paper that aid1 cited,like{xuhao:doi}
        citeddic2={}#doi dict of the paper that aid2 cited,like{xuhao:doi}
        caudic1={}#author dict of the paper that aid1 cited,like{xuhao:author name}
        caudic2={}#author dict of the paper that aid2 cited,like{xuhao:author name} 
        cxuhaolist1=[]#xuhao list of the paper that aid1 cited
        cxuhaolist2=[]#xuhao list of the paper that aid2 cited
        name1=set()#aid1's name
        name2=set()#aid2's name
        db=pymysql.connect('localhost','user','psw','dbname')
        #The list of articles that each publish
        cursor_ar1=db.cursor()
        cursor_ar2=db.cursor()
        sql_ar1='select distinct a.articleID,b.DI from author_init a,article_diff_init b\
            where a.articleID=b.articleID and a.aid_raw9=%d' % (aid1)
        sql_ar2='select distinct a.articleID,b.DI from author_init a,article_diff_init b\
            where a.articleID=b.articleID and a.aid_raw9=%d' % (aid2)
        try:
            cursor_ar1.execute(sql_ar1)
            rs_ar1=cursor_ar1.fetchall()
            for ar1 in rs_ar1:
                articleID1=ar1[0]
                pdoi1=ar1[1]
                if pdoi1 is None:
                    pdoi1=''
                if pdoi1!='':
                    pubset1.add(pdoi1)
                article1[articleID1]=pdoi1
        except Exception as e:
            print(e)
        try:
            cursor_ar2.execute(sql_ar2)
            rs_ar2=cursor_ar2.fetchall()
            for ar2 in rs_ar2:
                articleID2=ar2[0]
                pdoi2=ar2[1]
                if pdoi2 is None:
                    pdoi2=''
                if pdoi2!='':
                    pubset2.add(pdoi2)
                article2[articleID2]=pdoi2
        except Exception as e:
            print(e)
        #The list of articles that they cited
        for key in article1:
            cursor_ci1=db.cursor()
            sql_ci1='select xuhao,author,doi from cited_references where articleID=%d' % (key)
            try:
                cursor_ci1.execute(sql_ci1)
                rs_ci1=cursor_ci1.fetchall()
                for ci1 in rs_ci1:
                    xuhao1=ci1[0]
                    au1=ci1[1]                    
                    cdoi1=ci1[2]
                    cxuhaolist1.append(xuhao1)
                    if au1 is None:
                        au1=''
                    au1=au1.replace('.','').replace(' ','').strip().lower()
                    if cdoi1 is None:
                        cdoi1=''
                    if cdoi1!='':
                        citedset1.add(cdoi1)
                        citeddic1[xuhao1]=cdoi1
                    if au1!='':
                        causet1.add(au1)
                        caudic1[xuhao1]=au1
            except Exception as e:
                print(e)
        for key in article2:
            cursor_ci2=db.cursor()
            sql_ci2='select xuhao,author,doi from cited_references where articleID=%d' % (key)
            try:
                cursor_ci2.execute(sql_ci2)
                rs_ci2=cursor_ci2.fetchall()
                for ci2 in rs_ci2:
                    xuhao2=ci2[0]
                    au2=ci2[1]                    
                    cdoi2=ci2[2]
                    cxuhaolist2.append(xuhao2)
                    if au2 is None:
                        au2=''
                    au2=au2.replace('.','').replace(' ','').strip().lower()
                    if cdoi2 is None:
                        cdoi2=''
                    if cdoi2!='':
                        citedset2.add(cdoi2)
                        citeddic2[xuhao2]=cdoi2
                    if au2!='':
                        causet2.add(au2)
                        caudic2[xuhao2]=au2
            except Exception as e:
                print(e)
        #Get the name of author via [author] of table[author_init]
        cursor_name1=db.cursor()
        sql_name1='select distinct(author) from author_init where aid_raw9=%d' % (aid1)
        try:
            cursor_name1.execute(sql_name1)
            rs1=cursor_name1.fetchall()
            for row1 in rs1:
                au1=row1[0]
                if row1 is None:
                    au1=''
                if au1!='':
                    au1=au1.replace(',','').replace('.','').replace(' ','').strip().lower()
                    name1.add(au1)
        except Exception as e1:
            print(e1)
        cursor_name2=db.cursor()
        sql_name2='select distinct(author) from author_init where aid_raw9=%d' % (aid2)
        try:
            cursor_name2.execute(sql_name2)
            rs2=cursor_name2.fetchall()
            for row2 in rs2:
                au2=row2[0]
                if row2 is None:
                    au2=''
                if au2!='':
                    au2=au2.replace(',','').replace('.','').replace(' ','').strip().lower()
                    name2.add(au2)
        except Exception as e2:
            print(e2)
        
        tag1=False#Whether aid1 cited aid2
        tag2=False#Whether aid2 cited aid1
        for xh in cxuhaolist1:
            if xh in citeddic1:
                doi=citeddic1[xh]
                if doi in pubset2:
                    tag1=True
                    break
            else:
                if xh in caudic1:
                    au=caudic1[xh]
                    if au in name2:
                        tag1=True
                        break
        for xh in cxuhaolist2:
            if xh in citeddic2:
                doi=citeddic2[xh]
                if doi in pubset1:
                    tag2=True
                    break
            else:
                if xh in caudic2:
                    au=caudic2[xh]
                    if au in name1:
                        tag2=True
                        break
        tag=False
        if tag1 and tag2:
            tag=True
        return tag
