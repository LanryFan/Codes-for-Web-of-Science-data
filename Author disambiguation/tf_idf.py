# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 16:50:10 2017
@description:Compute the if-idf scores of the input text list

@author: Lanry Fan
"""
import math
class tfidf(object):
    def __init__(self,text=''):
        self.text=text
        self.words=set()
        self.dic={}
        self.doc_worddic={}
        self.docnum=0
        
        
    def addtext(self,text):
        self.docnum+=1
        mytext=text
        newwordlist=self.textclean(mytext)
        wordset=set()
        for w in newwordlist:
            self.words.add(w)
            wordset.add(w)
            if w in self.dic:
                self.dic[w]+=1
            else:
                self.dic[w]=1
        for w in wordset:
            if w in self.doc_worddic:
                self.doc_worddic[w]+=1
            else:
                self.doc_worddic[w]=1
        return newwordlist
    
    def textclean(self,text):
        mytext=text.lower()
        removestr=['||','!','@','#','$','%','^','&','*','(',')',\
            '_','-','+','=','`','~','[',']','{','}','\\',':','<',\
            '>','/','?',',']
        
        for rmstr in removestr:
            mytext=mytext.replace(rmstr,' ')
        mytext=' '.join(mytext.split())
        mytext=mytext.strip()
        mywordlist=mytext.split(' ')
        newwordlist=self.removeStopWords(mywordlist)
        return newwordlist
    
    def removeStopWords(self,wordlist):
        path='D:/python/eye_tracking/data/stoplist.txt'
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            try:
                stopwordlist=f.readlines()
                stopwordlist1=[]
                for w in stopwordlist:
                    w=w.replace(' \n','')
                    stopwordlist1.append(w)
                mywordlist=wordlist
                newwordlist=[]
                for w in mywordlist:
                    if w in stopwordlist1:
                        pass
                    else:
                        w1=w
                        newwordlist.append(w1)
            except Exception as e:
                print('Stop words remove error:',e)
        return newwordlist
        
    def idf_smooth(self, term):
        return math.log(1+float(self.docnum/self.doc_worddic[term]))      
        
    def get_cosine_tfidf(self,text1,text2):
        """TF-IDF score. Must specify a document id (within corpus) or pass text body."""
        mywordlist1=self.textclean(text1)
        mywordlist2=self.textclean(text2)
        wset1=set()
        wset2=set()
        wset=set()
        wdic1={}
        wdic2={}
        tfidf1={}
        tfidf2={}
        for w in mywordlist1:
            wset.add(w)
            wset1.add(w)
            if w in wdic1:
                wdic1[w]+=1
            else:
                wdic1[w]=1
        for w in mywordlist2:
            wset.add(w)
            wset2.add(w)
            if w in wdic2:
                wdic2[w]+=1
            else:
                wdic2[w]=1
        wlist=[]
        for w in wset:
            wlist.append(w)
        for w in wset1:
            idf1=self.idf_smooth(w)
            tf1=float(wdic1[w]/len(mywordlist1))
            tfidf1[w]=tf1*idf1
        for w in wset2:
            idf2=self.idf_smooth(w)
            tf2=float(wdic2[w]/len(mywordlist2))
            tfidf2[w]=tf2*idf2
        v1=[];v2=[]
        for w in wlist:
            if w in tfidf1:
                v1.append(tfidf1[w])
            else:
                v1.append(0)
            if w in tfidf2:
                v2.append(tfidf2[w])
            else:
                v2.append(0)
        #Compute the numerator(分子) and denominators(分母) of cosine similarit
        para_n=0;para_d1=0;para_d2=0
        length_v=len(v1)
        for k in range(length_v):
            para_n+=v1[k]*v2[k];
            para_d1+=v1[k]*v1[k]; para_d2+=v2[k]*v2[k]        
        para_d1=math.sqrt(para_d1); para_d2=math.sqrt(para_d2)
        para_d=para_d1*para_d2
        cosine_s=para_n/para_d
        return cosine_s
