# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 14:12:53 2018

@author: Lanry Fan
"""

import requests
import pymysql
import time
import random
from retrying import retry
from random import choice
from bs4 import BeautifulSoup



def pause_some_time(a,b):
    """
    定义进行随机等待的函数

    :param a: --- int，随机值（randint）的下限
    :param b: --- int，随机值（randint）的上限
    :return: None
    """
    sec = random.random() * random.randint(a, b)
    print('\t\t\t睡眠', sec, '秒')
    time.sleep(sec)
    return


def get_sid(session):
    """
    定义获取SID的函数，通过访问http://www.webofknowledge.com/，
    可以在URL或者COOKIES中提取所需的SID

    :param session: --- requests.Session，用来维持一个SID为本函数所获得的值的会话
    :return sid: --- str，所获得的SID
    """
    session.get('http://www.webofknowledge.com',timeout=25)
    sid = session.cookies['SID'].replace('"', '')
    print('\t获得SID：', sid)
    return sid


# 进行搜索
def get_search_result(session, sid, anum):
    """
    定义进行搜索的函数，接收搜索的关键字，并返回检索结果。
    默认检索的字段是Web of Science所有数据库的作者AND机构（AU AND AD），用来检索一个作者的发文信息
    如需检索其他字段，请将search_formdata中value(select1)的值换成想要检索字段的简称，
    如标题是TI
    目前直接使用search_formdata中的value(input1)进行表单提交
    
    如无必要，请勿改动header，search_headers仅包含了必要的字段，并已经通过了爬取测试

    :param session: --- requests.Session，用来维持一个SID为本函数所获得的值的会话
    :param sid: --- str,与session对应的SID
    :param anum: --- str，检索关键字，目前为Web of Science所有数据库的作者AND机构
    @ex:AU=Deng, Shujie AND AD=Bournemouth Univ'
    :return search_result: --- request.Response，检索结果页面的响应内容
    """
    search_url = 'http://apps.webofknowledge.com/WOS_GeneralSearch.do'

    def get_search_header(sid, anum):
        """
        定义构造检索时所需Headers和表单的函数，表单具体字段意义基本同其名字

        :param sid:  --- str,父函数的SID
        :param anum:  --- str，父函数的检索关键字，目前为Web of Science所有数据库的作者AND机构
        :return search_headers, search_formdata:  --- tuple(dic,dic)，[0]为所需的Header，[1]为需要提交的表单
        """
        search_headers = {
            'User-Agent': User_Agent,
            'Referer': 'http://apps.webofknowledge.com/WOS_GeneralSearch_input.do?product=WOS&SID=' + sid + '&search_mode=GeneralSearch',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        search_formdata = {
            'fieldCount': 1,
            'action': 'search',
            'product': 'WOS',
            'search_mode': 'AdvancedSearch',
            'SID': sid,
            'max_field_count': 25,
            'max_field_notice': '注意: 无法添加另一字段。',
            'input_invalid_notice': '检索错误: 请输入检索词。',
            'exp_notice': '检索错误: 专利检索词可在多个家族中找到',
            'input_invalid_notice_limits': ' <br/>注: 滚动框中显示的字段必须至少与一个其他检索字段相组配。',
            'sa_params': 'WOS||' + sid + '|http://apps.webofknowledge.com|',
            'formUpdated': 'TRUE',
            'value(input1)': anum,#'AU=Deng, Shujie AND AD=Bournemouth Univ',#anum,
            #'value(select1)': 'AU AND AD',
            'value(hidInput1)': '',
            'limitStatus': 'collapsed',
            'ss_lemmatization': 'On',
            'ss_spellchecking': 'Suggest',
            'SinceLastVisit_UTC': '',
            'SinceLastVisit_DATE': '',
            'period': 'Range Selection',
            'range': 'ALL',
            'startYear': '1900',
            'endYear': '2018',
            'update_back2search_link_param': 'yes',
            'ssStatus': 'display:none',
            'ss_showsuggestions': 'ON',
            'ss_query_language': 'auto',
            'ss_numDefaultGeneralSearchFields': 1,
            'rs_sort_by': 'PY.A;LD.A;SO.A;VL.A;PG.A;AU.A',#按时间正序
            #PY.A;LD.A;SO.A;VL.A;PG.A;AU.A
        }
        return search_headers, search_formdata

    headers, formdata = get_search_header(sid, anum)
    print('\t\t正在获取关键字为', anum, '的检索结果页面')
    time.sleep(random.random())
    search_result = session.post(search_url, data=formdata, headers=headers,timeout=25)
    print('\t\t检索结果页面状态码及链接：', search_result.status_code, search_result.url)
    return search_result
# 访问检索结果页面
def get_pubYear_result(search_result):
    """
    定义根据搜索结果获得最早发文信息的函数
    :param search_result: --- request.Response，检索结果页面的响应内容
    :return minPubYear: --- int，最早发文时间
    """

    def get_minPubYear(search_result):
        """
        根据检索结果的响应找到最早发文时间的函数

        :param search_result: --- request.Response，检索结果页面的响应内容
        :return minPubYear: --- int，最早发文时间
        """
        soup = BeautifulSoup(search_result.text, 'lxml')
        recode1 = soup.find(id='RECORD_1')
        content = recode1.find('div',attrs={"class":"search-results-content"})
        info = content.find_all('value')
        #link = content.find('div',attrs={"class":"search-action-item"})
        minPubTime = info[-1].text
        minPubYear=int(minPubTime.split(" ")[-1].strip())
        print('\t\t最早发文时间：', minPubYear)
        return minPubYear
    minPubYear = get_minPubYear(search_result)
    return minPubYear

def get_query_info():
    """
    从数据库中取出每个aid对应的[aid,author_fullName,affiliation]，根据自己数据库的结构进行修改
    
    :return: None
    """
    db=pymysql.connect('localhost','root','1234567890','eye_tracking_new')
    cursor=db.cursor()
    sql='select distinct(uniaid) from author_init5 where uniaid<=5000 \
        and uniaid not in (select distinct aid from first_pub_info_01) order by uniaid'

    counter=0
    try:
        cursor.execute(sql)
        rs=cursor.fetchall()
        for row in rs:
            
            aid=row[0]
            print('正在处理第',counter+1,'条记录...aid:'+str(aid))
            cursor_aid=db.cursor()
            sql_aid='select author_fullName,affiliation from author_init5 \
                where uniaid=%d' % (aid)
            try:
                cursor_aid.execute(sql_aid)
                rs2=cursor_aid.fetchall()
                namecoll=''
                afficoll=''
                for row2 in rs2:
                    name=row2[0]
                    affiliation=row2[1]
                    if name is None:
                        name=''
                    if affiliation is None:
                        affiliation=''
                    affiliation=affiliation.replace('||',' ').replace('[',' ').replace(']',' ').strip()
                    if affiliation!='':
                        affilist=[affi.strip() for affi in affiliation.split(',')[:2]]
                        affi='("'+affilist[0]+'" SAME "'+affilist[1]+'")'
                        afficoll+=affi+' OR '
                    if name!='':
                        namecoll+=name+' OR '
                if namecoll!='':
                    namecoll=namecoll[:-4]
                if afficoll!='':
                    afficoll=afficoll[:-4]
                if namecoll!='' and afficoll!='':
                    query='AU=('+namecoll+') AND AD=('+afficoll+')'
                    data.append([aid,query])
                    #data.append([aid,namecoll,afficoll])
            except Exception as e1:
                print(e1)
            counter+=1
    except Exception as e:
        print(e)
    print('数据处理完成！等待爬取...')
    db.close()
    return

@retry(wait_fixed=5000,stop_max_attempt_number=2)
def get_one_query_pubYear_result(query):
    """
    整合函数，完成获取一个作者最早发文时间的工作
    每一环节都允许失败重试一次，若再次失败则调用retry等待5秒后再重试
    ！！！这部分代码不够健壮，需要改进，若网站由于某些原因访问不了可能会浪费很多时间

    :param doc_tuple: --- list(str,str)，数据库中的字段，根据自己的数据库格式修改，最重要的是“检索”字段，其他字段主要用作文件命名格式
    :return: aid,minPubYear
    """
    global SESSION
    global SID
    global User_Agent
    global t_init

    if(time.time()-t_init>1700):
        SESSION = requests.session()
        SID = get_sid(SESSION)
        User_Agent = choice(UA)
        print('此SID和SESSION已使用接近30分钟，更换为新SID：',SID)
        t_init=time.time()

    print('使用SID：',SID)

    aid = query[0]
    anum = query[1]

    try:
        search_result = get_search_result(SESSION, SID, anum)
    except:
        print('出错！')
        """
        print('出错，等待后重试')
        pause_some_time(1,2)
        search_result = get_search_result(SESSION, SID, anum)
        """
    try:
        minPubYear = get_pubYear_result(search_result)
        print(str(aid),'-',str(minPubYear))
    except:
        return -1,-1
        """
        print('出错，等待后重试')
        pause_some_time(1,2)
        try:
            minPubYear = get_pubYear_result(search_result)
            print(str(aid),'-',str(minPubYear))
        except:
            return -1,-1
        """

    return aid,minPubYear



def mainprocess(query_list):
    """
    包装一下上面的整合函数，加入时间统计

    :param doc: --- tuple(str,str)，数据库中的字段，根据自己的数据库格式修改，最重要的是UT字段，其他字段主要用作文件命名格式
    :return: None
    """
    t0 = time.time()
    yearInfo=[]
    len_data=len(query_list)
    counter=0#计数器
    for query in query_list:
        try:
            minPubYear=0
            print('共',len_data,'条数据，正在爬取第',counter+1,'条数据...aid=',str(query[0]))
            aid,minPubYear = get_one_query_pubYear_result(query)
            print('本条用时', time.time() - t0, '秒')
            pause_some_time(1,3)
            print(str(aid) + '-' + str(minPubYear))
            if aid!=-1:
                yearInfo.append([aid,minPubYear])
        except Exception as e:
            print(e)
        counter+=1
    return yearInfo

UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36 OPR/37.0.2178.32',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 BIDUBrowser/8.3 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.9.2.1000 Chrome/39.0.2146.0 Safari/537.36',
    'Mozilla/5.0 (compatible; Googlebot/2.1; +https://www.google.com/bot.html)',
)

SESSION = requests.session()
SID = get_sid(SESSION)
User_Agent = choice(UA)
t_init=time.time()
data=[]


"""
anum='AU=Deng, Shujie AND AD=Bournemouth Univ'
search_result = get_search_result(SESSION, SID, anum)
pubYear_result = get_pubYear_result(SESSION, search_result)
"""

if __name__=='__main__':
    t1 = time.time()
    get_query_info()
    #Insert into table
    db=pymysql.connect('localhost','user','psw','dbname')
    cursor=db.cursor()
    length=len(data)
    print('共',length,'条待爬取数据，准备爬取...')
    for i in range(0,length,50):
        if i%50==0:
            pause_some_time(3,5)
        if i+50<length:
            print('正在爬取',i+1,'-',i+50,'条记录...')
            Info=mainprocess(data[i:i+50])
        elif i+50>=length:
            Info=mainprocess(data[i:length])
            print('正在爬取最后几条记录...')
    
        len_info=len(Info)
        counter1=0
        for row in Info:
            print('共',len_info,'条待插入数据，正在插入第',counter1+1,'条数据...')
            cursor_insert=db.cursor()
            sql_insert='insert into first_pub_info_01(aid,minPubYear)\
                values(%d,%d)' % (row[0],row[1])
            try:
                cursor_insert.execute(sql_insert)
                db.commit()
            except Exception as e:
                print(e)
                db.rollback()
            counter1+=1
    db.close()
    print('数据处理完成！')
