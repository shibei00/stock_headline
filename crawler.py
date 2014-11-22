#-*- coding:utf-8 -*-

import httplib2
import urllib
import json
import traceback
import bs4
from bs4 import BeautifulSoup
import datetime
import MySQLdb

headline_pre_url = 'http://finance.yahoo.com/q/h?'
base_filefolder = r'./data/'

conn = MySQLdb.connect(host='seis10.se.cuhk.edu.hk', user='bshi', db='bshi', passwd='20141031shib', charset='utf8')

def urlencode(base, param):
    param_code = urllib.urlencode(param)
    rurl = base + param_code
    return rurl

def get_response(url):
    head, content = httplib2.Http().request(url)
    return head, content

def get_news_mainpage():
    mainpage_head, mainpage_content = get_response(mainpage_url)
    return mainpage_head, mainpage_content

def read_file(file_name):
    f = open(file_name)
    l_list = f.readlines()
    f.close()
    return l_list

def write_file(file_name, content):
    f = open(file_name, 'w')
    f.write(content)
    f.close()

def crawl_headlines(start_url, stockname):
    try:
        head, content = httplib2.Http().request(start_url)
        soup = BeautifulSoup(content)
        a_list = soup.find_all('a', {'href':True,})
        next_url = ''
        for a in a_list:
            if a.find(text='Older Headlines'):
                next_url = a['href']
        if next_url:
            headline_div = soup.find_all('div', {'class':'mod yfi_quote_headline withsky'})[0]
            t_datetime = None
            for item in headline_div.children:
                if item.name == 'h3':
                    t_datetime = datetime.datetime.strptime(item.span.text, '%A, %B %d, %Y')
                if item.name == 'ul' and t_datetime:
                    for headline in item.children:
                        title = headline.a.text
                        link = headline.a['href']
                        nhead, ncontent = httplib2.Http().request(link)

                        import uuid
                        file_name = str(uuid.uuid1()) + '.html'
                        write_file(base_filefolder + file_name, ncontent)
                        cur = conn.cursor()
                        cur.execute("insert into stock_headline(link, filename, title, timestamp, stockname) values(%s, %s, %s, %s, %s)",(link, file_name, title, t_datetime, stockname,))
                        cur.close()

            next_url = 'http://finance.yahoo.com' + next_url
            print next_url
            conn.commit()
            crawl_headlines(next_url, stockname)
        else:
            return
    except:
        traceback.print_exc()
    
if __name__=='__main__':
    t_now = datetime.datetime.now()
    t_str = t_now.strftime('%Y-%m-%d')
    stock_list = read_file('stock.txt')
    for st in stock_list:
        t_list = st.split(',')
        s_name = t_list[0]
        c_name = t_list[1]
        param_dict = {'s':s_name, 't':t_str}
        start_url = urlencode(headline_pre_url, param_dict)
        print start_url
        crawl_headlines(start_url, s_name)
    conn.close()
