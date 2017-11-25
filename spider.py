import sqlite3
import urllib.error
import ssl
import bs4
import re
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup
from array import array

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

#cur.execute('''DROP TABLE IF EXISTS Entries''')
#cur.execute('''DROP TABLE IF EXISTS Offender_Information''')

cur.execute('''CREATE TABLE IF NOT EXISTS Entries
            (entry_id INTEGER PRIMARY KEY, entry_title TEXT, creation_date TEXT, creation_time TEXT,
            content TEXT)''')

def process_entry(url):
    regex_id = re.compile('nr=(.*)\&')
    id = regex_id.findall(url)
    
    cur.execute('''SELECT * FROM Entries WHERE entry_id = ?''', (int(id[0]),))
    row = cur.fetchone()
    if row != None:
        print("BREAK!")
        return
    
    
    url = "https://www.presseportal.de/text/"+str(url)
    html = urllib.request.urlopen(url, context=ctx).read()
    html = html.decode('windows-1252')

    regex_title = re.compile('\<div class="story_title"\>\<h1\>POL-BN: ([\s\S]*?)\<\/h1\>\<\/div\>')
    regex_date = re.compile('\n(.*?) \&ndash;')
    regex_time = re.compile('\&ndash; (.*?)\&nbsp;,')
    regex_content = re.compile('\<p\>(.*?)\<pre class="xml_contact"\>')
    
    
    
    title = regex_title.findall(html)
    date = regex_date.findall(html)
    time = regex_time.findall(html)
    content = regex_content.findall(html)
    
    print(id)
    print(title)
    print(date)
    print(time)
    print(content)
    
    
    cur.execute('''INSERT OR IGNORE INTO Entries (entry_id, entry_title, creation_date, creation_time, content) VALUES ( ?, ?, ?, ?, ?)''',
                                                    (int(id[0]), str(title[0]), str(date[0]),str(time[0]),str(content[0])))
    conn.commit()


def process_last_words(url, execution_id):
    print('lastwords ',execution_id)
    if (url[-3:] != "jpg"):
        url = "http://www.tdcj.state.tx.us/death_row/"+str(url)
        html = urllib.request.urlopen(url, context=ctx).read()
    
        last_row_data = []
        prog = re.compile("Last Statement:.*\<\/p\>.*\<p\>(.*)\<\/p")
        prog_alt = re.compile("Last Statement:.*\<\/span\> \<\/p\>.*\<p\>(.*)\<\/p")
        last_words_data = prog.findall(str(html))
        if len(last_words_data) == 0:
            last_words_data = prog_alt.findall(str(html))
            
        print(last_words_data)
        cur.execute('''INSERT OR IGNORE INTO Offender_Last_Words (id, lastwords) VALUES ( ?, ?)''',
                                                        (execution_id, last_words_data[0]))
        conn.commit()
        
        

url = "https://www.presseportal.de/text/p_story.htx?firmaid=7304"
next_url = "https://www.presseportal.de/text/p_story.htx?firmaid=7304"
regex = re.compile('\<a href="((?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)" class="contentheader"\>')
regex_next = re.compile('\<a href="\/blaulicht.*?start=([0-9]*)"\>weiter\<\/a\>')

while True == True:
    html = urllib.request.urlopen(url, context=ctx).read()
    entries = regex.findall(str(html))
    print(regex_next.findall(str(html)))
    next_url = "https://www.presseportal.de/text/p_story.htx?firmaid=7304&start="+regex_next.findall(str(html))[0]
    print(next_url)
    if next_url == []:
        break
    for url in entries:
        process_entry(url)
    url = next_url




cur.close()
