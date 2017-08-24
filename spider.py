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

#cur.execute('''DROP TABLE IF EXISTS Convicts''')
#cur.execute('''DROP TABLE IF EXISTS Offender_Information''')

cur.execute('''CREATE TABLE IF NOT EXISTS Convicts
            (execution_id INTEGER PRIMARY KEY, last_name TEXT,
            first_name TEXT, TDCJ_number TEXT, age INTEGER, ex_date TEXT,
            race TEXT, county TEXT)''')
            
cur.execute('''CREATE TABLE IF NOT EXISTS Offender_Information
            (id Integer, TDCJ_number TEXT, birth_date TEXT, age_rcv INTEGER, education_lvl INTEGER,
             age_at_offense INTEGER, offense_date TEXT, rcv_date TEXT, gender TEXT, hair_color TEXT, height TEXT,
             weight TEXT,eye_color TEXT, native_county TEXT, native_state TEXT,
             prior_occupation TEXT, prior_prison_record TEXT, inc_summary TEXT,
             co_defendants TEXT, race_gender_victim TEXT)''')
             
cur.execute('''CREATE TABLE IF NOT EXISTS Offender_Last_Words
            (id INTEGER, lastwords TEXT)''')

def process_offender_info(url, execution_id):
    if (url[-3:] != "jpg") and (str(url) != "dr_info/no_info_available.html"):
        url = "http://www.tdcj.state.tx.us/death_row/"+str(url)
        html = urllib.request.urlopen(url, context=ctx).read()
    
        opp_soup = BeautifulSoup(html, 'html.parser')
        opp_rows = opp_soup.find_all('tr')
        opp_row_data = []
        
        for opp_row in opp_rows:
            field_counter = 0
            for opp_field in opp_row.children:
                #print("Test"+str(opp_field))
                field_counter += 1
                if isinstance(opp_field, bs4.element.Tag) and opp_field.get('class') == ['tabledata_align_left_deathrow']:
                    opp_row_data.append(opp_field.string)
        
        #print("string: " + str(match_string))
        
        prog1 = re.compile("\<br[\s]\/\>(.*?)\<\/p>")
        prog2 = re.compile("\<br\/\>(.*?)\<\/p>")
        prog3 = re.compile('tabledata_deathrow_table"\>(.*?)\<\/p\>')
        page_bottom_data = prog1.findall(str(html))
        if len(page_bottom_data) == 0:
            page_bottom_data = prog2.findall(str(html))
        if len(page_bottom_data) == 0:
            page_bottom_data = prog3.findall(str(html))
        
        for item in page_bottom_data:
            opp_row_data.append(re.sub(r"[\r\n]+"," ",item.strip()))
        
        TDCJ_number = opp_row_data[1]
        birth_date = opp_row_data[2]
        rcv_date = opp_row_data[3]
        age_rcv = opp_row_data[4]
        education_lvl = opp_row_data[5]
        offense_date = opp_row_data[6]
        age_at_offense = opp_row_data[7]
        county = opp_row_data[8]
        gender = opp_row_data[10]
        hair_color = opp_row_data[11]
        height = opp_row_data[12]
        weight = opp_row_data[13]
        eye_color = opp_row_data[14]
        native_county = opp_row_data[15]
        native_state = opp_row_data[16]
        if len(page_bottom_data) == 5:
            prior_occupation = opp_row_data[17]
            prior_prison_record = opp_row_data[18]
            inc_summary = opp_row_data[19]
            co_defendants = opp_row_data[20]
            race_gender_victim = opp_row_data[21]
        else:
            prior_occupation = ""
            prior_prison_record = opp_row_data[17]
            inc_summary = opp_row_data[18]
            co_defendants = opp_row_data[19]
            race_gender_victim = opp_row_data[20]
         
        
        cur.execute('''INSERT OR IGNORE INTO Offender_Information (id, TDCJ_number, birth_date, rcv_date, age_at_offense, age_rcv,education_lvl,
                                                        offense_date, gender, hair_color, height, weight, eye_color, native_county,
                                                        native_state, prior_occupation, prior_prison_record, inc_summary,
                                                        co_defendants, race_gender_victim) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?
                                                        , ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                                        (execution_id, TDCJ_number, birth_date, rcv_date, age_at_offense, age_rcv, education_lvl, 
                                                        offense_date, gender, hair_color, height, weight, eye_color, native_county, native_state, 
                                                        prior_occupation, prior_prison_record, inc_summary, co_defendants, race_gender_victim))
        conn.commit()
    else:
        return

def process_last_words(url, execution_id):
    print('lastwords ',execution_id)
    if (url[-3:] != "jpg"):
        url = "http://www.tdcj.state.tx.us/death_row/"+str(url)
        html = urllib.request.urlopen(url, context=ctx).read()
    
        # last_soup = BeautifulSoup(html, 'html.parser')
#         last_rows = opp_soup.find_all('tr')
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
        
        

url = "http://www.tdcj.state.tx.us/death_row/dr_executed_offenders.html"
html = urllib.request.urlopen(url, context=ctx).read()
soup = BeautifulSoup(html, 'html.parser')


rows = soup.find_all('tr')

data = []
for row in rows:
    field_counter = 0
    row_data = []
    for field in row.children:
        field_counter += 1
        if field.string != "\n":
            if field_counter == 4:
                opp_anker = field.next_element
                if isinstance(opp_anker, bs4.element.Tag):
                    opp_href = opp_anker.get('href')
                    process_offender_info(opp_href,row_data[0])
                else: 
                    continue
            elif field_counter == 6:
                last_anker = field.next_element
                if isinstance(last_anker, bs4.element.Tag):
                    last_href = last_anker.get('href')
                    process_last_words(last_href,row_data[0])
                else:
                    continue
            else: 
                row_data.append(field.string)
    data.append(row_data)  
    
del(data[0])
for row in data:
        
    cur.execute('''INSERT OR IGNORE INTO Convicts (execution_id, last_name, first_name, TDCJ_number, age, ex_date,
                    race, county) VALUES ( ?, ?, ?, ?, ?, ?, ?, ? )''',
                                    (int(row[0]), str(row[1]), str(row[2]), str(row[3]), int(row[4]),
                                     str(row[5]), str(row[6]), str(row[7])))
    conn.commit()
cur.close()
