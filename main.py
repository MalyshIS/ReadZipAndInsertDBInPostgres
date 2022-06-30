import psycopg2
import pybase64
from psycopg2 import Error
import zipfile
import openpyxl
import logging
import pymqi

#search zip archive and excel file in zip file + read excel file + logging config
archive = zipfile.ZipFile('archive.zip', 'r')
xlfile = archive.open('excel.xlsx')
book = openpyxl.open(xlfile, read_only=True)
sheet = book.active
logging.basicConfig(filename="SystemOut.log", level=logging.INFO)

# read excel
for row in range(2, 3):
    kodterr = sheet[row][1].value
    snilspravo = sheet[row][2].value
    snilsymer = sheet[row][3].value
    namefiles = sheet[row][4].value
    idizve = sheet[row][5].value
    dataotp = sheet[row][6].value
    cmevid = sheet[row][7].value
    statuscmev = sheet[row][8].value
    statusdosva = sheet[row][9].value
    ecxel = kodterr, snilspravo, snilsymer, namefiles, idizve, dataotp, cmevid, statuscmev, statusdosva

#connect ty database and insert data
def db_insert():
    try:

        connection = psycopg2.connect(user="user",
                                      password="pass",
                                      host="ip",
                                      port="port",
                                      database="name database")

        cursor = connection.cursor()
        insert_query = """ INSERT INTO loxi (
        kodterr,
        snilspravo,
        snilsdeadzl,
        namefailpravo, 
        idizve,
        dateotp,
        idsmev,
        sdossmev,
        sdosepgy) 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        record_insert = (kodterr, snilspravo, snilsymer, namefiles, idizve, dataotp, cmevid, statuscmev, statusdosva)
        cursor.execute(insert_query, record_insert)
        connection.commit()
        print("1 запись успешно вставлена")
        cursor.execute("SELECT * from loxi")
        record = cursor.fetchall()
        print("Результат", record)
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        logging.critical('Ошибка при работе с PostgreSQL')
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")
            logging.info("Добавлена информация в БД")

#make base64
def makebase64():
  with open("archive.zip", "rb") as f:
     bytess = f.read()
     encoded = pybase64.b64encode(bytess)
     base = encoded.decode('utf-8')
     return base

#create xml file
def MakeXmlFle ():
    zip64 = makebase64()
    msg = ((
        '<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:xml="http://www.w3.org/XML/1998/namespace">'
            '<soap:Header>'
                '<InfoList>'
                    '<Info>'
                       f'<ID> {idizve} </ID>'
                       f'<CodeTerritori>{kodterr}</CodeTerritori>'
                       f'<SnilsAssignee>{snilspravo}</SnilsAssignee>'
                       f'<SnilsZL> {snilsymer} </SnilsZL>'
                       f'<NameFiles> {namefiles} </NameFiles>'
                       f'<Date> {dataotp} </Date>'
                       f'<IdSMEV> {cmevid} </IdSMEV>'
                       f'<StatusSmev> {statuscmev} </StatusSmev>'
                       f'<StatusEPGY> {statusdosva} </StatusEPGY>'
                       f'<Document> {zip64} </Document>'
                    '</Info>'
                '</InfoList>'
            '</soap:Header>'
        '</soap:Envelope>').format(zip64))
    message = bytes(msg, 'utf-8')
    with open("xmlfile.xml", "w") as output:
        output.write(str(message.decode('utf-8')))
        logging.info('Создан xml файл снилс -' + str(snilspravo) + ' - Удачно')
        #queue_manager = 'queue manager'
        #channel = 'channel'
        #host = 'ip'
        #port = 'port'
        #queue_name = 'queue'
        #messagee = message
        #conn_info = '%s(%s)' % (host, port)
        #qmgr = pymqi.connect(queue_manager, channel, conn_info)
        #queue = pymqi.Queue(qmgr, queue_name)
        #queue.put(messagee)
        #queue.close()



db_insert()
makebase64()
MakeXmlFle()