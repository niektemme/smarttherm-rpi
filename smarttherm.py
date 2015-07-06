#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# Niek Temme
# smart thermostat
# http://niektemme.com
#
#

#import sqlite3
import threading
from xbee import ZigBee
import serial
import struct
import codecs
import apsw
import sys
import time
import datetime
import happybase
import random
import logging

logging.basicConfig(filename='/usr/local/tempniek/18log.log',level=logging.INFO)
logging.info('I told you so')

try:
    
    logging.info("Hello from Raspberry pi")
    curtime = time.time()
    logging.info(curtime)
    
    # sqllite settings
    vuri = ':memory:'
    #db1 = apsw.connect(vuri, check_same_thread = False)
    dbc = apsw.Connection(vuri)
     
    #xbee connection
    ser = serial.Serial('/dev/ttyAMA0', 9600, timeout=5)
    xbee = ZigBee(ser,escaped=True)
    
    #basic vals
    knownprekeys = ['40b5af00_rx000A01_','40b5af00_rx000A02_','40b5af00_rx000A03_','40b5af00_rx000A04_','40b5af00_rx000A05_','40b5af00_rx000A06_','40b5af00_rx000A07_','40b5af01_rx000A01_','40b5af01_rx000A02_','40b5af01_rx000A03_','40b5af01_rx000A04_','40b5af01_rx000A05_','40b5af01_rx000A07_','40b5af01_rx000B01_','40b5af01_rx000B02_','40b5af01_rx000B03_','40b5af01_rx000B04_']
    
    time.sleep(2)
    hpool = happybase.ConnectionPool(6,host='localhost')
    
   
    
    #classes for threading
    class myThreadInsert (threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
        def run(self):
            try:
                xinsert()
            except Exception:
                logging.exception("xinsert")
            
    class myThreadRead (threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
        def run(self):
            try:
                fread()
            except Exception:
                logging.exception("fread")
    
    class myThreadDel (threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
        def run(self):
            try:
                fdel()
            except Exception:
                logging.exception("del")
                
    class myThreadStatus (threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
        def run(self):
            try:
                fstatus()
            except Exception:
                logging.exception("status")

    class myThreadCurrent (threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
        def run(self):
            try:
                fcurrent()
            except Exception:
                logging.exception("current")
                
    class myThreadControll (threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
        def run(self):
            try:
                fcontroll()
            except Exception:
                logging.exception("controll")
                
    class myThreadCurtemp (threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
        def run(self):
            try:
                fcurtemp()
            except Exception:
                logging.exception("curtemp")
     
    class myThreadGetactscenario (threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
        def run(self):
            try:
                fgetactscenario()
            except Exception:
                logging.exception("fgetactscenario")           
                
    class myThreadFupusedscenario (threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
        def run(self):
            try:
                fupusedscenario()
            except Exception:
                logging.exception("fupusedscenario")
    
    #coninues fucntions
    #read xbee to pi db
    #test = ['b','c','d']
    
    def fins(fvcurtime,fvsource,fvid,fvdata):
    #sqliteinsert function
        bvepoch = 9999999999
        bvsub = 9999999
        fvepoch = int(fvcurtime)
        fkepoch = bvepoch - fvepoch
        fvsub =  bvsub - int(datetime.datetime.fromtimestamp(fvcurtime).strftime('%f'))
        fvprekey = fvsource + '_' + fvid + '_'
        fvkey = fvprekey + str(fkepoch) + '_' + str(fvsub)
        dbi = dbc.cursor()
        dbi.execute("INSERT INTO sensvals (vkey,vepoch,vsub,vsource,vport,vprekey,vvalue) VALUES(?,?,?,?,?,?,?)",(fvkey,fvepoch,fvsub,fvsource,fvid,fvprekey,fvdata))
        dbi.close()
    
    def xinsert():
    # insert values as db1
        ficurtime = 0
        fivepoch = 0
        fiprevepcoh = 0
        ficursource = '40b5af01'
        DEST_ADDR_LONG = "\x00\x13\xA2\x00@\xB5\xAF\x00"
        while True:
        #for i1 in range(0, 20000):
            logging.debug("insert started")
            logging.debug(datetime.datetime.utcnow())
            try:
                ficurtime = time.time()
                fivepoch = int(ficurtime)
                fimintime = fivepoch - 5
                response = xbee.wait_read_frame()
                logging.debug(response)
                vid = (response['id'])
                if vid != 'tx_status':
                    vsource = (codecs.decode(codecs.encode(response['source_addr_long'],'hex'),'utf-8')[8:])
                    logging.debug(vsource)
                    #vsource = 'rs4021' #(codecs.decode(codecs.encode(response['source_addr_long'],'hex'),'utf-8')[8:])
                    
                    logging.debug(vid)
                    #vid = 'rx' # (response['id'])
                    #vdata = 'a'+random.choice(test)+':200,300,23423,2341,1234' #codecs.decode(response['rf_data'],'utf-8')
                    vdata = codecs.decode(response['rf_data'],'utf-8')
                    logging.debug(vdata)
                    if vid == 'rx':
                        vid = 'rx'+(vdata[0:3].zfill(6))
                        vdata = vdata[4:]
                        vdatas = vdata.split(',')
                        for vdatasv in vdatas:
                            vdatasv = int(vdatasv)
                            fins(time.time(),vsource,vid,vdatasv)
                    else:
                        vid = vid.zfill(8)
                        vdata = int(vdata)
                        fins(time.time(),vsource,vid,vdata)
                        
                if fivepoch > fiprevepcoh:
                    fiprevepcoh = fivepoch
                    dbd11 = dbc.cursor()
                    dbd11.execute("SELECT vsendmes from sendmes where vepoch > %i order by vepoch desc,vsub desc LIMIT 1" % fimintime)
                    rows = dbd11.fetchall()
                    for row in rows:
                        fipayload = row[0]
                        #logging.info(fipayload)
                        xbee.tx(dest_addr='\x00\x00',dest_addr_long=DEST_ADDR_LONG,data=str(fipayload),frame_id='\x01')
                        fins(ficurtime,ficursource,'rx000B04',int('9'+str(fipayload)))
                        #logging.info(str(fipayload))
                    dbd11.close()
                    
            except Exception:
                logging.exception("fxinsert")
            time.sleep(0.001)      
                
    #read pidb        
    def fread():
        #for i2 in range(0,4):
        listmins = []
        while True:
            logging.debug('read starting')
            logging.debug(datetime.datetime.utcnow())
            try:
                with hpool.connection(timeout=3) as connection:      
                    table = connection.table('hsensvals')
                    del listmins[:]
                    for knownprekey in knownprekeys:
                        hval = table.scan(row_prefix='%s' % knownprekey,batch_size=1,limit=1)
                        phval = next(iter(hval), None)[0]
                        listmins.append([knownprekey,phval])
                    #listmins.append(['rs4021_rx0000ab_', 'rs4021_rx0000ab_8581753968_9225133'])
                    #listmins.append(['rs4021_rx0000ac_', 'rs4021_rx0000ac_8581753968_9014061'])
                    logging.debug(listmins)
                    cur1 = dbc.cursor()
                    bsend = table.batch()
                    for listmin in listmins:
                        tvprekey = listmin[0]
                        tvkey = listmin[1]
                        #tukey = 'a'
                        logging.debug(tvkey)
                        rrows = 1
                        ihsend = 0
                        while (rrows>0):
                            #logging.debug(tvkey)
                            cur1.execute("SELECT vkey,vvalue FROM sensvals where vprekey = '%s' and vkey < '%s' order by vkey desc limit 500" % (tvprekey,tvkey))
                            #cur1.execute("SELECT count(*) FROM sensvals where vkey < '%s'" % 'rs4021_rxac_9999999999_9999999')
                            rows = cur1.fetchall()
                            rrows = len(rows)
                            logging.debug(rrows)
                            for row in rows:
                                #logging.debug(row)
                                tvkey = row[0]
                                bsend.put(row[0], {'fd:cd': str(row[1])})
                                ihsend = ihsend + 1
                            if ihsend > 5000:
                                bsend.send()
                                logging.debug("sendp")
                                ihsend = 0
                        #logging.debug(rrows)
                    cur1.close()
                    bsend.send()
            except Exception:
                logging.exception("hsend")
            logging.debug('send')
            time.sleep(2)
    
    def fdel():
    ## insert values as db1
        #for i3 in range(0, 100):
        while True:
            time.sleep(5)
            try:
                logging.debug("del started")
                fvcurtime = time.time()
                fdvepoch = int(fvcurtime) - 120
                dbd = dbc.cursor()
                dbd.execute("DELETE FROM sensvals WHERE vepoch < %i" % fdvepoch)
                dbd.close()
                
                fdusedscenvepoch = int(fvcurtime) - (2*24*60*60)
                dbdel2 = dbc.cursor()
                dbdel2.execute("DELETE FROM usedscenario WHERE viepoch < %i" % fdusedscenvepoch)
                dbdel2.close()    
                
            except Exception:
                logging.exception("fdel")
            
    def fstatus():
        while True:
            time.sleep(30)
            try:
                logging.info("status")
                fvcurtime = time.time()
                logging.info(fvcurtime)
                dbd2 = dbc.cursor()
                dbd2.execute("SELECT count(*) FROM sensvals")
                rows = dbd2.fetchall()
                for row in rows:
                    logging.info("TOT COUNT")
                    logging.info(row)
                dbd2.close()
                
                dbd20 = dbc.cursor()
                dbd20.execute("SELECT count(*) FROM sendmes")
                rows2 = dbd20.fetchall()
                for row2 in rows2:
                    logging.info("TOT sendmes")
                    logging.info(row2)
                dbd20.close()
                
                dbsatus3 = dbc.cursor()
                dbsatus3.execute("SELECT count(*) FROM usedscenario")
                rows3 = dbsatus3.fetchall()
                for row3 in rows3:
                    logging.info("TOT usedscenario")
                    logging.info(row3)
                dbsatus3.close()
                
                dbsatus4 = dbc.cursor()
                dbsatus4.execute("SELECT count(*) FROM actscenario")
                rows4 = dbsatus4.fetchall()
                for row4 in rows4:
                    logging.info("TOT actscenario")
                    logging.info(row4)
                dbsatus4.close()
                
                dbsatus5 = dbc.cursor()
                dbsatus5.execute("SELECT count(*) FROM curtemp")
                rows5 = dbsatus5.fetchall()
                for row5 in rows5:
                    logging.info("TOT actemp")
                    logging.info(row5)
                dbsatus5.close()
                
            except Exception:
                logging.exception("fstatus")   
            time.sleep(3600)
            
    def fcurrent():
        time.sleep(2)
        while True:   
            try:
                logging.debug("current")
                fvcurtime = time.time()
                logging.debug(fvcurtime)
                
                fvepoch = int(fvcurtime)
                fminvepochtemp = fvepoch - 20
                fminvepochset = fvepoch - 1
                cursource = '40b5af01'
                
                dbd3 = dbc.cursor()
                dbd3.execute("DELETE FROM senscur")

                dbd3.execute("INSERT INTO senscur (vepoch, vsource, vport, vvalue) SELECT MAX(vepoch), vsource, vport, round(AVG(vvalue)) FROM sensvals WHERE vepoch BETWEEN %i and %i AND vprekey in ('40b5af00_rx000A01_','40b5af00_rx000A04_','40b5af00_rx000A05_') GROUP BY vsource,vport" % (fminvepochtemp,fvepoch))
                dbd3.execute("INSERT INTO senscur (vepoch, vsource, vport, vvalue) SELECT MAX(vepoch), vsource, vport, round(AVG(vvalue)) FROM sensvals WHERE vepoch BETWEEN %i and %i AND vprekey in ('40b5af00_rx000A02_') GROUP BY vsource,vport" % (fminvepochset,fvepoch))
                dbd3.execute("INSERT INTO senscur (vepoch, vsource, vport, vvalue) SELECT vepoch, vsource, vport, vvalue FROM sensvals WHERE vepoch = %i AND vprekey in ('40b5af00_rx000A03_','40b5af00_rx000A07_') ORDER BY vsub DESC LIMIT 1" % (fvepoch))
                

                dbd3.execute("SELECT * FROM senscur")
                rows = dbd3.fetchall()
                for row in rows:
                    fins(fvcurtime,cursource,row[2],row[3])
                    logging.debug(row)
                dbd3.close()
            except Exception:
                logging.exception("fcurrent")   
            time.sleep(1)
            
    def fcurtemp ():
        while True:   
            try:
                dbdc1 = dbc.cursor()
                dbdc1.execute("SELECT min(vkey) FROM curtemp")
                rows = dbdc1.fetchall()
                #rrows = len(rows)
                #print(rrows)
                #for row in rows:
                #    rmaxtemp = (row)[0]
                maxtemp = (rows)[0][0]
                if (maxtemp == None):
                    maxtemp = 9999999999
                dbdc1.close()
                
                max1 = str(maxtemp)
                #logging.info(max1)
                    
                with hpool.connection(timeout=3) as connectioncur: 
                    tablefc = connectioncur.table('hcurtemp')
                    hscan2 = tablefc.scan(row_stop=max1,batch_size=1,limit=1)
                    
                dbdc2 = dbc.cursor()
                for key, data in hscan2:
                    #print(int(key),int(data['fd:curt']))
                    #with dbc: ##niet in techt script de with: dbc
                    dbdc2.execute("INSERT INTO curtemp (vkey, vvalue) VALUES(%i,%i)" % (int(key),int(data['fd:curt']) ) )
                    logging.info("new curtemp")
                    logging.info(str(data['fd:curt']))
                dbdc2.close()
                
                dbdc3 = dbc.cursor()
                #with dbc:
                dbdc3.execute("DELETE FROM curtemp WHERE vkey NOT IN (SELECT MIN(vkey) FROM curtemp)")
                dbdc3.close()
                
                #dbdc4 = dbc.cursor()
                #dbdc4.execute("SELECT * FROM curtemp")
                #rows = dbdc4.fetchall()
                #for row in rows:
                    #logging.info(row)
                #dbdc4.close()
            except Exception:
                logging.exception("fcurtemp")   
            time.sleep(550)
            
    def fgetactscenario():
        while True:   
            try:
                facurtime = time.time()
                favepoch = int(facurtime)
                
            
                dbdac1 = dbc.cursor()
                dbdac1.execute("SELECT min(vkey) FROM actscenario")
                rows = dbdac1.fetchall()
                maxrow = (rows)[0][0]
                if (maxrow == None):
                    maxrow = '40b5af01'+'_'+'9999999999'+'_'+'9999999'             
                dbdac1.close()
                    
                    #logging.info(maxrow)
                with hpool.connection(timeout=3) as connectionacts: 
                    tableacts = connectionacts.table('hactscenario')  
                    hscanacts = tableacts.scan(row_stop=maxrow)
                    
                dbdac2 = dbc.cursor()
                for key, data in hscanacts:
                    logging.info(str(key))
                    #logging.info(int(data['fd:tempdif']))
                    dbdac2.execute("INSERT INTO actscenario (vkey, vgroup, viepoch, vtempdif, vouttempdif, run0, run1, run2, run3, run4, run5, vscore) VALUES('%s',%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i)" % (str(key),int(data['fd:group']),int(data['fd:iepoch']),int(data['fd:tempdif']),int(data['fd:outtempdif']),int(data['fd:run0']),int(data['fd:run1']),int(data['fd:run2']),int(data['fd:run3']),int(data['fd:run4']),int(data['fd:run5']),int(data['fd:score']) )   )
                dbdac2.close()
                    
                dbdac3 = dbc.cursor()
                #with dbc:
                dbdac3.execute("DELETE FROM actscenario WHERE viepoch NOT IN (SELECT distinct viepoch FROM actscenario order by viepoch desc LIMIT 3)")
                dbdac3.close()
                    
            except Exception:
                logging.exception("fgetactscenario")   
            time.sleep(650)
            
    def fupusedscenario():
        time.sleep(60)
        while True:
            try:
                cursource = '40b5af01'
                logging.info("start uploading used scen")
                with hpool.connection(timeout=3) as iusconnection:      
                    tableius = iusconnection.table('husedscenario')
                    tableiustbsc = iusconnection.table('husedscenariotbsc')
                    iushval = tableius.scan(row_prefix='%s' % cursource,batch_size=1,limit=1)
                    iusphval = next(iter(iushval), None)[0]
                    lastiusprekey = iusphval[0:19]
                    
                    logging.info("lastuprekey")
                    logging.info(lastiusprekey)
                    
                    bus = tableius.batch()
                    bustbsc = tableiustbsc.batch()
                    dbdius1 = dbc.cursor()
                    dbdius1.execute("SELECT vkey,vtempdif,vouttempdif,vtemp,vouttemp,vsettemp,scenariokey from usedscenario where vprekey < '%s'" % (lastiusprekey,))
                    rows = dbdius1.fetchall()
                    for row in rows:
                        bus.put(str(row[0]), {'fd:tempdif': str(row[1]), 'fd:outtempdif': str(row[2]), 'fd:temp': str(row[3]), 'fd:outtemp': str(row[4]), 'fd:settemp': str(row[5]), 'fd:scenariokey': str(row[6])})
                        bustbsc.put(str(row[0]), {'fd:cd': 'tbc'})
                        logging.info("uploading used scenario")
                        logging.info(str(row[0]))
                    dbdius1.close() 
                    bus.send()
                    bustbsc.send()
                logging.info("end uploading used scen")
            except Exception:
                logging.exception("fupusedscenario")   
            time.sleep(800) 
            
    def frunScen (actTemp, setTemp):
        if ( (setTemp*10) - actTemp > 35):
            return 1
        else:
            return 0

    def fscenLength(actTemp, setTemp):
        scnel = 0
        if ( (setTemp*10) - actTemp > 260):
            scnel = (6*60)
        elif ( (setTemp*10) - actTemp > 160):
            scnel = (5*60)
        elif ( (setTemp*10) - actTemp > 70):
            scnel = (4*60)
        elif ( (setTemp*10) - actTemp > 40):
            scnel = (3*60)
        else:
            scnel =  (2*60)
        return scnel


    def fboilerStat(starts,scenl,cur ,actTemp,setTemp):
        if (actTemp -  (setTemp*10) < 35):
            if (cur - starts < scenl):
                return 1
            else:
                return 0  
        else:
            return 2
        
    def getscen(tempdif,outtempdif):
        selselmode=[1,2]
        selmode = random.choice(selselmode)
        usedkey = 'failed'
        runminutes=[]
        try:
            dbdconsa5 = dbc.cursor()
            dbdconsa5.execute("SELECT max(viepoch) from actscenario")
            rows = dbdconsa5.fetchall()
            for row in rows:
                maxiepoch = int(row[0])
            dbdconsa5.close()
            #print (maxiepoch)
            
            
            if (selmode == 1):
                dbdconsa = dbc.cursor()
                dbdconsa.execute("SELECT vkey, run0,run1,run2,run3,run4,run5 from actscenario a1 \
                                 INNER JOIN (SELECT MIN(vscore) as maxvscore, vgroup,vtempdif,vouttempdif FROM actscenario \
                                                WHERE viepoch = %i \
                                                GROUP BY vgroup,vtempdif,vouttempdif) a2 \
                                    ON a1.vgroup = a2.vgroup \
                                    AND a1.vouttempdif = a2.vouttempdif \
                                    AND a1.vtempdif = a2.vtempdif \
                                    AND a1.vscore = a2.maxvscore \
                                INNER JOIN (SELECT MIN(ABS(vouttempdif - %i)) as minouttempdif FROM actscenario \
                                            WHERE viepoch = %i) a3 \
                                            ON ABS(a1.vouttempdif - %i) = a3.minouttempdif \
                                WHERE viepoch = %i \
                                ORDER BY ABS(a1.vtempdif - %i) LIMIT 1" % (maxiepoch,outtempdif,maxiepoch,outtempdif,maxiepoch,tempdif))
                rows = dbdconsa.fetchall()
                for row in rows:
                    #print(row)
                    usedkey = str(row[0])
                    runminutes = row[1:]
                dbdconsa.close()
                
                #print (usedkey)
                #print (runminutes[0])
            
                #for runm in runminutes:
                #    print(runm)
            
            else:
                dbdconsa2 = dbc.cursor()
                dbdconsa2.execute("SELECT a1.vgroup, a1.vscore, a1.vouttempdif from actscenario a1 \
                                 INNER JOIN (SELECT MIN(vscore) as maxvscore, vgroup,vtempdif,vouttempdif FROM actscenario \
                                                WHERE viepoch = %i \
                                                GROUP BY vgroup,vtempdif,vouttempdif) a2 \
                                    ON a1.vgroup = a2.vgroup \
                                    AND a1.vouttempdif = a2.vouttempdif \
                                    AND a1.vtempdif = a2.vtempdif \
                                    AND a1.vscore <> a2.maxvscore \
                                INNER JOIN (SELECT MIN(ABS(vouttempdif - %i)) as minouttempdif FROM actscenario \
                                              WHERE viepoch = %i  ) a3 \
                                            ON ABS(a1.vouttempdif - %i) = a3.minouttempdif \
                                WHERE viepoch = %i \
                                ORDER BY ABS(a1.vtempdif - %i) LIMIT 1" % (maxiepoch,outtempdif,maxiepoch,outtempdif,maxiepoch,tempdif))
                rows = dbdconsa2.fetchall()
                for row in rows:
                    fselgroup = row[0]
                    fselscore = row[1]
                    fouttempdif = row[2]
                dbdconsa2.close()
                
                dbdconsa3 = dbc.cursor()
                dbdconsa3.execute("SELECT rowid FROM actscenario WHERE vgroup = %i AND vscore <= %i AND vouttempdif = %i " % (fselgroup,fselscore,fouttempdif))
                rows = dbdconsa3.fetchall()
                selrowid = int(random.choice(rows)[0])
                
                #print(selrowid)
                
                dbdconsa4 = dbc.cursor()
                dbdconsa4.execute("SELECT vkey,run0,run1,run2,run3,run4,run5 from actscenario WHERE rowid = %i" % (selrowid,))
                rows = dbdconsa4.fetchall()
                for row in rows:
                    #print(row)
                    usedkey = str(row[0])
                    runminutes = row[1:]
            return usedkey,runminutes
        
        except:
            logging.exception("getscen")   
            return usedkey,runminutes
         
            
    def fcontroll(): 
        runScen = 0
        boilerStat = 0
        setBoiler = 0
        maxScen = (10*60)
        scenLength = 0
        cursource = '40b5af01'
        logging.info("time sleep 10 started")
        time.sleep(10)
        logging.info("time sleep 10 complete")
        bvepoch = 9999999999
        usettemp = -30000
        prevsettemp = -30000
        settemptime = -(60*60*24*5)
        
        #bvsub = 9999999

        
        while True:
            try:
                fscurtime = time.time()
                fsvepoch = int(fscurtime)
                fsvsub = int(datetime.datetime.fromtimestamp(fscurtime).strftime('%f'))
                fkepoch = bvepoch - fsvepoch
                fsmintime = fsvepoch-5
                curtemp = -30000
                settemp = -30000
                curtempepoch = -(60*60*24*5)
                settempepoch = -(60*60*24*5)
                outtemp = 1500
                
                dbd8 = dbc.cursor()
                dbd8.execute("SELECT vepoch,vvalue FROM sensvals where vsource = '40b5af01' and vport='rx000A04' order by vepoch desc, vkey asc LIMIT 1")
                rows = dbd8.fetchall()
                for row in rows:
                    curtempepoch = row[0]
                    curtemp = row[1]
                dbd8.close()
                
                dbd9 = dbc.cursor()
                dbd9.execute("SELECT vepoch,vvalue FROM sensvals where vsource = '40b5af01' and vport='rx000A02' order by vepoch desc, vkey asc LIMIT 1")
                rows = dbd9.fetchall()
                for row in rows:
                    settempepoch = row[0]
                    settemp = row[1]
                dbd9.close()
                
                dbcontr1 = dbc.cursor()
                dbcontr1.execute("SELECT vvalue FROM curtemp ORDER BY vkey asc LIMIT 1")
                rows = dbcontr1.fetchall()
                for row in rows:
                    outtemp = row[0]
                dbcontr1.close()
                
                if (prevsettemp != settemp):
                    settemptime = fsvepoch
                elif (fsvepoch - settemptime > 5 ):
                    usettemp = settemp
                    
                #logging.info("settemp")
                #logging.info(str(settemp))
                #logging.info("usettemp")
                #logging.info(str(usettemp))
                #logging.info("prevsettemp")
                #logging.info(str(prevsettemp))
                    
                prevsettemp = settemp
                
                curtemptimediff = fsvepoch-curtempepoch
                settemptimediff = fsvepoch-settempepoch
                tempdif = (usettemp*10) - curtemp
                outtempdif = (usettemp*10) - outtemp
                
                if(runScen == 0): #no scenario running
                    runScen = frunScen(curtemp,usettemp)
                    
                elif (runScen == 1): #start scenario
                    usedkey = 'failed'
                    usedkey,runminutes = getscen(tempdif,outtempdif)
                    #logging.info(usedkey)
                    if (usedkey <> 'failed'):
                        scenLength = runminutes[0]
                        maxrun = 6
                        
                        usscenpreky = cursource+'_'+str(fkepoch)
                        uscenkey = usscenpreky+'_'+ usedkey
                        dbdconsa5 = dbc.cursor()   
                        dbdconsa5.execute("INSERT INTO usedscenario (vkey, vprekey, viepoch, scenariokey, vtempdif, vouttempdif, vtemp, vouttemp, vsettemp) values ('%s','%s',%i,'%s',%i,%i,%i,%i,%i)" % (uscenkey,usscenpreky,fsvepoch,usedkey,tempdif,outtempdif,curtemp,outtemp,settemp))
                        dbdconsa5.close()
                        
                        
                    else:
                        scenLength = fscenLength(curtemp,usettemp)
                        maxrun = 1
                     
                    logging.info(usedkey)  
                    runnum  = 0    
                    startScen = int(time.time())   
                    runScen = 2
                    boilerStat = 1
                    logging.info(str(maxrun))
                    logging.info(str(runnum))
                    logging.info(str(scenLength))
                  
                elif(runScen == 2): #run scenario
                    runCurtime = int(time.time())
                    
                    if (boilerStat == 1 ):
                        boilerStat = fboilerStat(startScen,scenLength,runCurtime,curtemp,settemp)
                  
                    if (boilerStat == 1 ):
                        setBoiler = 1 #send xbee 1
                    else:
                        setBoiler = 0 #send xbee 0
                  
                    if(runCurtime - startScen > maxScen):
                        runScen = 3
                
                elif(runScen == 3): #run scenario
                    runnum = runnum + 1
                    if (runnum < maxrun):
                        runScen = 2
                        boilerStat = 1
                        startScen = int(time.time())
                        scenLength = runminutes[runnum]
                        logging.info(str(maxrun))
                        logging.info(str(runnum))
                        logging.info(str(scenLength))
                    else:
                        runScen = 0
                
                #logging.info("runscen")
                #logging.info(runScen)
                #logging.info("scenlength")
                #logging.info(scenLength)
                #logging.info("boilerstat")
                #logging.info(boilerStat)
                #logging.info("setBoiler")
                #logging.info(setBoiler)
                
                scurhour = int(datetime.datetime.fromtimestamp(fscurtime).strftime('%H'))
                scurminute = int(datetime.datetime.fromtimestamp(fscurtime).strftime('%M'))
                
                if (curtemp > -30000 and curtemptimediff < 20 and settemp > -30000 and settemptimediff < 20):
                    vchecksum = scurhour+scurminute+setBoiler+curtemp
                    sendstr = str(scurhour).zfill(2) + str(scurminute).zfill(2) + str(setBoiler) + str(curtemp).zfill(4)+str(vchecksum).zfill(4)
                    #xbee.tx( dest_addr='\x00\x00',dest_addr_long=DEST_ADDR_LONG,data=sendstr,frame_id='\x01')
                    fins(fscurtime,cursource,'rx000B01',scenLength)
                    fins(fscurtime,cursource,'rx000B02',boilerStat)
                    fins(fscurtime,cursource,'rx000B03',setBoiler)
                    #logging.info(sendstr)
                    dbd10 = dbc.cursor()
                    dbd10.execute("INSERT INTO sendmes(vepoch, vsub, vsendmes) VALUES(?,?,?)",(fsvepoch,fsvsub,sendstr))
                    dbd10.close()
                    
                dbd13 = dbc.cursor()
                dbd13.execute("DELETE from sendmes where vepoch < %i" % fsmintime)
                dbd13.close()
                
            except Exception:
                logging.exception("fcontroll")   
                   
    
    
    def dbstart():
        try:
            db1 = dbc.cursor()
            db1.execute("DROP TABLE IF EXISTS sensvals")
            db1.execute("CREATE TABLE sensvals(vkey TEXT, vepoch INT, vsub INT, vsource TEXT, vport TEXT, vprekey TEXT, vvalue INT)")
            db1.execute("CREATE UNIQUE INDEX isens1 on sensvals(vkey)")
            db1.execute("CREATE INDEX isens2 on sensvals(vepoch)")
            db1.execute("CREATE INDEX isens3 on sensvals(vsource)")
            db1.execute("CREATE INDEX isens4 on sensvals(vport)")
            db1.execute("CREATE INDEX isens5 on sensvals(vprekey)")
            
            db1.execute("DROP TABLE IF EXISTS senscur")
            db1.execute("CREATE TABLE senscur(vepoch INT, vsource TEXT, vport TEXT, vvalue INT)")
            db1.execute("CREATE INDEX icur1 on sensvals(vepoch)")
            db1.execute("CREATE INDEX icur2 on sensvals(vsource)")
            db1.execute("CREATE INDEX icur3 on sensvals(vport)")
            
            db1.execute("DROP TABLE IF EXISTS sendmes")
            db1.execute("CREATE TABLE sendmes(vepoch INT, vsub INT, vsendmes TEXT)")
            db1.execute("CREATE INDEX sendmes1 on sendmes(vepoch)")
            db1.execute("CREATE INDEX sendmes2 on sendmes(vsub)")
            
            db1.execute("DROP TABLE IF EXISTS actscenario")
            db1.execute("CREATE TABLE actscenario(vkey TEXT, viepoch INT, vgroup INT, vtempdif INT, vouttempdif INT, run0 INT, run1 INT, run2 INT, run3 INT, run4 INT, run5 INT, vscore INT)")  
            db1.execute("CREATE UNIQUE INDEX iactscenario1 on actscenario(vkey)")
            db1.execute("CREATE INDEX iactscenario2 on actscenario(vtempdif)")
            db1.execute("CREATE INDEX iactscenario3 on actscenario(vouttempdif)")
            db1.execute("CREATE INDEX iactscenario4 on actscenario(viepoch)")
            db1.execute("CREATE INDEX iactscenario5 on actscenario(vscore)")
            db1.execute("CREATE INDEX iactscenario6 on actscenario(vgroup)")
            
            db1.execute("DROP TABLE IF EXISTS usedscenario")
            db1.execute("CREATE TABLE usedscenario(vkey TEXT, vprekey TEXT, viepoch INT, scenariokey TEXT, vtempdif INT, vouttempdif INT, vtemp INT, vouttemp INT, vsettemp INT)")
            db1.execute("CREATE UNIQUE INDEX iusedscenario1 on usedscenario(vkey)")
            db1.execute("CREATE UNIQUE INDEX iusedscenario2 on usedscenario(viepoch)")
            
            db1.execute("DROP TABLE IF EXISTS curtemp")
            db1.execute("CREATE TABLE curtemp(vkey INT, vvalue INT)")
            db1.execute("CREATE UNIQUE INDEX icurtemp1 on curtemp(vkey)")
    
        except Exception:
            logging.exception("dbstart")
    
    
    #hbase start
    def hbasestart():
        try:
            #insert first vals for ech known key
            knownminkeys = []
            bvepoch = 9999999999
            bvsub = 9999999
            ddata = '9999'
            
            for knownprekey in knownprekeys:
                vkey = knownprekey + str(bvepoch) + '_' + str(bvsub)
                knownminkeys.extend([vkey])
            
            logging.debug(knownminkeys)
            
            with hpool.connection(timeout=3) as connection:
                table = connection.table('hsensvals')
                b = table.batch()
                for knownminkey in knownminkeys:
                    b.put(knownminkey, {'fd:cd': ddata})
                b.send()
                
                itable2 = connection.table('husedscenario')
                ib2 = itable2.batch()
                ib2.put('40b5af01_9999999999_40b5af01_9999999999_9999999', {'fd:tempdif': ddata})
                ib2.send()
                        
        except Exception:
            logging.exception("hbasestart")
    
    def fmain():
        try:
            time.sleep(1)
            
            logging.info("hallostart")
            dbstart()
            hbasestart()
            
            time.sleep(3)
            
            threadInsert = myThreadInsert()
            threadRead = myThreadRead()
            threadDel = myThreadDel()
            threadStatus = myThreadStatus()
            threadCurrent = myThreadCurrent()
            threadControll = myThreadControll()
            threadCurtemp = myThreadCurtemp()
            threadGetactscenario = myThreadGetactscenario()
            threadFupusedscenario = myThreadFupusedscenario()
            
            threadInsert.start()
            threadRead.start()
            threadDel.start()
            threadStatus.start()
            threadCurrent.start()
            threadControll.start()
            threadCurtemp.start()
            threadGetactscenario.start()
            threadFupusedscenario.start()
        except Exception:
            logging.exception("fmain")   
        
    if __name__=="__main__":
        fmain()

except Exception:
    logging.exception("main")



