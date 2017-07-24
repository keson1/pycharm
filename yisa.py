#!/usr/local/bin/python2.7
#-*- coding: utf-8 -*-
import cx_Oracle
import requests,urllib,StringIO
import time
from datetime import datetime,timedelta
import thread
import sys
import os
import hashlib
import base64
import yaml
import redis
import logging
import json
from daemon import Daemon
import procname
reload(sys)  
sys.setdefaultencoding('utf-8')
os.environ['NLS_LANG']="SIMPLIFIED CHINESE_CHINA.ZHS16GBK"

class Producer(object):
    def __init__(self, config=None):
        config_file = open(os.path.dirname(os.path.abspath(__file__)) + '/config.yaml')
        server_config = yaml.safe_load(config_file)
        config_file.close()
        
        self.redis_client = redis.StrictRedis(host=server_config['redis']['host'], port=server_config['redis']['port'])
        self.config = config
        self.server_config = server_config
        self.connected = False
        self.kakou={'KK00022':'1','KK00023':'2','KK00006':'3','KK00005':'4','KK00108':'5','KK00109':'6','KK00110':'7','KK00111':'8','KK00099':'9','KK00100':'10','KK00097':'11','KK00098':'12','KK00112':'13','KK00113':'14','KK00061':'15','KK00062':'16','KK00018':'17','KK00019':'18','KK00015':'19','KK00042':'20','KK00043':'21','KK00055':'22','KK00056':'23','KK00085':'24','KK00086':'25','KK00087':'26','KK00088':'27','KK00102':'28','KK00103':'29','KK00104':'30','KK00105':'31','KK00106':'32','KK00107':'33','KK00009':'34','KK00011':'35','KK00012':'36','KK00037':'37','KK00048':'38','KK00016':'39','KK00017':'40','KK00063':'41','KK00064':'42','KK00002':'43','KK00001':'44','KK00065':'45','KK00066':'46','KK00047':'47','KK00046':'48','KK00007':'49','KK00008':'50','KK00020':'51','KK00021':'52','KK00053':'53','KK00054':'54','KK00044':'55','KK00045':'56','KK00051':'57','KK00052':'58','KK00067':'59','KK00068':'60','KK00038':'61','KK00039':'62','KK00091':'63','KK00092':'64','KK00093':'65','KK00094':'66','KK00035':'67','KK00036':'68','KK00071':'69','KK00072':'70','KK00069':'71','KK00070':'72','KK00096':'73','KK00095':'74','KK00040':'75','KK00041':'76','KK00031':'77','KK00032':'78','KK00033':'79','KK00034':'80','KK00073':'81','KK00074':'82','KK00075':'83','KK00076':'84','KK00101':'85','KK00027':'86','KK00028':'87','KK00049':'88','KK00050':'89','KK00079':'90','KK00080':'91','KK00089':'92','KK00090':'93','KK00029':'94','KK00030':'95','KK00024':'96','KK00025':'97','KK00026':'98','KK00057':'99','KK00058':'100','KK00233':'101','KK00234':'102','KK00059':'103','KK00060':'104'}
    def connect(self):
        while not self.connected:
            try:
                self.connected = False
                self.client = cx_Oracle.connect(***/***@xxx.19.36:1521/orcl')
            except cx_Oracle.DatabaseError as e:
                logging.error('Connecting error: %s', str(e))
                #print "Connecting error: "+str(e)
                time.sleep(1)
            else:
                #logging.info('Connected to Oracle server %s', '10.126.130.35')
                #print "Connected"
                self.connected = True
                break
                
    def sqlSelect(self,sql,connected):
        try:
            cr=connected.cursor()
            cr.execute(sql)
            rs=cr.fetchall()
            result = []
            for row in rs:
                tmp = {}
                i = 0
                for desc in cr.description:
                    tmp[desc[0]] = row[i]
                    i+=1
                result.append(tmp)
            cr.close()
            return result
        except cx_Oracle.DatabaseError as e:
            logging.error('sqlSelect error: %s', str(e))
            #print "sqlSelect error: "+str(e)
            return None
            
    def run(self):
        direction = {'由东向西':'1','由西向东':'2','由南向北':'3','由北向南':'4'}
        self.connect()
        while True:
            if self.connected:
                try:
                    min_id = int(self.read_id("AnShun.log").strip())
                    #query = "Select TO_CHAR(t.GCSJ , 'YYYY-MM-DD hh24:mi:ss') Last_PASS_TIME From V_TFC_PASS t Where Rownum = 1 Order by t.GCSJ DESC"
                    query = "SELECT MAX(ID) MAXID FROM WATCH "
                    rsid = self.sqlSelect(query,self.client)
                    if rsid == None:
                        print u'查询最大时间失败'
                        time.sleep(5)
                        continue
                    max_id = rsid[0]['MAXID']
                    if (max_id-min_id)>70:
                        max_id = min_id+70
                    else:
                        max_id = min_id#-timedelta(seconds=10)
                        print u'等待数据',min_id
                        time.sleep(5)
                        continue
                    #self.write_id('AnShun.log',max_time_str)
                    query = "SELECT ID ID,CAR_NO HPHM,CAR_SPEED SPEED,WATCH_TIME PSSJ,DEVICE_NO KKID,DRIVEWAY_2 LAND,HPYS HPYS FROM WATCH WHERE ID BETWEEN "+str(min_id)+" AND "+str(max_id)
                    #print query
                    #print "\n"
                    res = self.sqlSelect(query,self.client)
                    if res == None:
                        time.sleep(3)
                        continue
                    #print len(res)
                    max_id=min_id+int(len(res))+1
                    param = {}
                    #print max_id
                    self.write_id('AnShun.log', str(max_id))
                    for row in res:
                        param = {}
                        #if self.kakou.count(str(row['KKID']))==1:
                        #    param["source_id"] = "1"#1-车头 2-车尾 3-未知
                        #else:
                        #    if self.hunhe.count(str(row['KKID']))==1:
                        #        param["source_id"] = "3"
                        #    else:
                        #        param["source_id"] = "2"
                        if str(row['KKID']) in self.kakou:
                            param["source_id"] = "1"
                        else:
                            param["source_id"] = "3"
                        param['thirdpart_id'] = row['ID']
                        param["plate_number"] = row['HPHM'].decode("gbk").replace("-", "无").replace("00000000","无").replace("11111111","无")
                        #param["plate_number"] = row['HPHM'].decode("gbk").encode('utf-8').replace("车牌", "无").replace("未识别", "无").replace("不知道", "无").replace("-", "无")#车牌号码
                        #print row['HPHM'].decode("gbk")
                        #param["plate_number"] = row['HPHM'].encode('utf-8').replace("车牌", "无").replace("未识别", "无").replace("不知道", "无").replace("-", "无")#车牌号码
                        param["plate_type_id"] = '41'# 01黄牌，02蓝牌，41其它
                        param["plate_color_id"] = row['HPYS']# 1黄牌，2蓝牌，9其它
                        param["location_id"] = self.kakou[str(row['KKID'])] #地点编号
                        param["device_id"] = str(row['KKID'])#.replace("None","0")#设备编号
                        #print param["device_id"]
                        param["lane_id"] = str(row['LAND'])#车道编号,（01,02,03,……,16）
                        param["speed"] = str(int(row['SPEED']))#车辆速度
                        param["capture_time"] = str(row['PSSJ'])[0:19]#抓拍时间
                        param["capture_type_id"] = "1"#抓拍类型，（0-无，1-图片，2-录像）
                        param["unit_id"] = "0"#使用单位
                        param["direction_id"] = "0" #行驶方向
                        param["image_url"]="http://xx2.28.19.35:8080/GAKK/common/jsp/getRealImg.jsp?watchId="+str(row['ID'])+"&picId=0"
                        #print row['TPLJ'].decode("gbk")+row['TP1'].decode("gbk")
                        #param["image_url"]=row['TPLJ']+row['TP1'].decode("gbk").encode('utf-8') #图片地址
                        param["time_str"]  = time.strftime("%Y-%m-%d %H:%M:%S") #当前时间
                        msg = json.dumps(param,skipkeys=True)
			logging.info(str(row['ID'])+" " + param["capture_time"])
                        #print str(row['ID'])+" " + param["capture_time"]
                        self.redis_client.lpush(self.server_config['redis']['queue_key_name'], msg)
                    print 'sleep 1s'
                    time.sleep(1)
                except Exception as e:
                    logging.error('AnShun error: %s', str(e))
                    #print "AnShun_Error_: "+str(e)
                    time.sleep(2)
                    self.connected = False
                else:
                    pass
            else:
                self.connect()

    def read_id(self,file_name):
        file_object = open(file_name,'r')
        try:
            all_the_text = file_object.readline()
        finally:
            file_object.close()
        return all_the_text

    def write_id(self,file_name,id):
        file_object = open(file_name, 'w')
        try:
            file_object.write(id)
        finally:
            file_object.close()
 

#if __name__ == '__main__':
#    Producer().run()
class MyDaemon(Daemon):
    def __init__(self):
        self.name = 'yisa1'
        Daemon.__init__(self, pidfile='/var/run/%s.pid' % (self.name.lower()), stderr='/tmp/yisa_error.log')
        procname.setprocname(self.name)

    def run(self):
        config_file = open(os.path.dirname(os.path.abspath(__file__)) + '/config.yaml')
        server_config = yaml.safe_load(config_file)
        config_file.close()

        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename='/var/log/%s.log' % (self.name.lower()),level=logging.WARNING)
        logging.info('%s Started', self.name)
        requests_log = logging.getLogger("requests")
        requests_log.setLevel(logging.WARNING)
        Producer().run()

if __name__ == '__main__':
    daemon = MyDaemon()
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
            sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
