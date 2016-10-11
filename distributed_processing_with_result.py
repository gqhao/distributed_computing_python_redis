# -*- coding: UTF-8 -*-
import threading
import redis
from com.functions import *
from com.dbhelper import *
from com.dbconfig import *
import multiprocessing
import threading
import socket
import sys
from time import sleep

#########define the threading class
class MyThread(threading.Thread):
    def __init__(self,func,args,name=''):
        threading.Thread.__init__(self)
        self.name=name
        self.func=func
        self.args=args

    def run(self):
        apply(self.func,self.args)

##get the avg values
def get_avg_value(keylist):
    avg_value=[]
    for i in range(len(keylist)):
        tmp_list = []
        for j in range(len(keylist[i])):
            tmp_list.append(keylist[i][j][1])
        avg_value.append(sum(tmp_list)/len(tmp_list))
#        print 'len of tmplist',len(tmp_list)
        del tmp_list
    return avg_value

###get the slave keys
def get_slave_keys(keyslist):
    tmp_keys=[]
    for i in range(len(keyslist)):
        if keyslist[i].find('slave')==0:
            tmp_keys.append(keyslist[i])
    return tmp_keys

def get_diff_keys(keyslist1,keyslist2):
    tmp_resut=[x for x in keyslist1 if x not in keyslist2]
    return tmp_resut

def re_assign_work(re_config_r,re_config_w,re_config_w_pipe):
    print '###################################################################################'
    print 'New node is added or old node is removed,I have to stop to re-assign the works!'
    print '###################################################################################'
    task_keys = re_config_r.keys('*keylist')
    total_keys = []
    for item in task_keys:
        total_keys += re_config_r.lrange(item, 0, -1)
        re_config_w.delete(item)
    compute_node = re_config_r.hgetall('compute:node')
    compute_node_keys = compute_node.keys()
    real_compute_node = []
    for item in compute_node_keys:
        if compute_node.get(item) == '1':
            real_compute_node.append(item)
    compute_node_count = len(real_compute_node)
    if compute_node_count==0:
        compute_node_count=1

    compute_node_slice_count = len(total_keys) / compute_node_count
    count = 0
    for i in range(compute_node_count):
        if (i == compute_node_count - 1):
            tmp_list = total_keys[i * compute_node_slice_count:]
            for j in range(len(tmp_list)):
                re_config_w_pipe.lpush(real_compute_node[i]+':keylist', tmp_list[j])
                if count > 500:
                    re_config_w_pipe.execute()
                    count = 0
                count += 1
            re_config_w_pipe.execute()
            del tmp_list
        else:
            tmp_list = total_keys[i * compute_node_slice_count:(i + 1) * compute_node_slice_count]
            for j in range(len(tmp_list)):
                re_config_w_pipe.lpush(real_compute_node[i]+':keylist', tmp_list[j])
                if count > 500:
                    re_config_w_pipe.execute()
                    count = 0
                count += 1
            re_config_w_pipe.execute()
        print 'Processing: ',str(i*1.0/compute_node_count*100)+'%'
    print 'Processing: 100% '
    print 'Finished!'
    sleep(1)
    #print '\n'
    print '###################################################################################'
    print 'Works have been re-assigned,So damm hard!'
    print '###################################################################################'

def get_data_from_redis(para_conn_pool_redis,para_conn_mysql,para_keylist):
   # keylist=[]

    tmp_result_avg=[]
    re = redis.Redis(connection_pool=para_conn_pool_redis)
    redis_pipe=re.pipeline()
    insert_mysql="INSERT INTO MSSql_Data_TableSpace_total_multiprocessing(machine_name, host_name, db_name, table_name, schema_name, row_count, reserved_kb, data_kb, index_size_kb, unused_kb, collection_time) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    count=0
    keys=[]
    for item in para_keylist:
        redis_pipe.zrange(item,0,-1,withscores=True)
        if count>500:
            count=0
            tmp_dataset=redis_pipe.execute()
            for keyvalue in tmp_dataset:
                keys.append(keyvalue[0][0])
            tmp_result_avg+=(get_avg_value(tmp_dataset))
        count+=1
    tmp_dataset=redis_pipe.execute()
    for keyvalue in tmp_dataset:
        keys.append(keyvalue[0][0])


######write data into mysql-server
    #tmp_result_avg+=(get_avg_value(tmp_dataset))
    #result=[]
    #for i in range(0,len(tmp_result_avg),5):
    #    tmp=para_keylist[i].split(':')
    #    tmp=tmp[:-1]
    #    tmp.append(tmp_result_avg[i])
    #    tmp.append(tmp_result_avg[i+1])
    #    tmp.append(tmp_result_avg[i + 2])
    #   tmp.append(tmp_result_avg[i + 3])
    #    tmp.append(tmp_result_avg[i + 4])
    #    tmp.append(keys[i])
    #    result.append(tuple(tmp))
    #    del tmp

#    para_conn_mysql.executemany(insert_mysql,result)


    #queue.put(tmp_result_avg)

###get localmachine ip address

def get_my_ip():
    try:
        csock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        csock.connect(('8.8.8.8', 80))
        (addr, port) = csock.getsockname()
        csock.close()
        return addr
    except socket.error:
        return "127.0.0.1"

def sentinel_reset_master():
    re_sentinel1=redis.Redis(host='192.168.86.4',port='26379')
    re_sentinel2 = redis.Redis(host='192.168.86.6', port='26379')
    re_sentinel3 = redis.Redis(host='192.168.86.7', port='26379')
    re_sentinel1.execute_command('sentinel reset CentOS7-Server1')
    re_sentinel2.execute_command('sentinel reset CentOS7-Server1')
    re_sentinel3.execute_command('sentinel reset CentOS7-Server1')


#def func_get_avg_value(redis_keys,thread_num):
def main():
    pool_config_r = redis.ConnectionPool(host='127.0.0.1', port=6379, password=your_redis_server_password, db=0)
    re_config_r = redis.Redis(connection_pool=pool_config_r)
    #print 're_config_r created!'
    pool = redis.ConnectionPool(host='192.168.86.5', port=6378, password=your_redis_server_password, db=0)
    mysqlconn = ''#DBHelper(connectshort("sqlmonitordb"))
    re = redis.Redis(connection_pool=pool)
    hostname = socket.gethostname()
	
    #### do the role checking for self server####
    while True:
        count=0
        while True:
            try:
                repl_status=re_config_r.info('replication')
            except:
                print 'redis-server is loading rdb files or redis-server is starting, I will wait for 5 seconds'
                sleep(5)
                count += 1
                if count > 20:
                    print 'I have waited for 100 seconds,something wrong must have happend.Sorry,I have to exit(),bye bye !'
                    sys.exit(0)
                continue
            break

        repl_role= repl_status.get('role')
        repl_master_host=get_my_ip()
        repl_master_port=6379
        if repl_role!='master':
            count=0
         #   repl_master_host = repl_status.get('master_host')
         #   repl_master_port = repl_status.get('master_port')
            while True:
                repl_status = re_config_r.info('replication')
                repl_master_host = repl_status.get('master_host')
                repl_master_port = repl_status.get('master_port')

                repl_master_link_status =repl_status.get('master_link_status')
                if repl_master_link_status=='down':
                    print 'waiting for resuming replication!'
                    count+=1
                    sleep(5)
                    if count>24:
                        print 'waited for ',count*5,' seconds,resuming replication fails,I will call sys.exit() to exit!bye bye!'
                        sys.exit(0)
                else:
                    repl_master_host = repl_status.get('master_host')
                    repl_master_port = repl_status.get('master_port')
                    break
    
        pool_config_w=redis.ConnectionPool(host=repl_master_host, port=repl_master_port, password=your_redis_server_password, db=0)
        
        # re=redis.Redis(connection_pool=pool)
        # redis_pipe=re.pipeline()
        
        re_config_w = redis.Redis(connection_pool=pool_config_w)
        re_config_w_pipe=re_config_w.pipeline()
        
    #    keyslist=keyslist[0:60000]
    
    #    keyslist=keyslist[0:60000]
        #thread_num=4
    
        ########## designed for slave-computing-node########
    
        while True:
            if repl_role=='master':
                break
            function_name = 'dist_multiprocessing'
            count=0
        
            ##cope with the situation that master changed to slave


            ## check whether the redis-server is available
            while True:
                try:
                    cluster_node_role=re_config_r.hgetall('cluster:node:role')
                except:
                    print 'redis-server is load rdb files or redis-server is down, I will wait for 5 seconds'
                    sleep(5)
                    count+=1
                    if count >20:
                        print 'I have waited for 100 seconds,something wrong must have happend.Sorry,I have to exit(),bye bye !'
                        sys.exit(0)
                    continue
                break
            ## if the new computing-node is the old master node,do these steps
            if(cluster_node_role.get(hostname)=='master'):
                print 'I have already changed from master to slave,please know that!'
                re_config_w.hset('cluster:node:role',hostname,'slave')
                re_config_w.hset('compute:node', hostname, '1')
                print 'added into computed:node  by ',hostname
                re_config_w.hset('function:controller', function_name, 0)
                re_config_w.hset(hostname + ':function:times', function_name, '0')
                re_config_w.hset('cluster:node:change:flag', 'flag', '1')
    
             #   re_config_w.hset('compute:node', hostname, '1')
                del cluster_node_role
            node_info=re_config_r.hget('compute:node',hostname)
            ##cope with the situation that new compute node is added
            if not node_info:
                print 'New computing node is adding @',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                re_config_w.hset('function:controller',function_name,0)
                re_config_w.hset('compute:node',hostname,'1')
                print 'added into computed:node  by ', hostname
                re_config_w.hset(hostname+':function:times',function_name,'0')
                re_config_w.hset('cluster:node:role', hostname, 'slave')
                re_config_w.hset('cluster:node:change:flag', 'flag', '1')
                print 'New computing node is added @', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    
            if node_info=='0':
                print 'I am configured disable,so I will do nothing!'
                sleep(5)
                break
            ##computing node copes with the scenario of computing node changing
            cluster_node_change_flag=re_config_r.hget('cluster:node:change:flag', 'flag')
            if cluster_node_change_flag=='1':
                print 'New computing node is adding or old computing node is removing by the master,I will do nothing until the works reasigned by the master!'
                sleep(1)
                break

            starttime = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
            endtime = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    
            controller=re_config_r.hget('function:controller',function_name)
            try:
                times = int(re_config_r.hget(hostname+':function:times',function_name))
            except:
                break
            if times==0:
                print function_name,'has 0 times to run. main function will sleep for 1 second'
                sleep(1)
                break
            elif controller=='1':
                keyslist = re_config_r.lrange(hostname + ':' + 'keylist', 0, -1)
                keyslist.sort()
                starttime=time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))
                print function_name,'is startting @',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                threads=[]
                thread_num=2
                sublist_count=len(keyslist)/thread_num
                for i in range(thread_num):
                    if i ==(thread_num-1):
                        t = multiprocessing.Process(target=get_data_from_redis, args=(pool,mysqlconn,keyslist[i*sublist_count:]))
                    else:
                        t = multiprocessing.Process(target=get_data_from_redis,args=(pool,mysqlconn, keyslist[i * sublist_count:(i+1)*sublist_count]))
                    t.start()
                    threads.append(t)
                for process in threads:
                    process.join()
    
                endtime = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                print function_name,' is done @',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                re_config_w.zadd(hostname+':'+'starttime',function_name,starttime)
                re_config_w.zadd(hostname + ':' + 'endtime',function_name,endtime)
                times = times - 1
                #if times == 0:
                #    re_config_w.hset('function:controller', function_name, 0)
                re_config_w.hset(hostname+':function:times', function_name, times)

                while True:
                    starttime_new = re_config_r.zscore(hostname + ':' + 'starttime', function_name)
                    endtime_new = re_config_r.zscore(hostname + ':' + 'endtime', function_name)
                    if float(starttime) == starttime_new and float(endtime) == endtime_new:
                        print 'Received ACK from the master!'
                        break
                    else:
                        print 'Waiting for receiving ACK from the master.'
                        sleep(1)
                break
    
            else:
                print 'I am alive,but nothing to do,I will sleep for 1 second.'
                sleep(1)
                break

    ########designed for master-node
        while True:
            if repl_role=='slave':
                break
            function_name = 'dist_multiprocessing'
            count=0
            while True:
                try:
                    cluster_node_role=re_config_r.hget('cluster:node:role',hostname)
                except:
                    print 'redis-server is load rdb files or redis-server is down! I will wait for 5 seconds'
                    sleep(5)
                    count+=1
                    if count >20:
                        print 'I have waited for 100 seconds,something wrong must have happend.Sorry,I have to exit(),bye bye !'
                        sys.exit(0)
                    continue
                break

            slave_keys = get_slave_keys(repl_status.keys())
            ###cope with the isolate node
            if (not cluster_node_role) and (len(slave_keys)==0):
                print 'I am a isolate node,please edit my information in table cluster:node:details,thanks.I will sleep for 1 seconds.'
                re_delete_keys=re_config_r.keys()
                for item in re_delete_keys:
                    re_config_r.delete(item)
                sleep(1)
                break


 #           if len(slave_keys)==0:
 #               print 'Something error happened,I can not connect to the master'
 #               sleep(5)
 #               continue
            slave_ips=[]
            for i in range(len(slave_keys)):
                slave_ips.append(repl_status.get(slave_keys[i])['ip'])
            if cluster_node_role=='slave':
                print 'I have already changed from slave to master'
                re_config_w.hset('cluster:node:role',hostname,'master')
                re_config_w.hdel('compute:node',hostname)
                #re_config_w.hset('cluster:node:change:flag', 'flag', 1)
                re_config_w.delete(hostname + ':function:times')
                re_config_w.delete(hostname + ':starttime')
                re_config_w.delete(hostname + ':endtime')
                #re_config_w.delete(hostname + ':keylist')
                del cluster_node_role
    
    
            cluster_node_details=re_config_r.hgetall('cluster:node:details')
            cluster_node_details_history=re_config_r.hgetall('cluster:node:details:history')
            diff_curent_history=get_diff_keys(cluster_node_details.keys(),cluster_node_details_history.keys())
            diff_history_current = get_diff_keys(cluster_node_details_history.keys(), cluster_node_details.keys())
            #diff_salve_nodedetails=get_diff_keys(slave_keys,cluster_node_details.keys())
            diff_slave_nodedetails = get_diff_keys( slave_ips, cluster_node_details.values())
            diff_nodedetails_slave_ip = get_diff_keys(cluster_node_details.values(),slave_ips )
            diff_nodedetails_slave=[]
            for ip_slave in diff_nodedetails_slave_ip:
                for machine_name in cluster_node_details.keys():
                    if ip_slave==cluster_node_details.get(machine_name):
                        diff_nodedetails_slave.append(machine_name)
            diff_nodedetails_slave.remove(hostname)

            for item in diff_curent_history:
                print 'Configuration information of new node',item,' will be added into table cluster:node:role'
                tmp_ip=cluster_node_details.get(item)
                ##slave_info
                error_flag=0
                try:
                    re_conn_tmp=redis.Redis(host=tmp_ip,port=6379,db=0,password=your_redis_server_password)
                except:
                    print 'I Can not connect to the new node,please check it'
                    error_flag=1
                is_slave_flag = 0
                for i in slave_ips:
                    if i==tmp_ip:
                        is_slave_flag=1
                        break
                if is_slave_flag==1:
                    re_config_w.hset('cluster:node:role', item, 'slave')
                    re_config_w.hset('cluster:node:details:history', item, tmp_ip)
                    print 'Configuration information of new node', item, ' is added!'
                else:
                    flag=re_conn_tmp.execute_command("slaveof "+repl_master_host+" "+str(repl_master_port))
                    if flag=='OK':
                        re_config_w.hset('cluster:node:role', item, 'slave')
                        re_config_w.hset('cluster:node:details:history',item,tmp_ip)
                        print 'Configuration information of new node', item, ' is added!'
                    else:
                        print 'I cant not add the new node.'
                        break

    
            for item in diff_history_current:
                print 'The node ',item,' will leave us ,I will destroy it soon'
                tmp_ip = cluster_node_details_history.get(item)
                error_flag=0
                try:
                    re_conn_tmp = redis.Redis(host=tmp_ip, port=6379, db=0, password=your_redis_server_password)
                except:
                    print 'I Can not connect to ',item,',so I will just delete the configuration information!'
                    error_flag=1
                if error_flag==0:
                    flag = re_conn_tmp.execute_command("slaveof no one")
                    if flag=='OK':
                        sentinel_reset_master()
                        re_config_w.hdel('cluster:node:role', item)
                        re_config_w.hdel('cluster:node:details:history', item)
                        re_config_w.hdel('compute:node', item)
                        re_config_w.delete(item+':function:times')
                        re_config_w.delete(item+':starttime')
                        re_config_w.delete(item + ':endtime')
                   # re_config_w.delete(item + ':keylist')
                    tt_keys=re_conn_tmp.keys()
                    for item_key in tt_keys:
                        re_conn_tmp.delete(item_key)
                    print 'The node ',item,' is destroyed successfully!'
                re_config_w.hset('cluster:node:change:flag', 'flag', 1)

            for item in diff_slave_nodedetails:
                print 'The node ',item,' will leave us ,I will destroy it soon'
                ##tmp_ip = cluster_node_details_history.get(item)
                error_flag=0
                try:
                    re_conn_tmp = redis.Redis(host=item, port=6379, db=0, password=your_redis_server_password)
                except:
                    print 'I Can not connect to ',item,',so I will just delete the configuration information!'
                    error_flag=1
                if error_flag==0:
                    flag = re_conn_tmp.execute_command("slaveof no one")
                    if flag=='OK':
                   # re_config_w.delete(item + ':keylist')
                        sentinel_reset_master()
                        tt_keys = re_conn_tmp.keys()
                        for item_key in tt_keys:
                            re_conn_tmp.delete(item_key)
                        print 'The node ',item,' is destroyed successfully!'
                #re_config_w.hset('cluster:node:change:flag', 'flag', 1)
            #for item in diff_nodedetails_slave:
            #    re_config_w.hdel('cluster:node:details', item)

            if re_config_r.hget('cluster:node:change:flag','flag')=='1':
                re_assign_work(re_config_r,re_config_w,re_config_w_pipe)
                re_config_w.hset('cluster:node:change:flag','flag','0')
                #re_config_w.hset('function:controller',function_name,1)
    
            print 'I am master,I am alive,but not busy now,I will sleep for 1 second!'
            sleep(1)
            break


if __name__ == '__main__':
    main()
