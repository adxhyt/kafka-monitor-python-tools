#!/usr/bin/python
import urllib2
import json
import paramiko
import ConfigParser

def config_init():
    cf = ConfigParser.ConfigParser()
    cf.read("partition_remote_monitor.cfg")

    #secs = cf.sections()
    #print 'sections:', secs, type(secs)
    #opts = cf.options("db")
    #print 'options:', opts, type(opts)
    #kvs = cf.items("db")
    #print 'db:', kvs

    ssh_host = cf.get("ssh", "ssh_host")
    ssh_port = cf.getint("ssh", "ssh_port")
    ssh_user = cf.get("ssh", "ssh_user")
    ssh_key_path = cf.get("ssh", "ssh_key_path")

    log_dir=cf.get("logdir", "log_dir")
    urls=cf.get("url", "url_list")
    return ssh_host, ssh_port, ssh_user, ssh_key_path, log_dir, urls

def urls2array(urls):
    arr = []
    for url in urls:
        r = urllib2.Request(url)
        res = urllib2.urlopen(r)
        content = res.read()
        dic = json.loads(content)
        each_array = [ [x['Topic'], x['Partition'], x['Offset']] for x in dic['data'] ]
        arr.extend(each_array)
    return arr

def array2dict(array):
    dicts = {}
    for item in array:
        try:
            dicts[item[0]].append([ item[1],item[2] ])
        except KeyError:
            dicts[item[0]] = []
            dicts[item[0]].append([ item[1],item[2] ])
    return dicts

def data_handle(array):
    ret = {}
    for i in array:
        if i[0] in ret.keys():
            ret[i[0]] = max(int(ret[i[0]]),i[1])
        else:
            ret[i[0]]=i[1]
    return ret

def read_file(fname):
    fp=open(fname, "r");
    ret = {}
    for line in fp.readlines()[2:]:
        temp = line.split()
        try:
           ret[temp[0]].append([int(temp[1]), int(temp[2].strip())])
        except KeyError:
           ret[temp[0]] = []
           ret[temp[0]].append([int(temp[1]), int(temp[2].strip())])
    fp.close()
    return ret

def read_data_ssh(serverHost, serverPort, userName, keyFile, fname):
    channel = paramiko.SSHClient();
    channel.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    channel.connect(serverHost, serverPort,username=userName, key_filename=keyFile )
    stdin,stdout,stderr=channel.exec_command('cat ' + fname)
    ret = {}
    for line in stdout.readlines()[2:]:
            temp = line.split()
            try:
               ret[temp[0]].append([int(temp[1]), int(temp[2].strip())])
            except KeyError:
               ret[temp[0]] = []
               ret[temp[0]].append([int(temp[1]), int(temp[2].strip())])
    channel.close()
    return ret

cfg = config_init()
ssh_host = cfg[0]
ssh_port = cfg[1] 
ssh_user = cfg[2] 
ssh_key_path = cfg[3]
log_dir = cfg[4]
urls = cfg[5]

remote_array = urls2array(urls)
result = array2dict(remote_array)
pusherData = {}
for k,v in result.iteritems():
   pusherData[k] = data_handle(v)

data = read_data_ssh(ssh_host, ssh_port, ssh_user, ssh_key_path, log_dir)
for k, v in data.iteritems():
    data[k] = data_handle(v)

for topic, topicData in data.iteritems():
    for partition, offset in topicData.iteritems():
        if topic in pusherData:
            if partition in pusherData[topic].keys():
                data[topic][partition] = offset - pusherData[topic][partition]
            else:
                data[topic][partition] = "WARNING"
        else:
            data[topic][partition] = "WARNING"
        print 'topic:%s, partition:%s, offset:%s' % (topic, partition, data[topic][partition])
