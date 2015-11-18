#!/usr/bin/python
import urllib2
import json

# http service url for kafka pusher
urls = [ url1, url2 ]

# path for replication-offset-checkpoint log
fname = replication-offset-checkpoint-path

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

remote_array = urls2array(urls)
result = array2dict(remote_array)
pusherData = {}
for k,v in result.iteritems():
   pusherData[k] = data_handle(v)

data = read_file(fname)
for k, v in data.iteritems():
    data[k] = data_handle(v)

for topic, topicData in data.iteritems():
    for partition, offset in topicData.iteritems():
        if topic in pusherData:
            if partition in pusherData[topic]:
                data[topic][partition] = offset - pusherData[topic][partition]
            else:
                data[topic][partition] = "WARNING"
        else:
                data[topic][partition] = "WARNING"
        print 'topic:%s, partition:%s, offset:%s' % (topic, partition, data[topic][partition])

