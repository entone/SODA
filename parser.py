import Queue
import json
import datetime
from threading import Thread
import copy
import time

class Worker(Thread):
    data = None
    post = None
    rows = []
    
    def __init__(self, queue, columns=[], post=None, name=None):
        self.post = post
        self.queue = queue
        self.columns = columns
        self.rows = []
        Thread.__init__(self, name=name)
        self.daemon = True
        self.start()
        
    def run(self):
        while True:
            data = self.queue.get()
            cols = self.columns
            ar = []
            print "%s: parsing %s" % (self.name, len(data))
            for row in data:
                obj = {}
                for i, data in enumerate(row):
                    type = cols[i]['dataTypeName']
                    name = cols[i]['fieldName']
                    #print type
                    #print name
                    try:
                        data = getattr(self, "parse_%s"%type)(data, field=name)
                    except Exception as e:
                        pass
                    obj[name] = data
                ar.append(obj)
                self.rows.append(obj)
            if self.post: self.post(ar)
            self.queue.task_done()
    
    def parse_meta_data(self, data, field=None):
        return data
    
    def parse_date(self, data, field=None):
       try:
           return datetime.datetime.strptime(str(data), "%Y%m%d")
       except:
           return datetime.datetime.utcfromtimestamp(data) 
    
    def parse_number(self, data, field=None):
        return int(data)
    
    def replace(self, st, dic):
        for k,v in dic.iteritems(): st = st.replace(k, v)
        return st
    
    def parse_text(self, data, field=None):
        if field == 'phone_number': return u"+1%s" % self.replace(data, {"(":"", ")":"", " ":"", "-":""}).strip()
        if field == 'state': return data.upper()
        final = " ".join([s.capitalize() for s in data.lower().strip().split(" ")])
        return u"%s" % final
    
    def parse_html(self, data, field=None): 
        return self.parse_text(data, field=field)
    
    def parse_checkbox(self, data, field=None): return bool(data)

class Parser(object):
    
    def __init__(self, file, post=None):        
        workers=1
        print "Loading File: %s" % file
        data = json.loads(open(file).read())
        print "File Loaded"
        start = 0
        print "Workers: %s" % workers
        print "Data Length: %s" % len(data['data'])
        per = len(data['data'])/workers
        print "Rows Per Thread: %s" % per
        strt_time = time.time()
        q = Queue.Queue()
        threads = []
        for i in xrange(workers):
            end = start+per
            a = copy.copy(data['data'][start:end])
            cols = copy.copy(data['meta']['view']['columns'])            
            threads.append(Worker(queue=q, columns=cols, post=post, name="Parser %s" % i))
            q.put(a)
            start = end
        q.join()
        self.rows = [i for thread in threads for i in thread.rows]
        print "Final Length: %s" % len(self.rows)
        print "Total Time: %s" % (time.time()-strt_time)
        