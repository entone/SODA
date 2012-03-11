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
                    #print "Name: %s" % name
                    #print "Type: %s" % type
                    try:
                        data = getattr(self, "parse_%s"%type)(data, field=name)
                    except Exception as e:
                        print e
                        pass
                    obj[name] = data
                ar.append(obj)
                self.rows.append(obj)
            if self.post: self.post(ar)
            self.queue.task_done()
    
    def parse_meta_data(self, data, field=None):
        if field.find("_at") > 0:
            return self.parse_date(data, field)
        else: return data
    
    def parse_url(self, data, field=None):
        f = []
        for i in data:
            if i:
                try:
                    data = str(i)
                except: continue
                f.append(data)
        
        return f

    def parse_date(self, data, field=None):
        try:
           return datetime.datetime.strptime(str(data), "%Y%m%d")
        except:
            return datetime.datetime.utcfromtimestamp(data)

    def parse_location(self, data, field=None):
        f = []
        for i in data:
            if i:
                try:
                    data = float(i)
                except: continue
                f.append(data)
        
        return f

    def parse_calendar_date(self, data, field=None):
        return datetime.datetime.strptime(str(data), "%Y-%m-%dT%H:%M:%S") 

    def parse_number(self, data, field=None):
        try:
            return int(data)
        except:
            return float(data)
    
    def replace(self, st, dic):
        for k,v in dic.iteritems(): st = st.replace(k, v)
        return st
    
    def parse_phone(self, data, field=None):
        if isinstance(data, list):
            for i in data:
                if i: 
                    data = i
                    break;
        return u"+1%s" % self.replace(data, {"(":"", ")":"", " ":"", "-":""}).strip()

    def parse_text(self, data, field=None):
        if field in ['phone_number','fax','tty']: return self.parse_phone(data, field)
        if field == 'state': return data.upper()
        if field.find("_date") > 0: 
            print "DATE!! %s" % field
            return self.parse_date(data, field=field) 
        final = " ".join([s.capitalize() for s in data.lower().strip().split(" ")])
        return u"%s" % final
    
    def parse_html(self, data, field=None): 
        return self.parse_text(data, field=field)
    
    def parse_checkbox(self, data, field=None): return bool(data)

class Parser(object):
    
    def __init__(self, file=None, data={}, post=None):        
        workers=1
        if file:
            print "Loading File: %s" % file
            data = json.loads(open(file).read())
            print "File Loaded"
        d = data.get("data", [])
        cols = data.get('meta', {}).get('view', {}).get('columns', {})
        start = 0
        print "Workers: %s" % workers
        per = 0
        print "Data Length: %s" % len(d)
        per = len(d)/workers
        print "Rows Per Thread: %s" % per
        strt_time = time.time()
        q = Queue.Queue()
        threads = []
        for i in xrange(workers):
            end = start+per
            a = copy.copy(d[start:end])
            cols = copy.copy(cols)            
            threads.append(Worker(queue=q, columns=cols, post=post, name="Parser %s" % i))
            q.put(a)
            start = end
        q.join()
        self.rows = [i for thread in threads for i in thread.rows]
        print "Final Length: %s" % len(self.rows)
        print "Total Time: %s" % (time.time()-strt_time)
        