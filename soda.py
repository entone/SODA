import httplib
import json
import data

HOST = "data.cityofchicago.org"
SERVICE = "/views/INLINE/rows.json"

class base(object):
    
    def __init__(self, **kwargs):
        for k,v in kwargs.iteritems():
            try:
                setattr(self, "_%s_" % k, v)
            except: pass
    
    def json(self):
        obj = {}
        for k,v in self.__dict__.iteritems():
            if k.startswith("_") and k.endswith("_"):
                print k
                try:
                    obj.update(v.json())
                except Exception as e:
                    if isinstance(v, list):
                        try:
                            obj[k[1:-1]] = [i.json() for i in v]
                        except: pass
                    else:
                        obj[k[1:-1]] = v
        return obj

class Child(base):
    _type_ = "column"
    _value_ = None
    _columnId_ = None

class Query(base):
    _type_ = "operator"
    _value_ = "within_circle"
    _children_ = []
    
class Request(base):
    _originalViewId_ = None
    _name_ = None
    _start_ = 0
    _length_ = 10
    _query_ = None
    
class SODA(object):
    
    def __getattr__(self, key):
        def meth(**kwargs):
            try:
                args = getattr(data, key)
            except Exception as e:
                raise Exception("%s is not a valid method, see data.py for available endpoints" % key)
            else:
                q = {
                    "OriginalViewId":args['id'],
                    "name":key,
                    "query":{
                        "filterCondition":{
                            "type":"operator",
                            "value":"within_circle",
                            "children":[
                                {
                                    "type":"column",
                                    "columnId": args['location_column']
                                },
                                {
                                    "type":"literal",
                                    "value":kwargs.get("lat", 0)
                                },
                                {
                                    "type":"literal",
                                    "value":kwargs.get("lng", 0)
                                },
                                {
                                    "type":"literal",
                                    "value":kwargs.get("rad", 1000)
                                },
                            ]
                        }
                        }
                }
                js = json.dumps(q)
                print js
                conn = httplib.HTTPConnection(HOST)
                conn.request("POST", SERVICE+"?method=index", js, headers={ "Content-type:":"application/json"})
                res = conn.getresponse()
                return res.read()
                
        return meth
