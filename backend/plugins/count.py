import sys, os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.environ['PROJ4001'])
from plugin_support import getSparkContext

sc=getSparkContext('count')
serial=sys.argv[1]
regions=sc.mixin.inputData(serial,'regions')
locations=sc.mixin.inputData(serial,'locations')
_orders=[sc.mixin.inputData(serial, relation) for relation in ('orders-part-%05d'%i for i in xrange(6))]
orders=reduce(lambda a,b:a.union(b), _orders)
orders.cache()
print '#orders', orders.count()
