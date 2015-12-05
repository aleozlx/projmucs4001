import sys, os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.environ['PROJ4001'])
from plugin_support import getSparkContext

sc=getSparkContext('count')
fin, fout=sys.argv[1:3]
regions=sc.mixin.inputData(fin,'regions')
locations=sc.mixin.inputData(fin,'locations')
_orders=[sc.mixin.inputData(fin, relation) for relation in ('orders-part-%05d'%i for i in xrange(6))]
orders=reduce(lambda a,b:a.union(b), _orders)
orders.cache()

result={
    '#locations': locations.count(),
    '#regions': regions.count(),
    '#orders': orders.count(),
    '#packages': orders.map(lambda order:len(order.split('\t')[4].split(';'))).reduce(lambda a,b:a+b),
}

output="""Number of locations: %(#locations)s
Number of regions: %(#regions)s
Number of orders: %(#orders)s
Number of packages: %(#packages)s
""" % (result)

if sc.mixin.outputData(fout, (line for line in output.split('\n') if line)):
    print fout

