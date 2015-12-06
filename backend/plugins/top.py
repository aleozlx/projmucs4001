import sys, os, re, operator
sys.path.append(os.environ['PROJ4001'])
from plugin_support import getSparkContext

sc=getSparkContext('top')
fin, fout=sys.argv[1:3]
regions=sc.mixin.inputData(fin,'regions')
locations=sc.mixin.inputData(fin,'locations')

lr_mapping={location:region for (location, region) in (line.split('\t') for line in locations.collect())}

_orders=[sc.mixin.inputData(fin, relation) for relation in ('orders-part-%05d'%i for i in xrange(6))]

def deserialize(line):
    pattern=re.compile(r'^(\w+)\t(\w+)\t(\d+)\t([\w,]*)\t([\d,;]+)$')
    fro, to, duration, requirements, str_packages = pattern.match(line).groups()
    packages=tuple(tuple(int(j) for j in i.split(',')) for i in str_packages.split(';'))
    return ((fro, to), (duration, requirements, packages))

def rmap(((fro, to), (duration, requirements, packages))):
    return (lr_mapping[fro]+lr_mapping[to], (fro, to, duration, requirements, packages))

orders=reduce(lambda a,b:a.union(b), _orders).map(deserialize).map(rmap)
orders.cache()
print orders.take(5)

rcount1=orders.countByKey()
def rstat2(rc1):
    rset=set()
    rev=lambda r:r[2:]+r[:2]
    ct=lambda r:rc1[r] if r in rc1 else 0
    for r in rc1:
        if r in rset:
            continue
        rset.add(r)
        rrev=rev(r)
        rset.add(rrev)
        if r==rrev:
            continue
        c1=ct(r)
        c2=ct(rrev)
        d=abs(c1-c2)
        if d<2 or float(d)/max(c1,c2)<0.33:
            continue
        yield (r,c1,c2,d)

rcount2=sorted(rstat2(rcount1), key=operator.itemgetter(3), reverse=True)
print rcount2

def rstat3(rc1):
    def _rstat3(rc1):
        rset=set()
        rev=lambda r:r[2:]+r[:2]
        ct=lambda r:rc1[r] if r in rc1 else 0
        for r in rc1:
            if r in rset:
                continue
            rset.add(r)
            rrev=rev(r)
            rset.add(rrev)
            if r==rrev:
                continue
            yield (r,ct(r)+ct(rrev))
    mapping=sorted(_rstat3(rc1), key=operator.itemgetter(1))
    mmin=mapping[0][1]
    mmax=mapping[-1][1]
    mrange=mmax-mmin
    t=float(mrange)*0.1
    return filter(lambda (r,c):c>=mmax-t, mapping), filter(lambda (r,c):c<=mmin+t, mapping)

rcount3_1, rcount3_2 = rstat3(rcount1)
print rcount3_1
print rcount3_2


sys.exit(0)

result={
    '#locations': locations.count(),
    '#regions': regions.count(),
    '#orders': orders.count(),
    '#packages': orders.map(lambda order:len(order.split('\t')[4].split(';'))).reduce(lambda a,b:a+b),
}


if sc.mixin.outputData(fout, (line for line in output.split('\n') if line)):
    print fout

