import sys, os, re, operator
sys.path.append(os.environ['PROJ4001'])
from plugin_support import getSparkContext

sc=getSparkContext('top')
fin, fout=sys.argv[1:3]
regions=sc.mixin.inputData(fin,'regions')
locations=sc.mixin.inputData(fin,'locations')

lr_mapping={location:region for (location, region) in (line.split('\t') for line in locations.collect())}

_orders=(sc.mixin.inputData(fin, relation) for relation in ('orders-part-%05d'%i for i in xrange(6)))

def deserialize(line):
    pattern=re.compile(r'^(\w+)\t(\w+)\t(\d+)\t([\w,]*)\t([\d,;]+)$')
    fro, to, duration, requirements, str_packages = pattern.match(line).groups()
    packages=tuple(tuple(int(j) for j in i.split(',')) for i in str_packages.split(';'))
    return ((fro, to), (duration, requirements, packages))

def rmap(((fro, to), (duration, requirements, packages))):
    return (lr_mapping[fro]+lr_mapping[to], (fro, to, duration, requirements, packages))

orders=reduce(lambda a,b:a.union(b), _orders).map(deserialize).map(rmap)
print orders.takeSample(False, 2)

def rstat1():
    def sum_weight((key, (fro, to, duration, requirements, packages))):
        return key, sum(i[3] for i in packages)
    def prepare((key, s)):
        def _key(r):
            r1, r2=r[:2], r[2:]
            symmetric=(r1==r2)
            return (r,0,symmetric) if r1<=r2 else (r2+r1,1,symmetric)
        newkey, rev, sym=_key(key)
        return (newkey,(0, s, sym)) if rev else (newkey,(s, 0, sym))
    def merge((c11,c12,sym1),(c21,c22,sym2)):
        return (c11+c21,c12+c22,sym1) # sym1==sym2
    return orders.map(sum_weight).reduceByKey(operator.add).map(prepare).reduceByKey(merge)
rcount1=rstat1()
rcount1.cache()

print rcount1.take(2)

def asym((key,(c1,c2,sym))):
    return not sym

# rcount1={route:weight for (route, weight) in _rc0.collect()}

def rstat2(rc1):
    def flatDiff((key,(c1,c2,sym))):
        d=abs(c1-c2)
        if float(d)/max(c1,c2)<0.33:
            return []
        else:
            return [(key,c1,c2,d)]
    return rc1.filter(asym).flatMap(flatDiff).sortBy(operator.itemgetter(3), ascending=False).take(60)

rcount2=rstat2(rcount1)
print 'Unbalanced'
print rcount2

def rstat3(rc1):
    mapping=rc1.filter(asym).map(lambda (key,(c1,c2,sym)):(key,c1+c2)).sortBy(operator.itemgetter(1),ascending=False).collect()
    mmin=mapping[-1][1]
    mmax=mapping[0][1]
    mrange=mmax-mmin
    HI_THRESHOLD=mmax-float(mrange)*0.37
    LO_THRESHOLD=mmin+float(mrange)*0.016
    return filter(lambda (r,c):c>=HI_THRESHOLD, mapping), filter(lambda (r,c):c<LO_THRESHOLD, mapping)

rcount3_1, rcount3_2 = rstat3(rcount1)
print 'High'
print rcount3_1
print 'Low'
print rcount3_2

def rstat4(rc1):
    return rc1.filter(lambda (key,(c1,c2,sym)):sym).map(lambda (key,(c1,c2,sym)):(key,c1)).sortBy(operator.itemgetter(1),ascending=False).collect()

rcount4 = rstat4(rcount1)
print 'Hidden'
print rcount4
sys.exit(0)

result={
    '#locations': locations.count(),
    '#regions': regions.count(),
    '#orders': orders.count(),
    '#packages': orders.map(lambda order:len(order.split('\t')[4].split(';'))).reduce(lambda a,b:a+b),
}


if sc.mixin.outputData(fout, (line for line in output.split('\n') if line)):
    print fout

