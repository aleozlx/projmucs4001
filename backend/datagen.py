import os, sys, random, itertools, subprocess, traceback, logging
sjoin = lambda *x: '\x20'.join(str(i) for i in x)
randstr=lambda l:''.join(random.choice('qwertyuiopasdfghjklzxcvbnm') for i in xrange(l))
populate=lambda l,ll:list(set([randstr(ll) for i in xrange(l)]))
randrange=lambda l:xrange(random.randint(1,l))

locations=populate(8000,6)
regions=populate(120,2)
lr_mapping={location:regions[int(random.betavariate(2, 5)*len(regions))] for location in locations}
standard_packages=[(10,15,1,1),(15,15,15,5),(50,70,20,10),(120,120,120,60)] # packages (x,y,z,w)

class Collection(object):
	def __init__(self, a, **options):
		super(Collection, self).__init__()
		self.a = a
		self.options = options

	def __iter__(self):
		return iter(self.a)

	def __len__(self):
		return len(self.a)

	def __getattr__(self, name):
		return self.options[name] if name in self.options else None

def chunks(a, n):
	assert n>1
	for i in xrange(0, a.size or len(a), n):
		yield itertools.islice(a,i,i+n)

def ostream(fname):
	if fname.startswith('hdfs:'):
		return subprocess.Popen("hadoop fs -put - %s"%fname[5:],
			shell=True, bufsize=4*1024**2, stdin=subprocess.PIPE).stdin
	else:
		return open(fname, 'w')

def write(a):
	global cancel_flag
	size = a.size or len(a)
	try:
		if a.mode=='big':
			assert isinstance(a.part, int)
			fname='%s-part-%05d'%(os.path.join(output_path, a.name), a.part)
			if cancel_flag:
				logging.warn(sjoin('Skipped:', fname))
				return
			f=ostream(fname)
			for progress, p_chunk in enumerate(chunks(a, size/100)):
				sys.stderr.write('\rWriting %s - %d%%'%(fname, progress))
				sys.stderr.flush()
				for i in p_chunk:
					f.write(a.serializer(i))
			sys.stderr.write('\n')
			sys.stderr.flush()
		else:
			fname=os.path.join(output_path, a.name)
			if cancel_flag:
				logging.warn(sjoin('Skipped:', fname))
				return
			f=ostream(fname)
			for i in a:
				f.write(a.serializer(i))
	except KeyboardInterrupt:
		cancel_flag=True
		logging.error('Cancelled')
	except:
		traceback.print_exc()
	else:
		logging.info(sjoin(size, a.name, 'written'))
	finally:
		try:
			f.close()
		except:
			pass

def gen_orders((mininum, maximum), part=0):
	size=random.randint(mininum, maximum)
	def _gen():
		for i in xrange(size):
			yield ( # (fro,to,duration,requirements,packages)
				random.choice(locations),
				random.choice(locations),
				random.choice([1,2,2,2,5,5]),
				[],
				[random.choice(standard_packages) for i in randrange(4)]
			)
	return Collection(_gen(),
		name='orders', size=size, mode='big', part=part,
		serializer=lambda record:'%s\t%s\t%d\t%s\t%s\n'%(
			record[0],
			record[1],
			record[2],
			','.join(record[3]),
			';'.join(','.join(str(k) for k in j) for j in record[4])
		)
	)

output_path=None #sys.argv[1]
cancel_flag=False
#if not os.path.isdir(output_path):
#	os.mkdir(output_path)
def main(scale, path):
	global output_path
	output_path=path
	write(Collection(regions,
		name='regions',
		serializer=lambda i:'%s\n'%i
	))
	write(Collection(locations,
		name='locations',
		serializer=lambda i:'%s\t%s\n'%(i,lr_mapping[i])
	))

	#scale=(2E3, 2.4E3)
	for i in xrange(6):
		write(gen_orders(scale, part=i))
	logging.info('Done')

#os.system('du -h dataset/*')
