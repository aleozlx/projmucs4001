#!/usr/bin/env python
import os, argparse, subprocess, logging, json

import sys, django
sys.path.append('/var/www/mucs4001.proj/frontend')
os.environ['DJANGO_SETTINGS_MODULE']='frontend.settings'
django.setup()
from django.db import models, transaction
from django.contrib.auth.models import User
from accounts.models import InputData, ResultData

p0=argparse.ArgumentParser(prefix_chars='+')
p0.add_argument('+v', dest='verbose', action='count', help='verbose infomation')
p0.add_argument('+t', '++format', default='text', choices=['text','json'], help='output format')
p0.add_argument('func', choices=['fsck','runserver','gen-ex','exec','rm','cat','meta'], help='functionality to be used')
p0.add_argument('args', nargs='*', help='arguments passed down to the specified functionality')
p0args=p0.parse_args()
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=[logging.WARNING, logging.INFO, logging.DEBUG][p0args.verbose or 0])
F_TEXT='text/plain'
F_JSON='application/json'
FORMAT={
    'text': F_TEXT,
    'json': F_JSON,
}[p0args.format]

sjoin = lambda *x: '\x20'.join(str(i) for i in x)
_n = lambda s: s[:-1] if s.endswith('\n') else s

def shell(cmd, async=False):
    logging.info(cmd)
    p=subprocess.Popen(cmd, shell=True, bufsize=4*1024, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    if async:
        return p
    if p.wait()==0:
        logging.debug(_n(p.stderr.read()))
    return p

ok = lambda o=None: json.dumps({'ok':1, 'data':o})
class ParentScopeVar(object):
    __slots__=('val',)
    def __init__(self):
        super(ParentScopeVar, self).__init__()
        self.val=None

def print_err(msg):
    err = lambda msg: json.dumps({'ok':0, 'errmsg':msg})
    logging.error(_n(msg))
    if FORMAT=='text/plain':
        print msg,
    else:
        print err(msg)

def func_fsck(args):
    p1=argparse.ArgumentParser(prog='fsck')
    p1.add_argument('path', help='HDFS path to be checked')
    p1.add_argument('--destroy', action='store_true', help='remove all data at specified path')
    p1.add_argument('--repair', action='store_true', help='automatically repair data structure')
    p1args=p1.parse_args(args)
    logging.info(sjoin('Checking data structure on HDFS:',p1args.path))
    if p1args.destroy:
        shell('hadoop fs -rm -r '+p1args.path)
    e=ParentScopeVar()
    def exists(*paths):
        p=shell('hadoop fs -ls '+'\x20'.join(paths))
        if p.wait()!=0:
            e.val=p.stderr.read()
            return False
        else:
            return True
    PATHS=(
        p1args.path,
        os.path.join(p1args.path, 'inputs'),
        os.path.join(p1args.path, 'results'),
    )
    def fsck(depth=1):
        if exists(*PATHS):
            if FORMAT==F_TEXT:
                print 'OK'
            elif FORMAT==F_JSON:
                print ok()
            return 1
        else:
            if p1args.repair and depth:
                for path in PATHS:
                    shell('hadoop fs -mkdir '+path)
                fsck(depth-1)
            else:
                print_err(e.val)
    return fsck()

def func_runserver(args):
    """starting script hardcoded"""
    LOGFILE='/tmp/proj4001_server.log'
    DJMANAGE='/var/www/mucs4001.proj/frontend/manage.py'
    p1=argparse.ArgumentParser(prog='runserver')
    p1.add_argument('-p', dest='port', type=int, default=8001, help='TCP port to be listened')
    p1args=p1.parse_args(args)
    if not func_fsck(['proj4001']):
        logging.error('fsck failed')
        return
    logging.info(sjoin('Launching server at port', p1args.port))
    #shell(sjoin('nohup',DJMANAGE,'runserver','0.0.0.0:'+str(p1args.port),'>',LOGFILE,'&'), async=True)
    #os.execlp('tail', 'tail', '-f', LOGFILE)
    os.execlp('python', 'python', DJMANAGE, 'runserver','0.0.0.0:'+str(p1args.port))

def func_gen_ex(args):
    p1=argparse.ArgumentParser(prog='gen-ex')
    p1.add_argument('-u', dest='username', help='Input dataset owner')
    p1args=p1.parse_args(args)
    try:
        if p1args.username:
            username=p1args.username
        else:
            username=raw_input('Username: ')
        try:
            user=User.objects.get(username=username)
        except User.DoesNotExist:
            print_err(sjoin('User does not exist',username))
            return
        else:
            with transaction.atomic():
                inputdata=InputData.objects.create(user=user)
                logging.info(sjoin('Generating example data for',username))
                import datagen
                scale=(2E3, 2.4E3)
                hpath='proj4001/inputs/%s'%inputdata.serial
                datagen.main(scale, 'hdfs:'+hpath)
                inputdata.save()
            try:
                InputData.objects.get(serial=inputdata.serial)
                if FORMAT==F_TEXT:
                    print 'OK', inputdata.serial
                elif FORMAT==F_JSON:
                    print ok({'serial': inputdata.serial, 'exp_date': inputdata.exp_date.strftime("%D")})
            except InputData.DoesNotExist:
                print_err('I/O Error')
                return
    except KeyboardInterrupt:
        print_err('Interrupted')
        return

def func_exec(args):
    p1=argparse.ArgumentParser(prog='exec')
    p1.add_argument('plugin', help='plugin module that contains algorithm to process the dataset')
    p1.add_argument('path', help='path to be passed to the plugin (inputs/input-id)')
    p1.add_argument('--test', action='store_true', help="don't suppress stdout from plugin and don't generate metadata")
    p1args=p1.parse_args(args)
    logging.info(sjoin('Launching', p1args.plugin, 'on', p1args.path))
    os.environ['PROJ4001']='/var/www/mucs4001.proj/backend'
    #PROJ4001=/var/www/mucs4001.proj/backend spark-submit plugins/count.py ehede
    if not os.path.exists('plugins/%s.py'%p1args.plugin):
        print_err('Plugin doesn\'t exist')
        return
    try:
        inputdata=InputData.objects.get(serial=p1args.path[len('inputs/') if p1args.path.startswith('inputs/') else 0:])
    except InputData.DoesNotExist:
        print_err('Input data don\'t exist')
        return
    else:
        with transaction.atomic():
            resultdata=ResultData.objects.create(inputdata=inputdata)
            logging.info(sjoin('Everything ready! Executing plugin for', inputdata.user.username))
            try:
                p=shell('spark-submit plugins/%s.py %s %s'%(p1args.plugin, inputdata.serial, resultdata.serial))
            except KeyboardInterrupt:
                print_err('Interrupted')
                return
            if not p1args.test:
                ret=p.stdout.read().strip()
                if ret==resultdata.serial:
                    pass
                else:
                    print_err('Exec Error')
                    return
                resultdata.save()
            else:
                print p.stdout.read(),
        try:
            ResultData.objects.get(serial=resultdata.serial)
            if FORMAT==F_TEXT:
                print 'OK', resultdata.serial
            elif FORMAT==F_JSON:
                print ok({'serial': resultdata.serial})
        except ResultData.DoesNotExist:
            print_err('I/O Error')
            return

def func_rm(args):
    p1=argparse.ArgumentParser(prog='rm')
    p1.add_argument('-e', '--expired', action='store_true', help='automatically remove all data that are expired')
    p1.add_argument('path', nargs='?', help='remove data specified by path on HDFS (results/result-id or inputs/input-id)')
    p1args=p1.parse_args(args)
    if p1args.expired:
        logging.info('Removing expired data [FAKED]')
    if p1args.path:
        if '/' in p1args.path and not p1args.path.startswith('/'):
            logging.info(sjoin('Removing data:', p1args.path))
            p=shell('hadoop fs -rm -r proj4001/%s'%p1args.path)
            if p.wait()==0:
                # Try to remove metadata
                if p1args.path.startswith('inputs/'):
                    serial=p1args.path[len('inputs/'):]
                    try:
                        inputdata=InputData.objects.get(serial=serial)
                        inputdata.delete()
                    except:
                        pass
                elif p1args.path.startswith('results/'):
                    serial=p1args.path[len('results/'):]
                    try:
                        resultdata=ResultData.objects.get(serial=serial)
                        resultdata.delete()
                    except:
                        pass

                if FORMAT==F_TEXT:
                    print 'OK'
                elif FORMAT==F_JSON:
                    print ok()
            else:
                print_err('HDFS `rm` Error')
        else:
            print_err('Not allowed')

def func_cat(args):
    p1=argparse.ArgumentParser(prog='cat')
    p1.add_argument('path', help='print out data specified by path on HDFS (results/result-id)')
    p1args=p1.parse_args(args)
    if p1args.path.startswith('results/'):
        p=shell('hadoop fs -cat proj4001/%s/part-00000'%p1args.path)
    else:
        p=shell('hadoop fs -cat proj4001/%s'%p1args.path)
    if p.wait()==0:
        if FORMAT==F_TEXT:
            print p.stdout.read(),
        elif FORMAT==F_JSON:
            print ok(p.stdout.read())
    else:
        print_err('I/O Error')
        return

def func_meta(args):
    p1=argparse.ArgumentParser(prog='meta')
    p1.add_argument('path', help='show metadata (results/result-id or jobs/track-id ...)')
    p1args=p1.parse_args(args)
    print p1args.path

if __name__=='__main__':
    globals()[''.join(['func_', p0args.func.replace('-', '_')])](p0args.args)

