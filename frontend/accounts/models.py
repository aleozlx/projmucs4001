# -*- coding: utf-8 -*-
from django.db import models, transaction
from django.contrib.postgres.fields import ArrayField
from django.contrib.auth.models import User
import datetime, random

def default_expiration():
    return datetime.date.today()+datetime.timedelta(5)

def serial_gen():
    return ''.join(random.choice('qwertyuiopasdfghjklzxcvbnm') for i in xrange(5))

class InputData(models.Model):
    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)
    serial = models.CharField(max_length=5, default=serial_gen, unique=True)
    publish_date = models.DateField(default=datetime.date.today, db_index=True)
    exp_date = models.DateField(default=default_expiration)

class ResultData(models.Model):
    inputdata = models.ForeignKey(InputData, on_delete=models.SET_NULL, null=True)
    serial = models.CharField(max_length=5, default=serial_gen, unique=True)
    publish_date = models.DateField(default=datetime.date.today, db_index=True)
    exp_date = models.DateField(default=default_expiration)
    signature = models.CharField(max_length=255, blank=True, default='')
    tags = ArrayField(models.CharField(max_length=255), blank=True, default=list)

