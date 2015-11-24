# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import accounts.models
import datetime
import django.contrib.postgres.fields
import django.db.models.deletion
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='InputData',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('serial', models.CharField(default=accounts.models.serial_gen, unique=True, max_length=5)),
                ('publish_date', models.DateField(default=datetime.date.today, db_index=True)),
                ('exp_date', models.DateField(default=accounts.models.default_expiration)),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.SET_NULL, to=settings.AUTH_USER_MODEL, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='ResultData',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('serial', models.CharField(default=accounts.models.serial_gen, unique=True, max_length=5)),
                ('publish_date', models.DateField(default=datetime.date.today, db_index=True)),
                ('exp_date', models.DateField(default=accounts.models.default_expiration)),
                ('signature', models.CharField(default=b'', max_length=255, blank=True)),
                ('tags', django.contrib.postgres.fields.ArrayField(default=list, size=None, base_field=models.CharField(max_length=255), blank=True)),
                ('inputdata', models.ForeignKey(on_delete=django.db.models.deletion.SET_NULL, to='accounts.InputData', null=True)),
            ],
        ),
    ]
