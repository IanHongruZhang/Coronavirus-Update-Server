# -*- coding:utf-8 -*-
from celery import Celery

app = Celery("demo")
app.config_from_object("celery_app.celery_config")

