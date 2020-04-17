#!/usr/bin/env python
#-*- coding:utf-8 -*-
from datetime import timedelta
from celery.schedules import crontab
from kombu import Exchange,Queue
from celery import platforms
platforms.C_FORCE_ROOT = True
import logging.config

BROKER_URL = 'redis://:123456@127.0.0.1:6379/1'               # 指定 Broker
CELERY_RESULT_BACKEND = 'redis://:123456@127.0.0.1:6379/2'  # 指定 Backend

CELERY_TIMEZONE = 'Asia/Shanghai'
CELERY_ENABLE_UTC = False

# 指定导入的任务模块
CELERY_IMPORTS = (
    'celery_app.periodical_scraper',
    'celery_app.history_statistics',
    'celery_app.periodical_scraper_every_hour'
)

# 定时器模块
CELERYBEAT_SCHEDULE = {
    'scrape_every_24hours':{
    'task':'celery_app.periodical_scraper.execute',
    'schedule':crontab(minute=50,hour=23),
    'args':()
    },

    'scrape_every_24hours_history':{
    'task':'celery_app.history_statistics.execute',
    'schedule':crontab(minute=0,hour="3,7,8,9,10,12,17,22"),
    'args':()
    },

    'scrape_every_1hours': {
    'task': 'celery_app.periodical_scraper_every_hour.execute',
    'schedule':crontab(minute="0,30"),
    'args':()
    }
}

LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            # 'datefmt': '%m-%d-%Y %H:%M:%S'
            'format': '%(asctime)s \"%(pathname)s：%(module)s:%(funcName)s:%(lineno)d\" [%(levelname)s]- %(message)s'
        }
    },
    'handlers': {
        'celery': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': 'logging.log',
            'when': 'midnight',
            'encoding': 'utf-8',
        },
    },
    'loggers': {
         'celery_app': {
            'handlers': ['celery'],
            'level': 'INFO',
            'propagate': True,
         }
    }
}
logging.config.dictConfig(LOG_CONFIG)
