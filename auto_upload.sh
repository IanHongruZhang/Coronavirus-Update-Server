#!/bin/bash
cd /root/celery_coronavirus
git pull
git add .
git commit -m "auto_save"
git push -u origin master
