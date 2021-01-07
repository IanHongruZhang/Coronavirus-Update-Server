#!/usr/bin/env python
#-*- coding:utf-8 -*-
import re
import time
import os
import requests
import logging
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from celery_app import app
from fake_useragent import UserAgent
import json
import gspread
from df2gspread import df2gspread as d2g
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from celery.utils.log import get_task_logger
import sys
import warnings

from datetime import timezone
from datetime import timedelta

warnings.simplefilter("ignore")
logger = get_task_logger('celery_app')

wks_name_old = "Old_version"
wks_name_latest = 'Latest_data'
wks_name_history = "oversea_history"
spreadsheet_key = '1kNgqN0an1xePNfqkXD8bMQDiBWtanAOfiDSNUn22Ln8'

def create_cre():
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name('jsonFileFromGoogle.json', scope)
	return credentials

def data_transfer(table_data):
    new_header = table_data.iloc[0] #grab the first row for the header
    table_data= table_data[1:] #take the data less the header row
    table_data.columns = new_header #set the header row as the df header
    return table_data

def generate_time():
	nowTime = datetime.datetime.now(tz=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H-%M-%S')
	return str(nowTime)

def table_transfer(table,column,day):
    table_t = table[["name","province",column]]
    table["stats_type"] = column
    table = table[["name","province","stats_type",column]]
    table.rename(columns = {column:day},inplace = True)
    return table

@app.task
def execute():
	logger.info("history_data_start_to_update!")
	credentials = create_cre()
	gc = gspread.authorize(credentials)
	spreadsheet = gc.open_by_key(spreadsheet_key)
	table_data = pd.DataFrame(spreadsheet.worksheet("Map_oversea").get_all_values())
	history_data = pd.DataFrame(spreadsheet.worksheet("oversea_history").get_all_values())

	table_data_2 = data_transfer(table_data)
	history_data_final = data_transfer(history_data)

	# 得到今天的时间
	day = '{dt.year}年{dt.month}月{dt.day}日'.format(dt = datetime.datetime.now())
	table_data_3 = table_data_2.drop(["id"],axis = 1)

	table_casetotal = table_transfer(table_data_3,"casetotal",day)
	table_curecase = table_transfer(table_data_3,"curecase",day)
	table_deathcase = table_transfer(table_data_3,"deathcase",day)

	# 将得到的数据与原表merge得到顺序，然后贴进去
	table_final = pd.concat([table_casetotal,table_curecase,table_deathcase])
	table_history_final = pd.merge(history_data_final,table_final,on = ["name","stats_type"],how = "right")
	history_data_final.loc[:,day] = list(table_history_final[day+"_y"])

	# 存成excel   
	nowTime = generate_time()
	filename = str(nowTime) + "_history_data.xlsx"
	history_data_final.to_excel(filename)
	try:
		d2g.upload(history_data_final, spreadsheet_key, wks_name_history, credentials=credentials, row_names=False)
		logger.info("history_data_update_completed!")
	except Exception as e:
		logger.error("history_data_update_failed")
