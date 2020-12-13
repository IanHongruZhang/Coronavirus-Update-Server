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
from fake_useragent import UserAgent
import json
import gspread
from df2gspread import df2gspread as d2g
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from celery.utils.log import get_task_logger
from datetime import timezone
from datetime import timedelta

ua = UserAgent()
fake_header = {
    "user-agent":ua.random
}
params = (
    ('f', 'json'),
    ('where', '1=1'),
    ('returnGeometry', 'false'),
    ('spatialRel', 'esriSpatialRelIntersects'),
    ('outFields', '*'),
    ('orderByFields', 'OBJECTID ASC'),
    ('resultOffset', '0'),
    ('resultRecordCount', '1000'),
    ('cacheHint', 'true'),
    ('quantizationParameters', '{"mode":"edit"}'),
)

def scrape(params):
	try:
		query_url = "https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/ncov_cases/FeatureServer/1/query"
		response = requests.post(query_url,fake_header,params=params)
		return response
	except Exception as e:
		logger.error("failed to download!")

def parse(response):
	json_files = json.loads(response.text)
	table_every_country = pd.DataFrame(list(map(lambda x:x["attributes"],json_files["features"])))
	table_table_total = table_every_country.groupby("Country_Region").agg({"Last_Update":"first","OBJECTID":"first","Confirmed":"sum",
                                                                   "Recovered":"sum","Deaths":"sum"})
	tf = table_table_total.sort_values(by = "Confirmed",ascending = False)
	countries = pd.read_excel("/root/celery_coronavirus/celery_app/country_sequence.xlsx")
	tf_f = tf.reindex(list(countries["Country"]))
	tf_f["Last_Update"] = tf_f.apply(timestamp_transfer,axis = 1)
	tf_f["Automatic_Update_Time"] = generate_time()
	return tf_f

def generate_time():
	nowTime = datetime.datetime.now(tz=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H-%M-%S')
	return str(nowTime)

def timestamp_transfer(table):
    time_stamp = int(table["Last_Update"]/1000)
    time_local = time.localtime(time_stamp)
    dt = time.strftime("%Y-%m-%d %H:%M:%S",time_local)
    return dt

def export_to_gs_every_hour(tf_f):
	wks_name_old = "Old_version"
	wks_name_latest = 'Latest_data'
	spreadsheet_key = '1kNgqN0an1xePNfqkXD8bMQDiBWtanAOfiDSNUn22Ln8'

	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	credentials = ServiceAccountCredentials.from_json_keyfile_name('/root/celery_coronavirus/celery_app/jsonFileFromGoogle.json', scope)

	# 第四步：把新的版本上传到latest_version
	# tf ~ latest_version
	try:
		d2g.upload(tf_f, spreadsheet_key, wks_name_latest, credentials=credentials, row_names=True)
	except Exception as e:
		logger.error(e)
		logger.error("failed to import new version of table")

	# 第五步：留存最新版本记录(.xlsx)
	nowTime = generate_time()
	filename_new = str(nowTime) + "_new_version.xlsx"
	tf_f.to_excel(filename_new)

def execute():
	response = scrape(params)
	tf_f = parse(response)
	export_to_gs_every_hour(tf_f)

	logging_time = generate_time()
	logging_records = str(logging_time) + "-every hour task finished!"
	logger.info(logging_records)

execute()