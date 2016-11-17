#!/usr/bin/env python2
# -*- coding:utf-8 -*-
# Copyright (C) yanghongshun@gmail.com
#使用说明：
#第一步：./app_da.py -i ./../data/20150430/MF/CDS-7-MF.csv -s settings.conf -c MF-GOTERM.txt -t goterm -d MF
##第一步，程序会在result中生成MF_descendant_ancestor.csv这个文件，MF,CC,BP字符不可替代都是标识符
#第二步：./app_da.py -i ./../data/20150430/MF/CDS-H1-MF.csv -g ../result/MF_descendant_ancestor.csv -c MF-GOTERM.txt -t goterm
##第二步，程序本地调用父子集合对后续的文件开始生成
import operator
import csv
import types
import os
import sys
import itertools
import getopt
import ConfigParser
import MySQLdb

def readSetting(option,section,filePath):
	
	conf = ConfigParser.ConfigParser() 
	conf.read(filePath)
	
	option = option.lower()

	
	if os.path.isfile(filePath):
		##print section+":"+option + " read value from " +filePath
		pass
	else :
		print filePath+" not exist"
		sys.exit()
	
	if section not in conf.sections():
		print section + " not exist"
		sys.exit()
	else :
		if option not in conf.options(section):
			print option + " not exist"
			sys.exit()
		else:
			val = conf.get(section,option)
			if val != '':
				return val
			else :
				print section + ':' +option +'is null'
				sys.exit()
	


def setCsv(title,spliter,csvPath):
	
	data = []
	
	print "reading file: " + csvPath 
	
	if not os.path.isfile(csvPath):
		print "file not exist!"
		sys.exit()
		
	csvfile=csv.reader(open(csvPath, 'r'),delimiter=spliter)
	
	print "storing data"
	
	for line in csvfile:
			data.append(line)
	
	if title == 1:
		print "deleting title line"
		del data[0]
	
	
	return data


def savDataToFile(title,data,file_path,fmt=''):
	
	print "saving data to file :"+ file_path
	
	if os.path.isfile(file_path):
		os.remove(file_path)
	
	file_handle = open(file_path,'wb')
	if fmt=='':
		csv_writer = csv.writer(file_handle)##delimiter=' ',
	else:
		csv_writer = csv.writer(file_handle,fmt)##delimiter=' ',
	if len(title) >0 :
		csv_writer.writerow(title)
	csv_writer.writerows(data)
	
	file_handle.close()
	
	print "saved ok"

def countGOAncestor(ggc_data,gda_data,gc_data,col_count):
	
	count_data =[]
	
	for g_a in gc_data:##g_a[0]:goid(ancestor)
		g_a_cur_count = 0
		for g_g_c in ggc_data:##g_g_c[0]:goid(descendant) g_g_c[2]:goid_count
			for g_d_a in gda_data:##g_d_a[0]:goid(descendant) g_d_a[1]:goid(ancestor)
				
				if cmp(g_g_c[0].replace(" ", "").strip('\"'),g_d_a[0].replace(" ", "").strip('\"'))==0:#g_g_c[0] goid,goterm 所在原始数据中的列
					if cmp(g_d_a[1].replace(" ", "").strip('\"'),g_a[0].replace(" ", "").strip('\"'))==0:
						g_a_cur_count=g_a_cur_count+int(g_g_c[col_count])##goid 2,goterm 1
						print g_a
						print g_g_c
						print g_d_a
						#sys.exit(0)
			if cmp(g_g_c[0].replace(" ","").strip('\"'),g_a[0].replace(" ","").strip('\"'))==0:
				g_a_cur_count=g_a_cur_count+int(g_g_c[col_count])##goid 2,goterm 1
						
		count_data.append([g_a[0],g_a_cur_count])
		print [g_a[0],g_a_cur_count]
		print "-----------------"
		#sys.exit(0)		
	
	return count_data
	
def getDescendantByGOIDFromDb(goidterm,gogo_list,conf_settings_path):
	h = readSetting('go_db_host','GO',conf_settings_path)
	u = readSetting('go_db_user','GO',conf_settings_path)
	c = readSetting('go_db_pass','GO',conf_settings_path)
	p = int(readSetting('go_db_port','GO',conf_settings_path))
	n = readSetting('go_db_name','GO',conf_settings_path)
	
	descendants = []
	
	try:
		conn=MySQLdb.connect(host=h,user=u,passwd=c,db=n,port=p)
					
		curr = conn.cursor(MySQLdb.cursors.DictCursor)
		
		for gogo in gogo_list:
			
			if cmp(goidterm.upper(),'goid'.upper())==0:
				q="SELECT DISTINCT descendant.acc as goid, descendant.name as goterm, descendant.term_type as type FROM  term  INNER JOIN graph_path ON (term.id=graph_path.term1_id)  INNER JOIN term AS descendant ON (descendant.id=graph_path.term2_id) WHERE term.acc='%s' AND distance <> 0 " % (gogo)
			elif cmp(goidterm.upper(),'goterm'.upper())==0:
				q="SELECT DISTINCT descendant.acc as goid, descendant.name as goterm, descendant.term_type as type FROM  term  INNER JOIN graph_path ON (term.id=graph_path.term1_id)  INNER JOIN term AS descendant ON (descendant.id=graph_path.term2_id) WHERE term.name='%s' AND distance <> 0 " % (gogo)
			curr.execute(q)
			rows = curr.fetchall()
			if cmp(goidterm.upper(),'goid'.upper())==0:
				for row in rows:
					descendants.append([row['goid'],gogo])
			elif cmp(goidterm.upper(),'goterm'.upper())==0:
				for row in rows:
					descendants.append([row['goterm'],gogo])
		
		##add himself
		descendants.append([gogo,gogo])
		curr.close()
		conn.close()
	except MySQLdb.Error,e:
		 print "Mysql Connect Error %d: %s" % (e.args[0], e.args[1])

	return descendants	

def main(argv):
	
	map_go_type = 'goid' ##goterm
	#columns:goid gene_count
	goid_gene_count_path = ''
	
	#columns:descendant ancestor
	goid_descendant_ancestor_path=''
	
	#columns:goid(check ancestor)
	goid_check_path=''
	
	#settings.conf
	conf_settings_path=''
	#
	
	go_type='_'
	
	try:
		opts, args = getopt.getopt(argv,"hi:g:c:s:t:d:",["input=","goda=","check=","settings=","type=","desendant="])
	except getopt.GetoptError:
		print '请使用 -h 选项查看帮助'
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print '-i <--input csv file:原始数据文件 '
			print '-g <--goda  csv file:GO子孙数据 '
			print '-c <--check csv file:BP,MF,CC既要统计合并的GOID数据 '
			print '-s <--settings csv file:配置文件'
			print '-t <--type :goid,goterm'
			print '-d <--desendant name prefix like BP,CC,MF'
                        print '首次从远程数据库读取MF goterm并生成MF_descendant_ancestor.csv保存至本地'
			print './app.py -i ./../data/20150430/MF/CDS-7-MF.csv -s settings.conf -c MF-GOTERM.txt -t goterm -d MF'
                        print '从本地读取MF_descendant_ancestor.csv'
			print './app.py -i ./../data/20150430/MF/CDS-H1-MF.csv -g ../result/MF_descendant_ancestor.csv -c MF-GOTERM.txt -t goterm'
                        sys.exit()
		elif opt in ("-i","--input"):
			goid_gene_count_path = arg
		elif opt in ("-g","--goda"):
			goid_descendant_ancestor_path = arg
		elif opt in ("-c","--check"):
			goid_check_path = arg
		elif opt in ("-s","--settings"):
			conf_settings_path = arg
		elif opt in ("-t","--type"):
			map_go_type = arg
		elif opt in ("-d","--desendant"):
			go_type= arg
		
	#创建目录
	flname=os.path.basename(goid_gene_count_path)
	fl_path=os.path.split(os.path.dirname(goid_gene_count_path))
	
	fldir_date = './../result/'+os.path.split(fl_path[0])[1]
	if os.path.exists(fldir_date)==False:
		os.mkdir(fldir_date)
	fldir = fldir_date+'/'+fl_path[1]
	if os.path.exists(fldir)==False:
		os.mkdir(fldir)
	count_sum_data_path=fldir+'/'+flname+'-result.csv'
	fl_type = flname.split('-')
	#-d 替代了
	#if cmp(fl_type[1].upper(),'BP')==0:
	#	go_type='BP'
	#elif cmp(fl_type[1].upper(),'CC')==0:
	#	go_type='CC'
	#elif cmp(fl_type[1].upper(),'MF')==0:
	#	go_type='MF'
	csv_result_descendant_ancestor_path = './../result/'+go_type+'_descendant_ancestor.csv'

	goid_gene_count_data = setCsv(1,',',goid_gene_count_path)
	#print goid_gene_count_data
	
	go_ancestor_data = [] 
	
	if goid_check_path != '':
		go_ancestor_data_csv = setCsv(0,',',goid_check_path)
		for line in go_ancestor_data_csv :
			if len(line) >0 :
				go_ancestor_data.append(line[0])
				
	if len(go_ancestor_data) == 0:
		print "don't find go id|terms in file:" + go_file
		sys.exit()
	
	##-g 文件行不能重复
	go_ancestor_data_unique = list(set(go_ancestor_data))
	
	if len(go_ancestor_data_unique) < len(go_ancestor_data):
		print "Notice-----------------------------------------------"
		print "the file:%s contains repeat goid or goterm row" % go_file
		print "-----------------------------------------------------"
		sys.exit()
				
	##整理
	goid_check_data = []
	for goid in go_ancestor_data:
		goid_check_data.append([goid])
	
	if goid_descendant_ancestor_path=='':
	
		#对-g 文件中每一行goid/goterm 找出其子孙，生成文件	
		if conf_settings_path=='':
			print "settings file can not configure"
			sys.exit()
		
		csv_result_descendant_ancestor_data = []
		csv_result_descendant_ancestor_data = getDescendantByGOIDFromDb(map_go_type,go_ancestor_data,conf_settings_path)
		savDataToFile(['DESCENDANT','ANCESTOR'],csv_result_descendant_ancestor_data,csv_result_descendant_ancestor_path)	
		goid_descendant_ancestor_path = csv_result_descendant_ancestor_path
	
	goid_descendant_ancestor_data = setCsv(1,',',goid_descendant_ancestor_path)
	
	
	col_count=1##goterm:1,goid:2 count所在原始数据中的列
	tle_lst=[]
	#print goid_check_data
	count_sum_data=countGOAncestor(goid_gene_count_data,goid_descendant_ancestor_data,goid_check_data,col_count)
	if cmp(map_go_type.upper(),'GOID')==0:
		tle_lst=['goid','sum']
	elif cmp(map_go_type.upper(),'GOTERM')==0:
		tle_lst=['goterm','sum']
	savDataToFile(tle_lst,count_sum_data,count_sum_data_path)


if __name__=='__main__':
	if len(sys.argv) <=1:
		print "please use option -h"
	else :
		main(sys.argv[1:])
