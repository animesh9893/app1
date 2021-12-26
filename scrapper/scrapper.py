# package

import requests
from bs4 import BeautifulSoup
import mysql.connector
import urllib.parse
from datetime import datetime
import validators
import pandas as pd

# code

links = None # list for googleLinks
mydb,mycursor = None,None # object to use database
# list of paper for select few
papers = ["Business Insider India","CNBCTV18","Geo News","India Today","Moneycontrol","Moneycontrol.com","Nasdaq","NDTV","The Hindu","The Indian Express","The White House","Times Now"]

# functions

# it will replace the space with given prams
def replaceSpace(string,replace="_"):
	result = ""
	for i in string:
		if i==" ":
			result+=replace
		else:
			result+=i
	return result

# it will replace the space with given prams
def replaceString(string,replace=" ",replaceWith="_"):
	result = ""
	for i in string:
		if i==replace:
			result+=replaceWith
		else:
			result+=i
	return result

# it will return query which can create table
def createTable(table_name,params):
	# params = [[col_name,type,other],[.....]]
	query = "CREATE TABLE "+table_name+" ( "
	for col in params:
		q = ""
		for i in col:
			q+=i+" "
		query+=q+" ,"
	return query[:-1]+" );"

# it will genrate new id for table
def genrateId(table_name):
	global mycursor
	prefix = 1
	d = int(datetime.today().strftime('%m%d'))
	query = "SELECT id from "+table_name+";"
	mycursor.execute(query)
	result = mycursor.fetchall()
	if len(result)!=0:
		for i in result:
			if int(i[0])>prefix:
				prefix=int(i[0])
	d*=1000
	return d+prefix

# it will connect to database
def connectDatabase():
	global mydb
	global mycursor

	mydb = mysql.connector.connect(
	  host="localhost",
	  user="animesh",
	  password="15072002",
	  database="news_scrapper"
	)
	mycursor = mydb.cursor()

# it wll update the links from google link database
def updateLinks():
	global mycursor
	global links
	mycursor.execute("SELECT * FROM googleLink;")
	links = mycursor.fetchall()

#  adding links to googleLink
def addLinks(title=None,URL=None):
	global mycursor
	global mydb
	if URL==None:
		URL = "https://news.google.com/search?q="+replaceSpace(title,"%20")

	# if title is too long so it sort it
	if len(title)>30:
		result = title[0]
		for i in range(0,len(title)):
			if title[i]==" " and title[i+1]!=" " and title[i]!=None:
				result +=title[i+1]
		title = result

	mycursor.execute("SELECT id from googleLink");
	ids = max(mycursor.fetchall())[0]+1
	mycursor.execute("INSERT INTO googleLink values(%s,%s,%s)",(title,URL,ids))
	mydb.commit()
	updateLinks() # update link list in local variable
	genrateTable() # genrate table for newly added title

# it will genrate table to new topic
def genrateTable():
	global mycursor
	global mydb
	mycursor.execute("SELECT title from googleLink where table_name is NULL;");
	result = mycursor.fetchall()
	for i in result:
		table_name = replaceSpace(i[0],"_").lower()
		query = createTable(table_name,[
			["id","int"],
			["headline","varchar(1000)","NOT NULL"],
			["synopsis","varchar(1000)"],
			["image","varchar(1000)"],
			["content","text"],
			["link","text"],
			["paper","varchar(400)"]
		])
		mycursor.execute(query)
		query = "UPDATE googleLink SET table_name= '"+table_name+"' WHERE title='"+i[0]+"';"
		mycursor.execute(query)
		mydb.commit()

# insert data into tables
def insertGoogleNews(table_name,data):
	global mycursor
	global mydb
	
	values = ""
	for i in data:
		try:
			query = "INSERT INTO "+table_name+'(headline, paper, link, id) VALUES '
			query += "( \""+i[0]+"\", \""+i[1]+"\",\""+i[2]+"\","+str(i[3])+");"
			mycursor.execute(query)
			mydb.commit()
		except:
			print("erorr in inserting news")

# extract data
def GoogleNews(URL,_id):
	global papers
	page = requests.get(URL)
	# parse bs4
	soup = BeautifulSoup(page.content, 'html.parser')
	# finding article block
	result = soup.find_all(class_='EjqUne')
	data = []
	# seprate / filter news
	for i in result:
		title = i.find(class_="DY5T1d RZIKme").text
		paper = i.find(class_="wEwyrc AVN2gc uQIVzc Sksgp").text
		link  = "https://news.google.com"+(i.find(class_="DY5T1d RZIKme")["href"])[1:]
		# link  = (i.find(class_="DY5T1d RZIKme")["href"])[1:]
		if not validators.url(link):
			print("not valid")
			print(link)
		if paper in papers:
			data.append((replaceString(title,'"',r'\"'),replaceString(paper,'"',r'\"'),replaceString(link,'"',r'\"'),_id))
	return data

# it will add google news into database
def scrapeGoogleNews():
	global links
	updateLinks()
	genrateTable()
	# geting data
	for i in links:
		data=GoogleNews(i[1],genrateId(i[3]))
		insertGoogleNews(i[3],data)

# it will update the available paper table by list
def updatePaperTable():
	global papers
	global links
	global mycursor
	global mydb
	l = []
	d = {}
	for i in links:
		query = "SELECT paper,link from "+i[3]+";"
		mycursor.execute(query)
		result = mycursor.fetchall()
		for i in result:
			if i[0] in papers:
				d[i[0]] = i[1]
				l.append(i[0])
	l = list(set(l))
	print(len(d))
	for i in l:
		# try:
			query = "INSERT INTO paper (title,link) values (\""+i+"\",\""+d[i]+"\");"
			mycursor.execute(query)
		# except:
		# 	pass
	mydb.commit()


# reset tables the news table
def resetNewsTable():
	global links
	global mycursor
	global mydb
	for i in links:
		query = "DELETE from "+i[3]+";"
		mycursor.execute(query)
		mydb.commit()

# delete news table
def deleteTable():
	global links
	global mycursor
	global mydb
	for i in links:
		try:
			query = "DROP TABLE "+i[3]+";"
			mycursor.execute(query)
			mydb.commit()
		except:
			pass
	query = "UPDATE googleLink set table_name=NULL;"
	mycursor.execute(query)
	mydb.commit()

def scanPaper(data):
	global mycursor
	query = "SELECT * from scrapper_query where paper=\""+data['paper']+"\";"
	mycursor.execute(query)
	result = {}
	t = mycursor.fetchall()[0]
	result["headline"] = t[1]
	result["synopsis"] = t[2]
	result["timestamp"] = t[3]
	result["image"] = t[4]
	result["content"] = t[5]

	page = requests.get(data["link"])
	soup = BeautifulSoup(page.content, 'html.parser')
	loc = {"soup":soup}

	response = {}
	for i in result.keys():
		if i != None:
			code = "res = "+str(result[str(i)])
			exec(code,{},loc)
			response[i] = loc["res"]
	return response

def gv():
	global mycursor
	query = "SELECT * from paper;"
	mycursor.execute(query)
	result = mycursor.fetchall()
	df = pd.DataFrame(result,columns=["id","title","link"])
	df.to_csv("paper.csv")


if __name__ == '__main__':
	connectDatabase()
	updateLinks()
	genrateTable()
	resetNewsTable()
	updatePaperTable()
	