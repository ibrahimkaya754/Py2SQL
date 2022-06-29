# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 15:58:45 2022

@author: ibrahim.kaya
"""

from mysql.connector import connect
import pandas as pd
import numpy as np
from datetime import datetime

class Database():
    def __init__(self,DBname,host="127.0.0.1",user="admin",password="123456",
                 createNewDB= False):
        self.host = host
        self.user = user
        self.password = password
        self.DBname = DBname
        if createNewDB:
            self.createDB()
        #self.createTable(Tablename=["customer","customer_","_customer_"])
        
    
    ### createDB is for creating new Database with DBname ###
    def createDB(self):
    
        mydb = connect(host= self.host,
                       user= self.user,
                       password= self.password)
    
        mycursor = mydb.cursor()
    
        mycursor.execute("SHOW DATABASES")
    
        existingDB = []
    
        for db in mycursor:
            existingDB.append(list(db))
            #print(existingDB[-1])
    
        print("The databases existing at the host: ",existingDB)
        #print([str(self.DBname).lower()])
        #print([str(self.DBname).lower()] in existingDB)
    
        cntr = 0
        for elem in existingDB:
            if elem[0].lower() == str(self.DBname).lower():
                cntr = cntr + 1
                
        if cntr != 0:
                print("Database Exists with the name %s" % (str(self.DBname)))
        else:    
            order = "CREATE DATABASE " + str(self.DBname)
            mycursor.execute(order)
            print("Database %s has been created" %(self.DBname))
        
    ### Creating New Tables for existing Database with Tablename ###
    ### This module just creates the tables and the column names given as argumen in ""Columns" ###
    ### DateTime Column is created automatically ###
    def createTable(self,Tablename,PrimaryKey,Columns,DataType):
        mydb = connect(host= self.host,
                       user= self.user,
                       password= self.password,
                       database = self.DBname)
        
        mycursor = mydb.cursor()
        
        ### The very 1st thing is to check the table names ###
        mycursor.execute("SHOW TABLES")
        mytables = []
        for tbl in mycursor:
            mytables.append(list(tbl)[0]) #table returns are tuples, so we need to make them list. 
            
        for tbl,prim in zip(Tablename,PrimaryKey):
            if tbl in mytables:
                print("%s table exists" % (tbl))
            else:
                primaryKeyCreator = " (%s INT AUTO_INCREMENT PRIMARY KEY)" % (prim) ## Appending primary key
                sqlOrder = "CREATE TABLE "+tbl+primaryKeyCreator
                mycursor.execute(sqlOrder)
                print("%s table has been created with primary key %s" % (tbl,prim))
        
        for tbl,col,dtype in zip(Tablename,Columns,DataType):
            for ii in range(len(col)):
                ColumnCreater = " ADD COLUMN %s " % (col[ii]) ## Appending columns given as arguments
                sqlOrder = "ALTER TABLE "+tbl+ColumnCreater+dtype[ii]
                print(sqlOrder)
                mycursor.execute(sqlOrder)
                
        for tbl in Tablename:
            DateTimeColumn = " ADD COLUMN %s " % ("DateTime") ## Appending DateTime Column 
            sqlOrder = "ALTER TABLE "+tbl+DateTimeColumn+"VARCHAR(255)"  
            print(sqlOrder)
            mycursor.execute(sqlOrder)
            
              
    ### This function provides the columns of the tables ###
    def showtables(self,Tablename):
        self.mydb = connect(host= self.host,
                       user= self.user,
                       password= self.password,
                       database = self.DBname)
        
        mycursor = self.mydb.cursor()
        
        ### The very 1st thing is to check the table names ###
        mycursor.execute("SHOW COLUMNS FROM "+Tablename+" IN "+self.DBname)
        columns = []
        for col in mycursor:
            #print(list(col)[0])
            columns.append(list(col)[0])
        #print("Existing Columns are: ",columns)
        return columns
      
    ### This is the main functional sub-module which provides logging newly acquired data ###
    ### The user should give the data (as Values argument) as numpy array ### 
    def insert2Table(self,Tablename,Values):
        now = datetime.now()
        datetimestring = now.strftime("%d/%m/%Y %H:%M:%S")
        date = datetimestring[0:10]
        time = datetimestring[11:]
        vals    = Values
        columns = self.showtables(Tablename)
        print(columns)
        
        ### Appending the columns existing inside the Tablename to cols ###
        cols = "("
        wildcard = "("
        for c in columns[1:]:
            cols = cols+c+","
            wildcard = wildcard + "%s,"
        cols = cols[:-1]+")"
        wildcard = wildcard[:-1]+")"
        print("Existing Columns in table '%s' are: %s " % (Tablename,cols))
        
        ##############################################
        
        mycursor = self.mydb.cursor()
        for elem in vals:
            elem = list(elem)
            elem.append(datetimestring)
            sqlOrder= "INSERT INTO "+Tablename+" "+cols+" VALUES "+wildcard
            mycursor.execute(sqlOrder,list(elem))
            self.mydb.commit()
        
        mycursor.execute("SELECT COUNT("+columns[0]+") FROM "+Tablename)
        rows = mycursor.fetchall()        
        print("number of total data: ",rows[0][0])
        
    ##########################################################
    
my9db = Database(createNewDB=True,DBname="my9thDB")
my9db.createTable(["table1","table2","table3"],
                  ["pk1","pk2","pk3"],
                  [["col1"],["col1","col2"],["col1","col2","col3"]],
                  [["varchar(255)"],["varchar(255)","varchar(255)"],["varchar(255)","varchar(255)","varchar(255)"]])
mydummyarray1= np.random.rand(20,1)
mydummyarray2= np.random.rand(20,2)
mydummyarray3= np.random.rand(20,3)
my9db.insert2Table("table1", mydummyarray1)
my9db.insert2Table("table2", mydummyarray2)
my9db.insert2Table("table3", mydummyarray3)
