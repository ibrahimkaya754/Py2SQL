# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 15:58:45 2022

@author: ibrahim.kaya
"""

from mysql.connector import connect
from mysql.connector import Error
import pandas as pd
import numpy as np
from datetime import datetime

class Database():
    def __init__(self,DBname,host="127.0.0.1",user="DB01",
                      password="123456",createNewDB= False):
        """
        This is the Database Class where we make the initial 
        connection with the MySQL server.
        Args:
            DBname      : (str) Optional - Database Name
            host        : "127.0.0.1" - localhost
            user        : (str) Username
            password    : (str) Password for the connection
            createNewDB : (bool) True if a new DB is to be created
        """
        self.host = host
        self.user = user
        self.password = password
        self.DBname = DBname
        self.__connect2Server()
        if createNewDB:
            self.createDB()

    def __connect2Server(self):
        """
        This function tries to make the connection with the server.
        """
        try:
            self.srvr = connect(host=self.host,
                                user = self.user,
                                password = self.password)
            print("Succesful Connection to Server at Host: %s" % (self.host))
            self.mysrvr = self.srvr.cursor()
            self.mysrvr.execute("SHOW DATABASES")
            print("Existing Databases at the server are: ")
            [print(ii) for ii in self.mysrvr]                
        except Error as err:
            print(f"Error: '{err}'")
            print("No Succesful Connection to Server at host: %s" % (self.host))
    
    def __connect2DataBase(self):
        """
        This function tries to make the connection with the database on the server.
        """
        try:
            mydb = connect(host=self.host,
                           user = self.user,
                           password = self.password,
                           database= self.DBname)
            print("Succesful Connection to DataBase %s on Server at Host: %s" % (self.DBname,self.host))
            
        except Error as err:
            print(f"Error: '{err}'")
            print("No Succesful Connection to DataBase %s on Server at Host: %s" % (self.DBname,self.host))
        return mydb

    
    # def createDB(self):
    #     """
    #     This function creates a new Database at the connection.
    #     If the boolean createNewDB given at the instance is True, this
    #     function is called. 
    #     """
    
    #     self.mysrvr.execute("SHOW DATABASES")
    #     existingDB = []
    
    #     for db in self.mysrvr:
    #         existingDB.append(list(db))
    #         #print(existingDB[-1])
    
    #     print("The databases existing at the host: ",existingDB)    
        
    #     cntr = 0
    #     for elem in existingDB:
    #         if elem[0].lower() == str(self.DBname).lower():
    #             cntr = cntr + 1
                
    #     if cntr != 0:
    #             print("Database Exists with the name %s" % (str(self.DBname)))
    #     else:    
    #         order = "CREATE DATABASE " + str(self.DBname)
    #         self.mysrvr.execute(order)
    #         print("Database %s has been created" %(self.DBname))
        
  
    # def createTable(self,Tablename,PrimaryKey,Columns,DataType):
    #     """
    #     Creating New Tables for existing Database with Tablename.
    #     This module just creates the tables and the column names 
    #     given as argumen in ""Columns".
    #     DateTime Column is created automatically.
    #     Args:
    #         Tablename  : (list) name of the tables existing, given as str
    #         PrimaryKey : (list) name of the Primary Key of the table, given as str
    #         Columns    : (list) name of the columns (features or attributes), given as str
    #         DataType   : (type) data types of the columns (varchar(255), or INT, etc.)
    #     """

    #     mydb     = self.__connect2DataBase()
    #     mycursor = mydb.cursor()
        
    #     ### The very 1st thing is to check the table names ###
    #     mycursor.execute("SHOW TABLES")
    #     mytables = []
    #     for tbl in mycursor:
    #         mytables.append(list(tbl)[0]) #table returns are tuples, so we need to make them list. 
            
    #     for tbl,prim in zip(Tablename,PrimaryKey):
    #         if tbl in mytables:
    #             print("%s table exists" % (tbl))
    #         else:
    #             primaryKeyCreator = " (%s INT AUTO_INCREMENT PRIMARY KEY)" % (prim) ## Appending primary key
    #             sqlOrder = "CREATE TABLE "+tbl+primaryKeyCreator
    #             mycursor.execute(sqlOrder)
    #             print("%s table has been created with primary key %s" % (tbl,prim))
        
    #     for tbl,col,dtype in zip(Tablename,Columns,DataType):
    #         for ii in range(len(col)):
    #             ColumnCreater = " ADD COLUMN %s " % (col[ii]) ## Appending columns given as arguments
    #             sqlOrder = "ALTER TABLE "+tbl+ColumnCreater+dtype[ii]
    #             print(sqlOrder)
    #             mycursor.execute(sqlOrder)
                
    #     for tbl in Tablename:
    #         DateTimeColumn = " ADD COLUMN %s " % ("DateTime") ## Appending DateTime Column 
    #         sqlOrder = "ALTER TABLE "+tbl+DateTimeColumn+"VARCHAR(255)"  
    #         print(sqlOrder)
    #         mycursor.execute(sqlOrder)
            
              
    ### This function provides the columns of the tables ###
    def showtables(self,Tablename):
        ### The very 1st thing is to check the table names ###
        self.mycursor.execute("SHOW COLUMNS FROM "+Tablename+" IN "+self.DBname)
        columns = []
        for col in self.mycursor:
            columns.append(list(col)[0])
        return columns
      

    def insert2Table(self,Tablename,Values):
        """
        This is the main functional sub-module which provides logging newly acquired data ###
        The user should give the data (as Values argument) as numpy array
        Args:
            Tablename :
            Values    :
        """
        self.mydb      = self.__connect2DataBase()
        self.mycursor  = self.mydb.cursor()
        now            = datetime.now()
        datetimestring = now.strftime("%d/%m/%Y %H:%M:%S")
        vals           = Values
        columns        = self.showtables(Tablename)
        print(columns)
        
        ### Appending the columns existing inside the Tablename to cols #######
        cols = "("
        wildcard = "("
        for c in columns[1:]:
            cols = cols+c+","
            wildcard = wildcard + "%s,"
        cols = cols[:-1]+")"
        wildcard = wildcard[:-1]+")"
        print("Existing Columns in table '%s' are: %s " % (Tablename,cols))
        #######################################################################
        for elem in vals:
            elem = list(elem)
            elem.append(datetimestring)
            sqlOrder= "INSERT INTO "+Tablename+" "+cols+" VALUES "+wildcard
            self.mycursor.execute(sqlOrder,list(elem))
            self.mydb.commit()
        
        self.mycursor.execute("SELECT COUNT("+columns[0]+") FROM "+Tablename)
        rows = self.mycursor.fetchall()        
        print("number of total data: ",rows[0][0])
        #######################################################################
            
my9db = Database(createNewDB=False,DBname="my3rdDBfromSSH")
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
