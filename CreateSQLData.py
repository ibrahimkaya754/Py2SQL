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
import time

class Database():
    def __init__(self,DBname,host="127.0.0.1",user="DB01",
                      password="123456"):
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
        self.host     = host
        self.user     = user
        self.password = password
        self.DBname   = DBname
        self.__connect2Server()
        self.__connect2DataBase()

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
            self.mydb = connect(host=self.host,
                           user = self.user,
                           password = self.password,
                           database= self.DBname)
            print("Succesful Connection to DataBase %s on Server at Host: %s" % (self.DBname,self.host))
            
        except Error as err:
            print(f"Error: '{err}'")
            print("No Succesful Connection to DataBase %s on Server at Host: %s" % (self.DBname,self.host))
                          
    def showtables(self):
        """
        This function provides the columns of the tables 
        The very 1st thing is to check the table names 
        """
        self.mycursor  = self.mydb.cursor()
        self.mycursor.execute("SHOW TABLES")
        self.table = []
        self.columns = {}
        for tbl in self.mycursor:
            print("Existing Tables are: ")
            print(tbl)
            self.table.append(tbl)
            self.columns[str(tbl)] = []
        for tbl in self.table:
            self.mycursor.execute("SHOW COLUMNS FROM "+tbl[0]+" IN "+self.DBname)
            for col in self.mycursor:
                self.columns[str(tbl)].append(list(col)[0])
        for key,val in self.columns.items():
            print("Existing columns in table %s are:" % (key))
            print(val)
      
    def insertRawWifiData(self,Tablename="ALLInfo",dataLoc="../all_MAC.csv"):
        """
        This is the main functional sub-module which provides logging newly acquired data
        We are giving the values in .csv file (i.e.,allMac.csv)
        Args:
            Tablename : (str) AllInfo table consists raw wifi data before preprocessing is done
            dataLoc   : (str) is the addres of the csv file
        """
        start_time     = time.time()
        self.mycursor  = self.mydb.cursor()
        now            = datetime.now()
        datetimestring = now.strftime("%Y-%m-%d %H-%M-%S")
        AllDataPd      = pd.read_csv(dataLoc)
        AllDataPd.rename(columns={'Unnamed: 0':'DataID'},inplace=True)

        self.showtables()

        table          = Tablename
        now            = datetime.now()
        datetimestring = now.strftime("%Y-%m-%d %H-%M-%S")
        numberOfSeq = len(AllDataPd.values) // 1000
        res = len(AllDataPd.values) % 1000
        print("WE ARE SENDING DATA AS PACKAGES OF SIZE 1000")
        for seq in range(numberOfSeq):
            init = seq*1000
            end  = (seq+1)*1000
            sqlOrder       = "INSERT INTO "+table+" (File_no,Burst_no,SNRs,Burst_start_offset,Burst_end_offset,Burst_duration_microsec,CFO,receiver_address,transmitter_address,mac_frame_type,format_,Burst_name,File_name,SdrPozisyonID,CihazPozisyonID,SdrID,DateTime) VALUES "
            for ii in AllDataPd.values[init:end]:
                order          = list(ii[1:])
                order.append(datetimestring)
                sqlOrder= sqlOrder+str(tuple(order))+","

            self.mycursor.execute(sqlOrder[:-1])
            self.mydb.commit()
            print("PACKAGE %s HAS BEEN UPLOADED" % (seq))

        ## The last portion of data will be sent to MySQLDB

        for ii in AllDataPd.values[end:]:
            order          = list(ii[1:])
            order.append(datetimestring)
            sqlOrder= sqlOrder+str(tuple(order))+","

        self.mycursor.execute(sqlOrder[:-1])
        self.mydb.commit()

        print("--- Data Loading to MySQL DB is: %s seconds ---" % (time.time() - start_time))
        
mydb = Database(DBname="IOTDB")
mydb.insertRawWifiData()

