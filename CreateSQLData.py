"""
Created on Tue Mar 29 15:58:45 2022

BASICALLY DATA IN CSV FILE IS UPLOADED TO DATABASE ON MYSQL.
WE CREATE AN INSTANCE AND MAKE THE CONNECTION WITH THE DATABASE AT SERVER.
DATA UPLOADING CAN BE SLOW (MOST PROBABLY PROBLEMATIC) IS ALL THE DATA IS TRIED TO BE SENT TOTALLY.
TO PREVENT IT, WE DIVIDE THE TOTAL DATA TO PACKAGES OF SMALL DATA AT SIZEs 1000.

@author: ibrahim.kaya
"""

from mysql.connector import connect
from mysql.connector import Error
import pandas as pd
import numpy as np
from datetime import datetime
import time

class Database():
    def __init__(self,DBname,host="127.0.0.1",user="DB01",password="123456",port="3306"):
        """
        This is the Database Class where we make the initial 
        connection with the MySQL server.
        Args:
            DBname      : (str) Optional - Database Name
            host        : "127.0.0.1" - localhost
            user        : (str) Username
            password    : (str) Password for the connection
        """
        self.host     = host
        self.user     = user
        self.password = password
        self.DBname   = DBname
        self.port     = port
        self.__connect2Server()
        self.__connect2DataBase()
        self.showtables()

        ExampleUsage =  """
        ### EXAMPLE USAGE of MODULE - COPY AND PASTE THE RELATED PARTS ###
        
        ##CONNECT TO SERVER AND DATABASE
        mydb  = Database(DBname="IOTDB")

        ##ADD DATA TO DATABASE
        mydb.insertRawWifiData() 

        ## FETCH DATA FROM DATABASE
        query = "select * from ALLInfo where DateTime>='2022-04-03 10:00' AND DateTime<='2022-04-05 10:00'"
        mydb.mycursor.execute(query)
        mydb.fetchedData   = mydb.mycursor.fetchall()
        mydb.fetchedDataDF = pd.DataFrame(mydb.fetchedData)

        ## TO CHANGE THE COLNAMES TO THE ONES EXISTING ON THE DATABASE
        colnameDict = {ii:jj for ii,jj in zip(list(mydb.fetchedDataDF.columns),list(mydb.columns.values())[0])}
        mydb.fetchedDataDF.rename(columns=colnameDict,inplace=True)
        """   

        print(ExampleUsage)

    def __connect2Server(self):
        """
        This function tries to make the connection with the server.
        """
        try:
            self.srvr = connect(host=self.host,
                                user = self.user,
                                password = self.password,
                                port = self.port)
            print("Succesful Connection to Server at Host: %s" % (self.host))
            self.mysrvr = self.srvr.cursor()
            self.mysrvr.execute("SHOW DATABASES")
            print("Existing Databases at the server are: ")
            [print(ii) for ii in self.mysrvr]                
        except Error as err:
            print(f"Error: '{err}'")
            print("No Succesful Connection to Server at Host: %s:%s" % (self.host,self.port))
    
    def __connect2DataBase(self):
        """
        This function tries to make the connection with the database on the server.
        """
        try:
            self.mydb = connect(host=self.host,
                                user = self.user,
                                password = self.password,
                                database= self.DBname,
                                port=self.port)
            print("Succesful Connection to DataBase %s on Server at Host: %s:%s" % (self.DBname,self.host,self.port))
            self.mycursor  = self.mydb.cursor()
        except Error as err:
            print(f"Error: '{err}'")
            print("No Succesful Connection to DataBase %s on Server at Host: %s:%s" % (self.DBname,self.host,self.port))
                          
    def showtables(self):
        """
        This function provides the columns of the tables 
        The very 1st thing is to check the table names 
        """
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
        
        #now            = datetime.now()
        #datetimestring = now.strftime("%Y-%m-%d %H-%M-%S")
        AllDataPd      = pd.read_csv(dataLoc)
        AllDataPd.rename(columns={'Unnamed: 0':'DataID'},inplace=True)

        def ParseDateTime(ii):

            date_ = AllDataPd["Burst_name"].values[ii][5:11]
            time_ = AllDataPd["Burst_name"].values[ii][12:18]
            datetimestring = date_[0:2]+"-"+date_[2:4]+"-"+date_[4:]+" "+time_[0:2]+"-"+time_[2:4]+"-"+time_[4:]
            return datetimestring
        
        self.showtables()

        table          = Tablename
        numberOfSeq    = len(AllDataPd.values) // 1000
        print("WE ARE SENDING DATA AS PACKAGES OF SIZE 1000")
        start_time     = time.time()
        counter        = 0
        for seq in range(numberOfSeq):
            init = seq*1000
            end  = (seq+1)*1000
            sqlOrder       = "INSERT INTO "+table+" (File_no,Burst_no,SNRs,Burst_start_offset,Burst_end_offset,Burst_duration_microsec,CFO,receiver_address,transmitter_address,mac_frame_type,format_,Burst_name,File_name,SdrPozisyonID,CihazPozisyonID,SdrID,DateTime) VALUES "
            for ii in AllDataPd.values[init:end]:
                order          = list(ii[1:])
                datetimestring = ParseDateTime(counter)
                order.append(datetimestring)
                sqlOrder       = sqlOrder+str(tuple(order))+","
                counter        = counter + 1
            self.mycursor.execute(sqlOrder[:-1])
            self.mydb.commit()
            print("PACKAGE %s HAS BEEN UPLOADED" % (seq))

        ## The last portion of data will be sent to MySQLDB
        sqlOrder       = "INSERT INTO "+table+" (File_no,Burst_no,SNRs,Burst_start_offset,Burst_end_offset,Burst_duration_microsec,CFO,receiver_address,transmitter_address,mac_frame_type,format_,Burst_name,File_name,SdrPozisyonID,CihazPozisyonID,SdrID,DateTime) VALUES "
        for ii in AllDataPd.values[end:]:
            order          = list(ii[1:])
            datetimestring = ParseDateTime(counter)
            order.append(datetimestring)
            sqlOrder       = sqlOrder+str(tuple(order))+","
            counter        = counter + 1
        self.mycursor.execute(sqlOrder[:-1])
        self.mydb.commit()

        print("--- Data Loading to MySQL DB is: %s seconds ---" % (time.time() - start_time))

    def fetchData(self,Tablename="ALLInfo",Column="DateTime",Condition=">1955000"):
        """
        This is the function to fetch data with a condition.
        Args:
            Tablename : The table where the data will be fetched
            Column    : Column inside the table where the condition will be related to
            Condition : Condition to fetch the data
        """
        tablename = Tablename
        column    = Column
        condition = Condition
       
        query     = "select * from "+tablename+" where "+column+condition
        print("The '%s' query has been sent to MySQL database" % (query))
        self.mycursor.execute(query)
        
        self.fetchedData   = self.mycursor.fetchall()
        self.fetchedDataDF = pd.DataFrame(self.fetchedData)

        ## Below we change the colnames of DataFrame to the names existing inside the Database.
        colnameDict = {ii:jj for ii,jj in zip(list(self.fetchedDataDF.columns),list(self.columns.values())[0])}
        self.fetchedDataDF.rename(columns=colnameDict,inplace=True)


   


