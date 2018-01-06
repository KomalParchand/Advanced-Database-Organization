ASSIGNMENT 3 - RECORD MANAGER

TEAM - 05

TEAM MEMBERS :
1)Indranil Thakur (A20377114)
2)Akshay Chaudhari (A20380588)
3)Komal Parchand (A20373541)

<<<<<<<<<<<  CONTENTS	>>>>>>>>>>>>>>>
1. Problem statement
2. Steps to execute the program
3. Function description



<<<<<<<<<<<<<  PROBLEM STATEMENT	>>>>>>>>>>>>>

The record manager is used to handle table which can insert records, delete records, update records, and scan through the records.


<<<<<<<<<<<<	STEPS TO EXECUTE THE PROGRAM	>>>>>>>>>>>>

1. In the Linux terminal, navigate to the directory of your assignemnt using "cd" command
2. Type the commmand "make" 



<<<<<<<<<<<<	FUNCTION DESCRIPTION	>>>>>>>>>>>>


|initRecordManager| 
===================
This function is used to initialize the memory for record manager 

|shutdownRecordManager| 
=======================
This function is used to free all the memory used in record manager.

|createTable| 
=============
This function passes schema to serialize the schema.

|openTable| 
===========
This function opens the file in which name parameter is passed.
Buffer Pool is intialise and first page is pinned from file
the schema structure is stored in RM_TableData structure.

|closeTable| 
============
This function calls shutdownBufferPool and frees the memory from RM_TableData structure


|deleteTable| 
=============
This function calls destroyPageFile function that deletes the table specified as name parameter.
	
|getNumTuples| 
==============
This function returns the total number of tuples count in table
	

|insertRecord| 
==============
This function us used for inserting the record in the RM_datatable and Rid is assigned

|deleteRecord| 
==============
This function is used to delete the record from table as per rid for this the tombstone flag needs to be prefixed

|updateRecord| 
==============
This function is used to update record in RM_tabledata. If tombstone is prefixed then record cannot be upadated

|getRecord| 
===========
This function is used for retrival of record for  specific Rid from RM_datatable and it is stored in record parameter

|startScan| 
===========
This function is used for initialising RM_scanhandle and RM_info structure
	
|next| 
======
This function searches the tuples on basis of scearch i.e scan condition and if its returns NULL then tuple is scanned till end of table 

|closeScan|
===========
This function returns that the all resources used are deleted and resources are free

|getRecordSize| 
===============
The  function returns size of records

|createSchema|
==============
This function is used to create a schema.Memory and values is initalise

|freeSchema| 
============
This function is used to free the memory assigned to the schema

|createRecord|
==============
This function is used to create a record and memory is initalise

|freeRecord| 
============
This function is used to free the memory assigned to the record

|setAttr| 
=========
This function sets the value in the record 

|getAttr| 
=========
This function gets the value of the record 



