Sample Code Package contains 3 main components:
ExecuteSQL.xml --> Spring configuration where the data injection and SQL connection take place
ExecuteSQLByXMLTasklet.java --> Main Java code
SQL_Input.xml --> Input file that will be injected and used in the java code

Functionality/Intended Use:
The program will validate and run SQL queries which are specified in the SQL_Input.xml. SELECT statement will run,
but not the intended purpose of this program.

How to use:
--> Modify SQL_Input.xml: user can add as many queries as they like as long as each query has a unique key
--> Run ExecuteSQLByXMLTasklet.java

Summary of ExecuteSQLByXMLTasklet.java:
--> Generate SQL queries from the XML input file
--> Move the input file to archive
	--> Archive location = input location + "_Archive"
	--> Archive input name = input name + timestamp up to the seconds
	--> Delete original input file to prevent unintended run
--> Run each queries against the specified SQL database
--> Retrieve results in log

Note: Several critical components are omitted for simplicity. This sample program will not run because Spring
configurations are omitted and other required classes are removed.
