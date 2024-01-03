from DBMS8 import RDBMS

db = RDBMS()

# CREATE THE NECESSARY PRE_LOADED TABLES
state1 = "CREATE TABLE ii_1000 (Int Integer, Int2 Integer, PRIMARY KEY (Int));"
db.parse_sql(state1)
# INSERT VALUES
ii_1000 = []
for i in range(1,1001):
    ii_1000.append((i,i))
for i in ii_1000:
    db.insert("ii_1000", i)

state = "CREATE TABLE i1_1000 (Int Integer, NumOne Integer, PRIMARY KEY (Int), FOREIGN KEY (Int) REFERENCES ii_1000(Int));"
db.parse_sql(state)
i1_1000 = []
for i in range(1,1001):
    i1_1000.append((i,1))
for i in i1_1000:
    db.insert("i1_1000", i)

db.create_table("ii_10000", {'Int1': 'Integer','Int2': 'Integer'})
ii_10000 = []
for i in range(1,10001):
    ii_10000.append((i,i))
for i in ii_10000:
    db.insert("ii_10000", i)

db.create_table("i1_10000", {'Int': 'Integer','NumOne': 'Integer'})
i1_10000 = []
for i in range(1,10001):
    i1_10000.append((i,1))
for i in i1_10000:
    db.insert("i1_10000", i)

# Pre-made Employee Details Table

employee_details_create = "CREATE TABLE EmployeeDetails (ID INTEGER, Name STRING, Department STRING, Salary FLOAT, PRIMARY KEY (ID)"

db.parse_sql(employee_details_create)

detail_insert1 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (1, 'John Doe', 'HR', 50000)"

detail_insert2 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (2, 'Jane Smith', 'Marketing', 48000)"

detail_insert3 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (3, 'Bob Johnson', 'Finance', 55000)"

detail_insert4 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (4, 'Mary Wilson', 'Engineering', 62000)"

detail_insert5 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (5, 'David Brown', 'IT', 58000)"

detail_insert6 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (6, 'Lisa Jackson', 'Marketing', 49000)"

detail_insert7 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (7, 'Michael Jones', 'Finance', 56000)"

detail_insert8 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (8, 'Susan Miller', 'Engineering', 63000)"

detail_insert9 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (9, 'Richard Davis', 'IT', 59000)"

detail_insert10 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (10, 'Jennifer White', 'Marketing', 50000)"

detail_insert11 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (11, 'William Moore', 'Finance', 57000)"

detail_insert12 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (12, 'Patricia Harris', 'Engineering', 64000)"

detail_insert13 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (13, 'James Thomas', 'IT', 60000)"

detail_insert14 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (14, 'Elizabeth Martin', 'Marketing', 51000)"

detail_insert15 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (15, 'John Wilson', 'Finance', 58000)"

detail_insert16 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (16, 'Sarah Anderson', 'Engineering', 65000)"

detail_insert17 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (17, 'Robert Lewis', 'IT', 61000)"

detail_insert18 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (18, 'Linda Garcia', 'Marketing', 52000)"

detail_insert19 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (19, 'Daniel Martinez', 'Finance', 59000)"

detail_insert20 = "INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (20, 'Karen Hernandez', 'Engineering', 66000)"

db.parse_sql(detail_insert1)

db.parse_sql(detail_insert2)

db.parse_sql(detail_insert3)

db.parse_sql(detail_insert4)

db.parse_sql(detail_insert5)

db.parse_sql(detail_insert6)

db.parse_sql(detail_insert7)

db.parse_sql(detail_insert8)

db.parse_sql(detail_insert9)

db.parse_sql(detail_insert10)

db.parse_sql(detail_insert11)

db.parse_sql(detail_insert12)

db.parse_sql(detail_insert13)

db.parse_sql(detail_insert14)

db.parse_sql(detail_insert15)

db.parse_sql(detail_insert16)

db.parse_sql(detail_insert17)

db.parse_sql(detail_insert18)

db.parse_sql(detail_insert19)

db.parse_sql(detail_insert20)




# Pre-made Employee Contact Table


employee_contact_create = "CREATE TABLE EmployeeContactInfo (ID INTEGER, Email STRING, Phone String, PRIMARY KEY (Email), FOREIGN KEY (ID) REFERENCES EmployeeDetails(ID));"

db.parse_sql(employee_contact_create)

contact_insert1 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (1, 'john.doe@example.com', '(555) 123-4567')"

contact_insert2 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (2, 'jane.smith@example.com', '(555) 234-5678')"

contact_insert3 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (3, 'bob.johnson@example.com', '(555) 345-6789')"

contact_insert4 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (4, 'mary.wilson@example.com', '(555) 456-7890')"

contact_insert5 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (5, 'david.brown@example.com', '(555) 567-8901')"

contact_insert6 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (6, 'lisa.jackson@example.com', '(555) 678-9012')"

contact_insert7 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (7, 'michael.jones@example.com', '(555) 789-0123')"

contact_insert8 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (8, 'susan.miller@example.com', '(555) 890-1234')"

contact_insert9 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (9, 'richard.davis@example.com', '(555) 901-2345')"

contact_insert10 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (10, 'jennifer.white@example.com', '(555) 012-3456')"

contact_insert11 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (11, 'william.moore@example.com', '(555) 123-4567')"

contact_insert12 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (12, 'patricia.harris@example.com', '(555) 234-5678')"

contact_insert13 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (13, 'james.thomas@example.com', '(555) 345-6789')"

contact_insert14 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (14, 'elizabeth.martin@example.com', '(555) 456-7890')"

contact_insert15 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (15, 'john.wilson@example.com', '(555) 567-8901')"

contact_insert16 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (16, 'sarah.anderson@example.com', '(555) 678-9012')"

contact_insert17 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (17, 'robert.lewis@example.com', '(555) 789-0123')"

contact_insert18 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (18, 'linda.garcia@example.com', '(555) 890-1234')"

contact_insert19 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (19, 'daniel.martinez@example.com', '(555) 901-2345')"

contact_insert20 = "INSERT INTO EmployeeContactInfo (ID, Email, Phone) VALUES (20, 'karen.hernandez@example.com', '(555) 012-3456')"

db.parse_sql(contact_insert1)

db.parse_sql(contact_insert2)

db.parse_sql(contact_insert3)

db.parse_sql(contact_insert4)

db.parse_sql(contact_insert5)

db.parse_sql(contact_insert6)

db.parse_sql(contact_insert7)

db.parse_sql(contact_insert8)

db.parse_sql(contact_insert9)

db.parse_sql(contact_insert10)

db.parse_sql(contact_insert11)

db.parse_sql(contact_insert12)

db.parse_sql(contact_insert13)

db.parse_sql(contact_insert14)

db.parse_sql(contact_insert15)

db.parse_sql(contact_insert16)

db.parse_sql(contact_insert17)

db.parse_sql(contact_insert18)

db.parse_sql(contact_insert19)

db.parse_sql(contact_insert20)

# ****QUERIES****
# --CREATE TABLE--
# CREATE TABLE EmployeeDetailsSample (ID INTEGER, Name STRING, Department STRING, Salary FLOAT, PRIMARY KEY (ID)
# INSERT INTO EmployeeDetailsSample (ID, Name, Department, Salary) VALUES (1, 'John Doe', 'HR', 50000)
# INSERT INTO EmployeeDetailsSample (ID, Name, Department, Salary) VALUES (2, 'Alex Henry', 'Marketing', 45000)
# SELECT * FROM EmployeeDetailsSample

# --W/ FOREIGN KEY--
# CREATE TABLE Projects (ProjectID INTEGER, EmployeeID Integer, PRIMARY KEY (ProjectID), FOREIGN KEY (EmployeeID) REFERENCES EmployeeDetailsSample(ID))
# INSERT INTO Projects (ProjectID, EmployeeID) VALUES (20, 1)
# SELECT ProjectID, EmployeeID FROM Projects

# FOREIGN KEY Violation
# INSERT INTO Projects (ProjectID, EmployeeID) VALUES (15, 3)
# PRIMARY KEY Violation
# INSERT INTO Projects (ProjectID, EmployeeID) VALUES (20, 2)

# --Preloaded Tables--
# SELECT ID, Name, Department, Salary FROM EmployeeDetails
# SELECT ID, Email, Phone FROM EmployeeContactInfo

# Testing Primary Key
# INSERT INTO EmployeeDetails (ID, Name, Department, Salary) VALUES (17, 'John Doe', 'HR', 50000)

# --Aggregate Operators--
# SELECT MAX(Salary) FROM EmployeeDetails (correct number = 66000.0)
# SELECT MIN(Salary) FROM EmployeeDetails (correct number = 48000.0)
# SELECT SUM(Salary) FROM EmployeeDetails (correct number = 1143000.0)
# SELECT AVG(Salary) FROM EmployeeDetails (correct number = 57150.0)
# SELECT COUNT(Name) FROM EmployeeDetails (correct number = 20)

# --WHERE and Logical Operators--
# SELECT ID, Name, Department FROM EmployeeDetails WHERE Department = 'Marketing'

# SELECT * FROM EmployeeDetails WHERE Salary > 55000 AND Department = 'Engineering'
# SELECT ID, Name, Department, Salary FROM EmployeeDetails WHERE Salary > 62000 OR Department = 'Marketing'
# SELECT ID, Name, Department, Salary FROM EmployeeDetails WHERE Salary > 62000 OR Department = 'Marketing' OR Name = 'Bob Johnson'
# SELECT ID, Name, Department, Salary FROM EmployeeDetails WHERE ID < 10 AND Salary < 58000 AND Department = 'Marketing'

# --DISTINCT--
# SELECT DISTINCT Department FROM EmployeeDetails

# --JOIN--
# SELECT EmployeeDetails.ID, EmployeeDetails.Name, EmployeeDetails.Department, EmployeeContactInfo.Email, FROM EmployeeDetails JOIN EmployeeContactInfo ON EmployeeDetails.ID = EmployeeContactInfo.ID
# --NATURAL JOIN--
# SELECT EmployeeDetails.ID, EmployeeDetails.Name, EmployeeDetails.Department, EmployeeDetails.Salary, EmployeeContactInfo.Email, EmployeeContactInfo.Phone FROM EmployeeDetails NATURAL JOIN EmployeeContactInfo
# SELECT EmployeeDetails.ID, EmployeeDetails.Name, EmployeeDetails.Department, EmployeeContactInfo.Email, FROM EmployeeDetails JOIN EmployeeContactInfo ON EmployeeDetails.ID = EmployeeContactInfo.ID

# --UPDATE--
# UPDATE EmployeeDetails SET Department = 'IT' WHERE ID = 3
# UPDATE EmployeeDetails SET Department = 'Engineering' WHERE Name = 'John Doe' OR Salary < 50000

# --DELETE--
# DELETE FROM EmployeeDetails WHERE ID > 9
# DELETE FROM EmployeeDetails WHERE Department = 'Engineering' AND Salary < 60000

# **PRINT**
# SELECT ID, Name, Department, Salary FROM EmployeeDetails

# **CLASS TABLE QUERIES**
# Print each table
# SELECT Int, Int2 FROM ii_1000
# SELECT Int, NumOne FROM i1_1000
# SELECT Int1, Int2 FROM ii_10000
# SELECT Int, NumOne FROM i1_10000

# SELECT Int, NumOne FROM i1_10000 WHERE Int < 5

# --MERGE SORT JOIN-- (10,000 row & 10,000 row)
# SELECT ii_10000.Int1, ii_10000.Int2, i1_10000.NumOne FROM ii_10000 JOIN i1_10000 ON ii_10000.Int1 = i1_10000.Int

# --NESTED LOOP JOIN-- (10,000 row & 1,000 row)
# SELECT ii_1000.Int, ii_1000.Int2, ii_10000.Int1 FROM ii_1000 JOIN ii_10000 ON ii_1000.Int2 = ii_10000.Int2

# --INDICES-- (10,000 row & 1,000 row)
# SELECT ii_1000.Int, ii_1000.Int2, ii_10000.Int1 FROM ii_1000 JOIN ii_10000 ON ii_1000.Int = ii_10000.Int1

while True:
    query = input("Query: ")

    if query.lower() == 'exit':
        print("Exiting the program.")
        break  # Exits the loop and terminates the program

    db.parse_sql(query)
