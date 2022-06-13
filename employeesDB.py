import pymysql

# Fucntion used in option 1 to view Employees & Departments
def emp_dept():

    db = pymysql.connect(host = "localhost", user = "root", password = "root", db = "employees", cursorclass = pymysql.cursors.DictCursor)

    sql = "select e.name, d.name from employee e inner join dept d on d.did = e.did order by d.name, e.name"

    with db:
        cursor = db.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

# Fucntion used in option 2 to view salary details of employees
def find_salaries(eid):
    
    db = pymysql.connect(host = "localhost", user = "root", password = "root", db = "employees", cursorclass = pymysql.cursors.DictCursor)

    sql = "select format(min(s.salary), 2) as Minimum, format(avg(s.salary), 2) as Average, format(max(s.salary), 2) as Maximum from salary s inner join employee e on s.eid = e.eid where e.eid = %s"


    with db:
        cursor = db.cursor()
        cursor.execute(sql, eid)
        return cursor.fetchall()

# Fucntion used in option 3 to view the employees by month of birth
def find_dob(month):
    db = pymysql.connect(host = "localhost", user = "root", password = "root", db = "employees", cursorclass = pymysql.cursors.DictCursor)

    sql = "select eid, name, dob from employee where month(dob) = %s"

    with db:
        cursor = db.cursor()
        cursor.execute(sql, month)
        return cursor.fetchall()

# Fucntion used in option 4 to add a new employee to the DB as long as employee ID doesn't already exist and the department exists
def add_employee(eid, name, dob, did):

    db = pymysql.connect(host = "localhost", user = "root", password = "root", db = "employees", cursorclass = pymysql.cursors.DictCursor)

    sql = "INSERT INTO employee VALUES (%s, %s, %s, %s)"

    with db:
        try:
            cursor = db.cursor()
            cursor.execute(sql, (eid, name, dob, did))
            db.commit()           
        except pymysql.err.IntegrityError as e:
            print(e.args[0])
            if e.args[0] == 1062:
                print("")
                print(f"*** ERROR ***: {eid} already exists")
            elif e.args[0] == 1452:
                print("")
                print(f"*** ERROR ***: Department {did} does not exist")
        except pymysql.err.OperationalError:
            print("")
            print(f"*** ERROR ***: Invalid DOB: {dob}")
        else:
            print("")
            print("Employee successfully added")

# Fucntion used in option 5 to find the budget of the department managed by the employee that the user inputted
def find_budget(budget):

    db = pymysql.connect(host = "localhost", user = "root", password = "root", db = "employees", cursorclass = pymysql.cursors.DictCursor)

    sql = "select did, format(budget, 0) as budget from dept where did = %s"

    with db:
        cursor = db.cursor()
        cursor.execute(sql, budget)
        return cursor.fetchall()

# Fucntion used in option 6 to find the employee ID on the SQL DB
def find_eid(eid):

    db = pymysql.connect(host = "localhost", user = "root", password = "root", db = "employees", cursorclass = pymysql.cursors.DictCursor)

    sql = "select eid from employee where eid = %s"

    with db:
        cursor = db.cursor()
        cursor.execute(sql, eid)
        return cursor.fetchall()

# Fucntion used in option 6 to find the department ID on the SQL DB
def find_did(did):
    
    db = pymysql.connect(host = "localhost", user = "root", password = "root", db = "employees", cursorclass = pymysql.cursors.DictCursor)

    sql = "select did from dept where did = %s"

    with db:
        cursor = db.cursor()
        cursor.execute(sql, did)
        return cursor.fetchall()

# Fucntion used in option 7 to select all details from the department table on SQL
def all_depts_details():
    
    db = pymysql.connect(host = "localhost", user = "root", password = "root", db = "employees", cursorclass = pymysql.cursors.DictCursor)

    sql = "select * from dept"

    with db:
        cursor = db.cursor()
        cursor.execute(sql)
        return cursor.fetchall()