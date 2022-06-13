import employeesDB
import calendar
from neo4j import GraphDatabase
from neo4j import exceptions

driver = None # Using the global variable driver in the connect() function

# Connect to the Neo4J database
def connect():
    global driver
    uri = "neo4j://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "M1nouburp!"), max_connection_lifetime=1000)

# Fucntion that is used in option 6 to ensure that the employee IDs are unique
def eid_unique(tx):
    query = "CREATE CONSTRAINT eid_unique IF NOT EXISTS ON (e:Employee) ASSERT e.eid IS UNIQUE"
    tx.run(query)

# Fucntion that is used in option 6 to ensure that the department IDs are unique
def did_unique(tx):
    query = "CREATE CONSTRAINT did_unique IF NOT EXISTS ON (d:Department) ASSERT d.did IS UNIQUE"
    tx.run(query)

# Fucntion used in option 6 to check if the department exists or not in the Neo4j DB
def find_department(tx, did):
    query = "MATCH(d:Department{did:$dept1}) RETURN d.did"
    results = tx.run(query, dept1=did) 
    department = []
    for result in results:
        department.append(result["d.did"])
    return department 

# Fucntion used in option 6 to add the department to the Neo4j DB if it does not already exist
def add_department(tx, did):
    query = "CREATE(d:Department{did:$dept1})"
    tx.run(query, dept1=did)

# Fucntion used in option 6 to check if the employee exists or not in the Neo4j DB
def find_employee(tx, eid):
    query = "MATCH(e:Employee{eid:$person1}) RETURN e.eid"
    results = tx.run(query, person1=eid) 
    employee = []
    for result in results:
        employee.append(result["e.eid"])
    return employee

# Fucntion used in option 6 to add the employee to the Neo4j DB if it does not already exist
def add_employee(tx, eid):
    query = "CREATE(e:Employee{eid:$person1})"
    tx.run(query, person1=eid)

# Fucntion used in option 5 to find the departments managed by the employee ID that the user has entered
def find_departments(tx, p1):
    query = "MATCH(e:Employee{eid:$person1})-[:MANAGES]->(d:Department)RETURN d.did"
    results = tx.run(query, person1=p1)    
    departments = []
    for result in results:
        departments.append(result["d.did"])
    return departments

# Fucntion used in option 6 to add the employee ID that the user entered as the new manager of the department that the user has entered. If there already exists a relationship "managed by" on this department, the query will not run
def add_manager(tx, eid, did):
    query = "MATCH(e{eid:$person1}) MATCH(d{did:$dept1}) WHERE NOT EXISTS(()-[:MANAGES]->(d)) CREATE(e)-[:MANAGES]->(d) RETURN e.eid, d.did"
    tx.run(query, person1=eid, dept1=did)

# This function tests if the above function ran or not. If the above function yielded 0 results, then the department was already managed by another employee
def test_add_manager(tx, eid, did):
    query = "MATCH(e{eid:$person1})-[:MANAGES]->(d{did:$dept1}) RETURN e.eid, d.did"
    results = tx.run(query, person1=eid, dept1=did)
    managers =[]
    for result in results:
        managers.append({"eid":result["e.eid"], "did":result["d.did"]})
    return managers

# Fucntion used in option 6 to let the user know which manager already manages a certain department
def find_managerid(tx, did):
    query = "MATCH (e:Employee)-[:MANAGES]->(d:Department{did:$dept1}) RETURN e.eid"
    results = tx.run(query, dept1=did)
    employee_name = []
    for result in results:
        employee_name.append(result["e.eid"])
    return employee_name

# Fucntion used in option 8 to find all of the managers and departments that they manage 
def find_departments_managed(tx):
    query = "MATCH(e:Employee)-[:MANAGES]->(d:Department) RETURN e.eid, COLLECT(d.did) ORDER BY e.eid"
    results = tx.run(query) 
    departments = []
    for result in results:
        departments.append({"manager":result["e.eid"], "departments":result["COLLECT(d.did)"]})
    return departments


# Main function
def main():
    connect() # Connect to the neo4J DB

    # Empyt list used in option 7. If the user chooses option 7 again, the information is not read from the database again. Instead, the information read the first-time option 7 was chosen is used.
    arr = []
    display_menu()

    while True:
        choice = input("Enter Choice: ")

        if choice == "1":
            empdept = employeesDB.emp_dept()
            
            for i in range(0, len(empdept), 2):
                for row in empdept[i:i+2]:
                    print(row["name"], "|", row["d.name"])
                quit = input("-- Quit (q) --")
                if quit != "q":
                    continue
                else:
                    break

            while True:
                if quit != "q":
                    quit = input("-- Quit (q) --")
                else:
                    break

            display_menu()

        elif choice == "2":
            eid = input("Enter EID: ")
            salaries = employeesDB.find_salaries(eid)
            print("")
            print(f"Salary details for Employee: {eid}")
            print("----------------------------")
            print("Mimimum | Average | Maximum")
            if salaries == [{'Minimum': None, 'Average': None, 'Maximum': None}]:
                print("")
            else:
                for i in salaries:
                    print(i["Minimum"], "|", i["Average"], "|", 
                    i["Maximum"])

            display_menu()
        elif choice == "3":
            while True:

                month = (input("Enter Month: "))
                try:
                    month_int = int(month)
                    if (month_int > 12 or month_int < 1):
                        continue


                    else:
                        dob = employeesDB.find_dob(month_int)
                        print("")
                        for i in dob:
                            print(i["eid"], "|", i["name"], "|", i["dob"])
                        break
                except:
                    month_str = str(month)
                    month_str = month_str.capitalize()
                    if len(month_str) > 3:
                        continue

                    else:
                        try:
                            abbr_to_num = {name: num for num, name in enumerate(calendar.month_abbr) if num}

                            numeric_month = abbr_to_num[month_str]
                            dob = employeesDB.find_dob(numeric_month)
                            print("")
                            for i in dob:
                                print(i["eid"], "|", i["name"], "|", i["dob"])
                            break
                        except:
                            continue

            display_menu()
        elif choice == "4":
            print("")
            print("Add New Employee")
            print("----------------")
            eid = input("EID: ")
            name = input("Name: ")
            dob = input("Dob: ")
            did = input("Dept ID: ")
            employeesDB.add_employee(eid, name, dob, did)
            display_menu()
            
        elif choice == "5":
            connect()
            print("")
            p1 = input("Enter EID: ")
            print("")
            print(f"Departments managed by: {p1}")
            print("-----------------------------")
            print("Department | Budget")
            with driver.session() as session:
                values = session.read_transaction(find_departments, p1)

                for i in values:
                    budget = employeesDB.find_budget(i)
                    for j in budget:
                        print(j["did"], "|", j["budget"])

            display_menu()
        elif choice == "6":
            connect()
            print("")
            with driver.session() as session:
                session.write_transaction(eid_unique)
                session.write_transaction(did_unique)
            while True:
                eid = input("Enter EID: ")
                did = input("Enter DID: ")
                employee = employeesDB.find_eid(eid)
                department = employeesDB.find_did(did)
            
                if (employee == () and department == ()):
                    print(f"Employee {eid} does not exist")
                    print(f"Department {did} does not exist")
                    print("")
                    continue
                elif employee == ():
                    print(f"Employee {eid} does not exist")
                    print("")
                    continue
                elif department == ():
                    print(f"Department {did} does not exist")
                    print("")
                    continue                
                else:

                    with driver.session() as session:
                        test_dept = session.read_transaction(find_department, did)
                        test_emp = session.read_transaction(find_employee, eid)

                        if (test_dept == [] and test_emp == []):
                            session.write_transaction(add_department, did)
                            session.write_transaction(add_employee, eid)

                            node_id = session.write_transaction(add_manager, eid, did)
                            values = session.read_transaction(test_add_manager, eid, did)

                            print("")
                            print(f"Employee {eid} now manages Department {did}")
                            break

                        elif test_dept == []:
                            session.write_transaction(add_department, did)

                            node_id = session.write_transaction(add_manager, eid, did)
                            values = session.read_transaction(test_add_manager, eid, did)

                            print("")
                            print(f"Employee {eid} now manages Department {did}")
                            break

                        elif test_emp == []:

                            values = session.read_transaction(test_add_manager, eid, did)

                            if values == []:
                                value_emp = session.read_transaction(find_managerid, did)
                                if value_emp == []:
                                    session.write_transaction(add_employee, eid)
                                    node_id = session.write_transaction(add_manager, eid, did)

                                    print("")                                
                                    print(f"Employee {eid} now manages Department {did}")
                                    break
                                else:                                    
                                    
                                    print("")
                                    print(f"Department {did} is already managed by Employee {value_emp[0]}")
                                    continue
                            else:
                                session.write_transaction(add_employee, eid)

                                node_id = session.write_transaction(add_manager, eid, did) 

                                print("")                               
                                print(f"Employee {eid} now manages Department {did}")
                                break


                        else:

                            node_id = session.write_transaction(add_manager, eid, did)
                            values = session.read_transaction(test_add_manager, eid, did)

                            if values == []:
                                value_emp = session.write_transaction(find_managerid, did)

                                print("")
                                print(f"Department {did} is already managed by Employee {value_emp[0]}")
                                continue
                            else:
                                print("")
                                print(f"Employee {eid} now manages Department {did}")
                                break                            

            display_menu()
        elif choice == "7":

            if arr == []:

                depts = employeesDB.all_depts_details()
                print("")
                print("DID | Name | Location | Budget")
                    
                for i in depts:
                    print(i["did"], "|", i["name"], "|", i["lid"], "|", i["budget"])
                arr += depts
  
            else:
                for i in arr:
                    print(i["did"], "|", i["name"], "|", i["lid"], "|", i["budget"])
            
            display_menu()

        elif choice == "8":
            print("")
            print("List of managers and departments")
            print("--------------------------------")
            print("Manager | Department(s)")
            with driver.session() as session:
                values = test_dept = session.read_transaction(find_departments_managed)

                for i in values:
                    print(i["manager"], "|", i["departments"])


        elif choice == "x":
            break
        else:
            display_menu()


def display_menu():
    print("")
    print("Employees")
    print("---------")
    print("")
    print("MENU")
    print("=" * 4)
    print("1 - View Employees & Departments")
    print("2 - View Salary Details")
    print("3 - View by Month of Birth")
    print("4 - Add New Employee")
    print("5 - View Departments Managed by Employee")
    print("6 - Add Manager to Department")
    print("7 - View Departments")
    print("8 - View Managers & Departments")
    print("x - Exit Application")


if __name__ == "__main__":
    main()