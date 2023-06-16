import re

def replace_bind_variables(table_references: list[str]):
    """
    Replace all the bind variables in the SQL extracted from the UC4 SQL repo with the correct values. The SQL we are extracting for the UC4 jobs contains bind variables ${some_bind_variable} .
    We need to be able to replace those with the required values.
    """

    #the number used to iterate through the whole list in order
    number = 0

    #create list that will hold the corrected SQL values with no bind variables.
    improved_table_references = []
    #read csv that contains the bind variables and their correct td values.
    with open("bind_variables.csv", "r") as bind_file:
        bind_variables = bind_file.read()

    #Iterate through the list of table references
    #use each item in table_references and iterate through bind_variables to see if there is a connection.
    #if there is a connection turn the item into the bind_variable so that it has the correct values.

    for reference in table_references:
        print("reference is ", reference)
        reference_split = reference.split(" ")
        reference_string = str(reference_split)
        print(reference_string)
        bind_variable = re.findall(r"\$.*?}", reference_string)
        print(bind_variable)
        if bind_variable == bind_variables:
            bind_variables = bind_variable
            improved_table_references.append(bind_variables)
    print("new list is ", improved_table_references)


test_list = ["CREATE TABLE ${my-dataset}.${my-table}", "CREATE VIEW IF NOT EXISTS my-view"]

replace_bind_variables(table_references=test_list)
