import re

def replace_bind_variables(table_references: list[str]):
    """
    Replace all the bind variables in the SQL extracted from the UC4 SQL repo with the correct values.
    The SQL we are extracting for the UC4 jobs contains bind variables ${some_bind_variable}.
    We need to be able to replace those with the required values.
    """

    #create list that will hold the corrected SQL values with no bind variables.
    improved_table_references = []
    clean_list_bind_variables = []

    #read csv that contains the bind variables and their correct td values.
    with open("bind_variables.csv", "r") as bind_file:
        bind_variables = bind_file.read()

    #Iterate through the list of table references and pull out any bind variables with regex.
    for reference in table_references:
        print("reference is ", reference)
        bind_variable_reference = re.findall(r"\$.*?}", reference)
        if bind_variable_reference == []:
            continue
        print("bind reference is ", bind_variable_reference)
        #Isolate the csv variables from each other and iterate through the reference variables one at a time.
        for i in range(len(bind_variable_reference)):
            if type(bind_variables) == type(str()):
                bind_variables = bind_variables.split("\n")
            else:
                pass
            for i in bind_variables:
                if i == "":
                    continue
                else:
                    clean_list_bind_variables.append(i)
            print(clean_list_bind_variables)

            print("file contents are ", bind_variables)
            print("individual reference ", bind_variable_reference[i])
            bind_variables_start = bind_variables[i]
            print("individual variable from file is ", bind_variables_start)
            #once isolated, take the bind_variables and compare them to the csv values, 
            #so if there is a match the bind variables will be replaced with their teradata equivalents.
            if bind_variables_start == " ":
                continue
            else:
                print("first variable in file ", bind_variables_start)
                if bind_variables_start == bind_variable_reference[i]:
                    bind_variable = bind_variables[i+i]
                    improved_table_references.append(bind_variable)
    print("new list is ", improved_table_references)


test_list = ["CREATE TABLE ${my-dataset}.${my-table}", "CREATE TABLE IF NOT EXISTS ${my-table}"]
replace_bind_variables(table_references=test_list)
