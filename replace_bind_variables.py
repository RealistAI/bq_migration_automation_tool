import re
import csv

def replace_bind_variables(table_references: list[str]):
    """
    Replace all the bind variables in the SQL extracted from the UC4 SQL repo with the correct values.
    The SQL we are extracting for the UC4 jobs contains bind variables ${some_bind_variable}.
    We need to be able to replace those with the required values.
    """

    #create list that will hold the corrected SQL values with no bind variables.
    improved_table_references = []
    #where the corrected bind variables will be stored
    corrected_values = []

    #read csv that contains the bind variables and their correct td values.
    with open("bind_variables.csv", "r") as bind_file:
        bind_variable_mapping = csv.reader(bind_file, delimiter=",")
        for bind in bind_variable_mapping:
            print(bind)
            print("\n")

            #Iterate through the list of table references and pull out any bind variables with regex.
            for reference in table_references:
                matched_bind_variables = re.findall(r"\$.*?}", reference)
                if matched_bind_variables == []:
                    continue

                for bind_variable in matched_bind_variables:
                    print("iterable is", bind_variable)
                    print("\n")

                    print("bind mapping value is ", bind[0])
                    print("\n")
                    if bind_variable == bind[0]:
                        corrected_values.append(bind[1])

            print("new list is ", corrected_values)
            improved_reference = reference.replace("${my-dataset}", str(corrected_values[0]))
            improved_reference = improved_reference.replace("${my-table}", str(corrected_values[1]))
            improved_table_references.append(improved_reference)
        print("improved table reference list is ", improved_table_references)

test_list = ["CREATE TABLE ${my-dataset}.${my-table}", "CREATE TABLE IF NOT EXISTS ${my-table}"]
replace_bind_variables(table_references=test_list)
