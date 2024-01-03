import sqlparse

from sqlparse.sql import IdentifierList, Identifier, Parenthesis

from sqlparse.tokens import Keyword, DML, Whitespace, Literal, Punctuation, Name, Operator

import time

from sorting_techniques import pysort

from BTrees.OOBTree import OOBTree


class RDBMS:

    def __init__(self):

        self.tables = {}

        self.relationships = {}

        # Additional structures for indexing, query optimization, etc.

    def parse_sql(self, sql_query):

        parsed_statement = sqlparse.parse(sql_query)

        self.execute(parsed_statement)

    def execute(self, parsed_statements):

        # Parse the SQL query

        # Handle each parsed statement

        start_time = time.perf_counter()


        for statement in parsed_statements:

            # Tokenize the statement to extract the command type

            tokens = statement.tokens

            first_token = tokens[0]

            if first_token.ttype is Keyword.DML:

                # Handle DML commands (INSERT, SELECT)

                if statement.get_type() == 'INSERT':

                    self.extract_insert_data(statement)



                elif statement.get_type() == 'SELECT':

                    # print("Calling Select properly!")

                    self.extract_select_data(statement)



                elif statement.get_type() == 'DELETE':

                    self.extract_delete_data(statement)



                elif statement.get_type() == 'UPDATE':

                    self.extract_update_data(statement)











            elif first_token.ttype is Keyword.DDL:

                # Handle DDL commands (CREATE TABLE, DROP TABLE)

                if statement.get_type() == 'CREATE':

                    self.create_manager(statement)







                elif statement.get_type() == 'DROP':

                    self.drop_manager(statement)



            else:

                print("Invalid SQL Argument")

                return

        end_time = time.perf_counter()

        execution_time = end_time - start_time

        print(f"The query: took {execution_time} seconds to execute.")

    # Database Definition Language (DDL) Methods

    def create_manager(self, statement):

        # Determine whether it's a table or index creation

        create_table = False

        create_index = False

        for token in statement.tokens:

            if token.ttype is Keyword.DDL and token.value.upper() == 'CREATE':
                continue

            if token.ttype is Keyword and token.value.upper() == 'TABLE':
                create_table = True

                break

            if token.ttype is Keyword and token.value.upper() == 'INDEX':
                create_index = True

                break

        if create_table:

            self.extract_create_table_data(statement)







        elif create_index:

            self.extract_create_index_data(statement)  # Pass the original statement

    def drop_manager(self, statement):

        # Determine whether it's a table or index drop

        drop_table = False

        drop_index = False

        for token in statement.tokens:

            if token.ttype is Keyword.DDL and token.value.upper() == 'DROP':
                continue

            if token.ttype is Keyword and token.value.upper() == 'TABLE':
                drop_table = True

                break

            if token.ttype is Keyword and token.value.upper() == 'INDEX':
                drop_index = True

                break

        if drop_table:

            self.extract_drop_table_data(statement)







        elif drop_index:

            self.extract_drop_index_data(statement)  # Pass the original statement

    def extract_create_table_data(self, statement):

        table_name = None

        schema = {}

        create_found = False

        table_found = False

        schema_next = False

        column_names = []

        column_types = []

        type_next = False

        column_next = False

        primary_found = False

        primary_key_next = False

        primary_key_found = False

        primary_key = None

        foreign_key_found = False

        foreign_table_next = False

        foreign_table_name = None

        key_found = False

        current_table_reference = None

        foreign_column_found = False

        foreign_reference_found = False

        foreign_key = None

        foreign_table_found = False

        for token in statement.flatten():

            if token.ttype is Keyword.DDL and token.value.upper() == 'CREATE':
                create_found = True

                continue

            if create_found and not schema_next and token.ttype is Keyword and token.value.upper() == 'TABLE':
                table_found = True

                continue

            if not primary_found and token.ttype is not Punctuation and not token.is_whitespace and token.value.upper() == "PRIMARY":
                primary_found = True

                continue

            if foreign_key_found and not key_found and token.value.upper() == "KEY":
                key_found = True

                foreign_table_next = True

                continue

            if primary_found and token.value.upper() == "KEY":
                primary_key_next = True

                continue

            if not foreign_key_found and token.value.upper() == "FOREIGN":
                foreign_key_found = True

                continue

            if foreign_key_found and foreign_table_next and token.ttype is not Punctuation and not token.is_whitespace:
                current_table_reference = token.value

                foreign_column_found = True

                foreign_table_next = False

                continue

            if foreign_column_found and token.value.upper() == "REFERENCES":
                foreign_reference_found = True

                continue

            if foreign_column_found and not foreign_table_found and foreign_reference_found and token.ttype is not Punctuation and not token.is_whitespace:
                foreign_table_name = token.value

                foreign_table_found = True

                continue

            if foreign_table_found and token.ttype is not Punctuation and not token.is_whitespace:
                reference_column = token.value

                continue

            if not primary_key_found and primary_key_next and token.ttype is not Punctuation and not token.is_whitespace:
                primary_key_next = False

                primary_key = token.value

                primary_key_found = True

            if table_found and not schema_next and not token.is_whitespace and token.ttype is not Punctuation:

                if table_name is None:
                    table_name = token.value

                    schema_next = True

                    continue

            if schema_next and token.ttype is not Whitespace and token.ttype is not Punctuation:

                # Parsing column definitions for the schema

                if not type_next and token.ttype is not Punctuation and not token.is_whitespace:

                    column_name = token.value

                    column_names.append(column_name)

                    type_next = True

                    continue



                elif token.ttype is Name.Builtin:

                    column_type = token.value

                    column_types.append(column_type)

                    type_next = False

                    continue

        schema = {name: type for name, type in zip(column_names, column_types)}

        # print(schema)

        # Once table name and schema are extracted, create the table

        if table_name and schema:

            self.create_table(table_name, schema)



        else:

            print("Error creating table")

        if primary_key_found:
            self.create_index(primary_key, table_name, primary_key)

        if foreign_key_found:
            self.create_foreign_key(table_name, current_table_reference, foreign_table_name, reference_column)

    def create_foreign_key(self, table_name, curr_table_column, foreign_table, foreign_column):

        # Check if both tables exist

        if table_name not in self.tables:
            raise Exception(f"Table {table_name} does not exist.")

        if foreign_table not in self.tables:
            raise Exception(f"Referenced table {foreign_table} does not exist.")

        # Check if the foreign key column exists in the current table

        if curr_table_column not in self.tables[table_name]["columns"]:
            raise Exception(f"Column {curr_table_column} does not exist in table {table_name}.")

        # Check if the referenced column exists in the foreign table

        if foreign_column not in self.tables[foreign_table]["columns"]:
            raise Exception(f"Referenced column {foreign_column} does not exist in table {foreign_table}.")

        # Add foreign key information to the current table's schema

        self.tables[table_name]["foreign_keys"][curr_table_column] = (foreign_table, foreign_column)

        print(f"Foreign key added to {table_name}: {curr_table_column} references {foreign_table}({foreign_column})")

    def create_table(self, table_name, schema, primary_key=None, foreign_keys=None):

        if table_name in self.tables:

            raise Exception(f"Table {table_name} already exists.")

        else:

            self.tables[table_name] = {

                "columns": schema,

                "data": [],

                "primary_key": primary_key if primary_key else [],

                "foreign_keys": foreign_keys if foreign_keys else {}  # Initialize as empty dict if not provided

            }

        print(f"Table {table_name} created with schema {schema}")

    def extract_drop_table_data(self, statement):

        table_name = None

        drop_found = False

        table_found = False

        for token in statement.flatten():

            if token.ttype is Keyword.DDL and token.value.upper() == 'DROP':
                drop_found = True

                continue

            if drop_found and token.ttype is Keyword and token.value.upper() == 'TABLE':
                table_found = True

                continue

            if table_found and not token.is_whitespace and token.ttype is not Punctuation:

                if table_name is None:
                    table_name = token.value

                    # print("WE HAVE FOUND THE TABLE NAME AND IT IS  :  ", table_name)

                    continue

        if table_name:

            self.drop_table(table_name)



        else:

            print("Error dropping table")

    def drop_table(self, table_name):

        # Check if the table exists

        if table_name in self.tables:

            # Remove the table from the dictionary

            del self.tables[table_name]

            print(f"Table {table_name} has been dropped.")

        else:

            # If the table does not exist, print an error message

            print(f"Table {table_name} does not exist and cannot be dropped.")

    # Indexing Methods

    def extract_create_index_data(self, statement):

        index_name = None

        index_found = False

        index_name_found = False

        column_next = False

        table_name = None

        columns = []

        create_index_found = False

        on_found = False

        for token in statement.flatten():

            if create_index_found and token.ttype is Name and index_name is None:
                index_name = token.value

                continue

            if token.ttype is Keyword.DML and token.value.upper() == 'CREATE':
                create_index_found = True

                continue

            if token.ttype is Keyword and token.value.upper() == 'INDEX':
                index_found = True

                continue

            if index_found and not index_name_found and token.ttype is Name:
                index_name = token.value

                index_name_found = True

            if token.ttype is Keyword and token.value.upper() == 'ON':
                on_found = True

                continue

            if on_found and token.ttype is Name and table_name is None:
                table_name = token.value

                column_next = True

                continue

            if column_next and token.ttype is not Punctuation and not token.is_whitespace:
                column = token.value

                columns.append(column)

        self.create_index(index_name, table_name, columns)

    def create_index(self, index_name, table_name, columns):

        if table_name not in self.tables:
            print(f"Table {table_name} does not exist.")

            return

        if 'indexes' not in self.tables[table_name]:
            self.tables[table_name]['indexes'] = {}

        if index_name in self.tables[table_name]['indexes']:
            print(f"Index {index_name} already exists on table {table_name}.")

            return

        # Create a new B-tree index

        index = OOBTree()

        # Populate the index

        for row_id, row in enumerate(self.tables[table_name]["data"]):

            index_key = tuple(row[col] for col in columns)

            if index_key not in index:
                index[index_key] = []

            index[index_key].append(row_id)

        # Store the index along with the column names

        self.tables[table_name]['indexes'][index_name] = {'index': index, 'columns': columns}

        print(f"Index {index_name} created on table {table_name} for columns {columns}")

    def extract_drop_index_data(self, statement):

        index_name = None

        table_name = None  # If your DROP INDEX syntax requires specifying the table name

        table_found = False

        on_found = False

        drop_index_found = False

        index_found = False

        for token in statement.flatten():

            if token.ttype is Keyword.DDL and token.value.upper() == 'DROP':
                drop_index_found = True

                continue

            if drop_index_found and token.ttype is Keyword and token.value.upper() == 'INDEX':
                continue

            if drop_index_found and not index_found and not token.is_whitespace and token.ttype is not Punctuation:
                index_name = token.value

                # print("INDEX FOUND  :  ", index_name)

                index_found = True

                continue

            if index_found and token.ttype is Keyword and token.value.upper() == "ON":
                on_found = True

                continue

            if on_found and not token.is_whitespace and token.ttype is not Punctuation:
                table_name = token.value

                # print("Found Table Name  :  ", table_name)

                table_found = True

        if index_found and table_found:

            self.drop_index(index_name, table_name)  # Pass the table name if needed







        else:

            print("Error: table or index not found")

    def drop_index(self, index_name, table_name):

        if table_name and table_name not in self.tables:
            print(f"Table {table_name} does not exist.")

            return

        # Assuming each table has a 'indexes' dictionary where keys are index names

        if index_name in self.tables[table_name]['indexes']:

            del self.tables[table_name]['indexes'][index_name]

            print(f"Index {index_name} has been dropped from table {table_name}.")







        else:

            print(f"Index {index_name} does not exist in table {table_name}.")

    # Data Manipulation Language (DML) Methods

    def extract_insert_data(self, statement):

        table_name = None

        values = []

        insert_found = False

        into_found = False

        values_next = False

        table_name_found = False

        is_table_name_next = False  # Define this flag

        for token in statement.flatten():

            if token.ttype is Keyword.DML and token.value.upper() == 'INSERT':
                insert_found = True

                continue

            if insert_found and token.ttype is Keyword and token.value.upper() == 'INTO':
                into_found = True

                is_table_name_next = True

                continue

            if into_found and is_table_name_next and table_name is None and not token.is_whitespace:
                table_name = token.value

                table_name_found = True

                is_table_name_next = False

                continue

            if table_name_found and not is_table_name_next and token.ttype is Keyword and token.value.upper() == 'VALUES':
                values_next = True

                continue

            if values_next and not token.is_whitespace and token.ttype is not Punctuation:
                value = token.value.strip("'")

                values.append(value)

        self.insert(table_name, values)

    def insert(self, table_name, values):
        # Check if the table exists
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} does not exist.")

        table = self.tables[table_name]
        column_names = list(table["columns"].keys())

        # Check if the number of values matches the number of columns
        if len(values) != len(column_names):
            raise Exception("Value count does not match column count.")

        # Convert values to their correct types based on the schema
        converted_values = []
        for col, val in zip(column_names, values):
            try:
                if table["columns"][col].lower() == "integer":
                    converted_values.append(int(val))
                elif table["columns"][col].lower() == "float":
                    converted_values.append(float(val))
                elif table["columns"][col].lower() == "string":
                    converted_values.append(str(val))
                else:
                    raise Exception(f"Unsupported data type for column {col}")
            except ValueError as e:
                raise Exception(f"Invalid value for column {col}: {val}")

        new_record = dict(zip(column_names, converted_values))

        # Check for foreign key constraints
        if "foreign_keys" in table:
            for fk_column, (ref_table, ref_column) in table["foreign_keys"].items():
                fk_value = new_record[fk_column]
                if not any(row[ref_column] == fk_value for row in self.tables[ref_table]["data"]):
                    print(f"Foreign key constraint violation: {fk_value} does not exist in {ref_table}({ref_column})")
                    return

        # Check if the record violates any unique index constraints
        if 'indexes' in table:
            for index_name, index_info in table['indexes'].items():
                index_columns = index_info['columns']

                # Ensure index_columns is a list
                if not isinstance(index_columns, list):
                    index_columns = [index_columns]

                # Create index key for the new record
                index_key_parts = [new_record.get(col) for col in index_columns]
                index_key = tuple(index_key_parts)

                # Check if the index key already exists in the index
                if index_key in index_info['index']:
                    print(
                        f"Unique constraint violation: Record with key {index_key} already exists in index {index_name}.")
                    return

        # If no unique constraints are violated, insert the record
        table["data"].append(new_record)
        new_row_id = len(table["data"]) - 1

        # Update indexes
        if 'indexes' in table:
            for index_name, index_info in table['indexes'].items():
                index_columns = index_info['columns']

                # Ensure index_columns is a list
                if not isinstance(index_columns, list):
                    index_columns = [index_columns]

                # Retrieve the index object
                index = index_info['index']

                # Create index key for the new record
                index_key_parts = [new_record.get(col) for col in index_columns]
                index_key = tuple(index_key_parts)

                # Add new row ID to the index
                if index_key not in index:
                    index[index_key] = [new_row_id]

    def find_selectivity(self, table_name, column):
        # Check if the table exists
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} does not exist.")

        table = self.tables[table_name]

        # Check if the column exists in the table
        if column not in table["columns"]:
            raise Exception(f"Column {column} does not exist in table {table_name}.")

        # Calculate the number of distinct values in the column
        distinct_values = set()
        for record in table["data"]:
            if column in record:
                distinct_values.add(record[column])

        # Calculate the total number of tuples in the table
        num_tuples = len(table["data"])

        # Calculate selectivity
        selectivity = (num_tuples / len(distinct_values)) / num_tuples if num_tuples > 0 and distinct_values else 0

        return selectivity

    def extract_delete_data(self, statement):

        delete_found = False

        from_found = False

        table_found = False

        table_name = None

        where_found = False

        operator_found = False

        condition_found = False

        column_found = False

        columns = []

        values = []

        operators = []

        logical_operator_found = False

        operator = None

        logical_operator = None

        for token in statement.flatten():

            # print("TOKEN VALUE  :  ", token.value, "  :  ", token.ttype)

            if token.ttype is Keyword.DML and token.value.upper() == 'DELETE':
                delete_found = True

                continue

            if delete_found and token.ttype is Keyword and token.value.upper() == 'FROM':
                from_found = True

                continue

            if delete_found and from_found and not table_found and token.ttype is Name:
                table_name = token.value

                table_found = True

                # print("Table Found  :  ", table_name)

                continue

            if table_found and not where_found and token.ttype is Keyword and token.value.upper() == 'WHERE':
                where_found = True

                # print("where...")

                continue

            if where_found and token.ttype is Keyword and (token.value.upper() == 'OR' or token.value.upper() == 'AND'):
                # print("logical operator found: ", token.value.upper())

                logical_operator = token.value.upper()

                logical_operator_found = True

                continue

            if where_found and not operator_found and token.ttype is not Punctuation and token.ttype is not Operator.Comparison and not token.is_whitespace:
                column = token.value.strip("'")

                columns.append(column)

                column_found = True

                # print(" COLUMN FOUND  :  ", column)

                continue

            if column_found and token.ttype is Operator.Comparison and not operator_found:
                operator_found = True

                # print("OPERATOR  :  ", token.value)

                operator = token.value

                operators.append(operator)

                continue

            if not condition_found and where_found and operator_found and token.ttype is not Punctuation and token.ttype is not Operator.Comparison and not token.is_whitespace:
                value = token.value.strip("'")

                values.append(value)

                # print("CONDITION  :  ", value)

                condition_found = False

                operator_found = False

                column_found = False

                continue

        if logical_operator_found:

            self.delete_multiple(table_name, columns, values, logical_operator, operators)

        else:

            self.delete(table_name, columns[0], values[0], operator)

    def delete(self, table_name, column, value, operator):
        if table_name not in self.tables:
            print(f"Table {table_name} does not exist.")
            return

        table = self.tables[table_name]
        table_schema = table["columns"]

        if column not in table_schema:
            print(f"Column {column} does not exist in table {table_name}.")
            return

        # Convert value based on column type
        column_type = table_schema[column].lower()
        if column_type == 'integer':
            try:
                value = int(value)
            except ValueError:
                print(f"Value {value} is not a valid integer.")
                return
        elif column_type == 'float':
            try:
                value = float(value)
            except ValueError:
                print(f"Value {value} is not a valid float.")
                return
        elif column_type == 'string':
            value = str(value).strip("'\"")  # Strip quotes for string values

        # Check if there's a suitable index
        index_used = False
        suitable_index = self.find_suitable_index(table.get('indexes', {}), column)

        if suitable_index:
            index_used = True
            # Use index to delete rows
            rows_to_delete = set()
            for index_key, row_ids in suitable_index.items():
                for row_id in row_ids:
                    if self.evaluate_delete_condition(table["data"][row_id], column, value, operator):
                        rows_to_delete.add(row_id)

            new_data = [row for i, row in enumerate(table["data"]) if i not in rows_to_delete]

        else:
            # Fallback to full table scan
            new_data = []
            for row in table["data"]:
                if not self.evaluate_delete_condition(row, column, value, operator):
                    new_data.append(row)

        # Update the table data
        deleted_rows = len(table["data"]) - len(new_data)
        table["data"] = new_data

        # Update any indexes (if needed)
        # ...

        print(f"Deleted {deleted_rows} rows from {table_name}.")
        if index_used:
            print("Deletion performed using index.")
        else:
            print("Deletion performed using full table scan.")
            # Evaluate condition

    def evaluate_delete_condition(self, row, column, value, operator):

        if column not in row:
            return False

        if isinstance(value, int):

            try:

                row[column] = int(row[column])

            except ValueError:

                print(f"Value {value} is not a valid integer.")

                return

        elif isinstance(value, float):

            try:

                row[column] = float(row[column])

            except ValueError:

                print(f"Value {value} is not a valid float.")

                return

        elif isinstance(value, str):

            row[column] = str(row[column])

        # Comparison operations

        if operator == '=':

            # print(f"operator is {operator}")

            # print("type of row = ", type(row[column]), "| type of value = ", type(value))

            return row[column] == value

        elif operator == '<':

            return row[column] < value

        elif operator == '>':

            return row[column] > value

        elif operator == '<=':

            return row[column] <= value

        elif operator == '>=':

            return row[column] >= value

        elif operator == '!=':

            return row[column] != value

        else:

            raise ValueError(f"Unsupported operator: {operator}")

    def delete_multiple(self, table_name, columns, values, logical_operator, operators):

        if table_name not in self.tables:
            print(f"Table {table_name} does not exist.")

            return

        table_schema = self.tables[table_name]["columns"]

        new_data = []

        # Calculate selectivity for each condition

        conditions_with_selectivity = []

        for column, value, operator in zip(columns, values, operators):
            selectivity = self.find_selectivity(table_name, column)

            print(f"Selectivity for column {column}: {selectivity}")

            conditions_with_selectivity.append(((column, operator, value), selectivity))

        # Sort conditions based on selectivity

        if logical_operator == 'AND':

            conditions_with_selectivity.sort(key=lambda x: x[1])  # Sort by selectivity, ascending

        elif logical_operator == 'OR':

            conditions_with_selectivity.sort(key=lambda x: x[1], reverse=True)  # Sort by selectivity, descending

        print("Order of evaluation based on selectivity:", [cond[0] for cond, _ in conditions_with_selectivity])

        # Convert values based on column types and evaluate conditions

        for row in self.tables[table_name]["data"]:

            condition_results = []

            for condition, _ in conditions_with_selectivity:

                column, operator, value = condition

                column_type = table_schema[column].lower()

                if column_type == 'integer':

                    converted_value = int(value)

                elif column_type == 'float':

                    converted_value = float(value)

                elif column_type == 'string':

                    converted_value = str(value).strip("'\"")  # Strip quotes for string values

                else:

                    raise Exception(f"Unsupported column type: {column_type}")

                result = self.evaluate_update_condition(row, column, operator, converted_value)

                condition_results.append(result)

                # Short-circuit evaluation

                if logical_operator == 'AND' and not result:

                    break  # No need to evaluate further for AND

                elif logical_operator == 'OR' and result:

                    break  # No need to evaluate further for OR

            if (logical_operator == 'AND' and all(condition_results)) or (

                    logical_operator == 'OR' and any(condition_results)) or (

                    not logical_operator and condition_results[0]):

                continue

            else:

                new_data.append(row)

        deleted_count = len(self.tables[table_name]["data"]) - len(new_data)

        self.tables[table_name]["data"] = new_data

        print(f"{deleted_count} rows deleted from {table_name} where the conditions were met.")

    def matches_condition(self, row, columns, values, operators, logical_operator, table_schema):

        condition_matches = []

        for column, value, operator in zip(columns, values, operators):

            if column not in row:
                continue

            # Convert value to the appropriate type based on the column's data type

            if table_schema[column] == 'Integer':

                converted_value = int(value)



            else:

                converted_value = value

            # Evaluate condition based on operator

            if operator == '=':

                match = row[column] == converted_value



            elif operator == '<':

                match = row[column] < converted_value



            elif operator == '>':

                match = row[column] > converted_value



            elif operator == '<=':

                match = row[column] <= converted_value



            elif operator == '>=':

                match = row[column] >= converted_value



            elif operator == '!=':

                match = row[column] != converted_value



            else:

                raise ValueError(f"Unsupported operator: {operator}")

            condition_matches.append(match)

        if logical_operator.upper() == 'OR':

            return any(condition_matches)



        elif logical_operator.upper() == 'AND':

            return all(condition_matches)

        return False

    def extract_update_data(self, statement):

        update_found = False

        set_found = False

        where_found = False

        table_name = None

        columns = []

        values = []

        conditions = []

        logical_operator = None

        condition_column = None

        condition_operator = None

        for token in statement.flatten():

            if token.ttype is Keyword.DML and token.value.upper() == 'UPDATE':
                update_found = True

                continue

            if update_found and token.ttype is Name and table_name is None:
                table_name = token.value

                continue

            if token.ttype is Keyword and token.value.upper() == 'SET':
                set_found = True

                continue

            if set_found and token.ttype is Name and token.ttype is not Punctuation and not token.is_whitespace and not where_found:
                column = token.value

                columns.append(column)

                continue

            if set_found and token.ttype is Operator.Comparison and not where_found:
                # Skip processing this as it's part of SET clause

                continue

            if set_found and token.ttype is not Punctuation and not token.is_whitespace and not where_found and token.value.upper() != 'WHERE':
                value = token.value.strip("'")

                values.append(value)

                continue

            if token.ttype is Keyword and token.value.upper() == 'WHERE':
                where_found = True

                set_found = False  # Disable set clause processing

                continue

            if where_found and token.ttype is Keyword and (token.value.upper() == 'OR' or token.value.upper() == 'AND'):
                logical_operator = token.value.upper()

                continue

            if where_found and token.ttype is Name:
                condition_column = token.value

                continue

            if where_found and token.ttype is Operator.Comparison:
                condition_operator = token.value

                continue

            if where_found and token.ttype is not Punctuation and not token.is_whitespace:
                condition_value = token.value.strip("'")

                conditions.append((condition_column, condition_operator, condition_value))

                condition_column = None

                condition_operator = None

        if not conditions:
            print("No conditions found for update.")

            return

        self.update(table_name, columns, values, conditions, logical_operator)

    def update(self, table_name, columns, values, conditions, logical_operator=None):

        if table_name not in self.tables:
            print(f"Table {table_name} does not exist.")

            return

        if len(columns) != len(values):
            print("Error: The number of columns and values do not match.")

            return

        set_values = dict(zip(columns, values))

        # Calculate selectivity for each condition

        conditions_with_selectivity = []

        for condition in conditions:

            column = condition[0]

            selectivity = self.find_selectivity(table_name, column)

            if len(conditions) > 1:
                print(f"Selectivity for column {column}: {selectivity}")

            conditions_with_selectivity.append((condition, selectivity))

        # Sort conditions based on selectivity

        if logical_operator == 'AND':

            conditions_with_selectivity.sort(key=lambda x: x[1])  # Sort by selectivity, ascending

        elif logical_operator == 'OR':

            conditions_with_selectivity.sort(key=lambda x: x[1], reverse=True)  # Sort by selectivity, descending

        if len(conditions_with_selectivity) > 1:
            print("Order of evaluation based on selectivity:", [cond[0] for cond, _ in conditions_with_selectivity])

        # Evaluate conditions in the sorted order

        for row in self.tables[table_name]["data"]:

            condition_results = []

            for condition, _ in conditions_with_selectivity:

                column, operator, condition_value = condition

                # Adjust the condition value based on the column type

                column_type = self.tables[table_name]["columns"].get(column).lower()

                if column_type == 'integer':

                    condition_value = int(condition_value)

                elif column_type == 'float':

                    condition_value = float(condition_value)

                elif column_type == 'string':

                    condition_value = str(condition_value).strip("'\"")

                result = self.evaluate_update_condition(row, column, operator, condition_value)

                condition_results.append(result)

                # Short-circuit evaluation for logical operators

                if logical_operator == 'AND' and not result:

                    break

                elif logical_operator == 'OR' and result:

                    break

            # Apply updates if conditions are met

            if (logical_operator == 'AND' and all(condition_results)) or \
                    (logical_operator == 'OR' and any(condition_results)) or \
                    (not logical_operator and condition_results[0]):

                for column, value in set_values.items():

                    # Convert value to appropriate data type

                    if column in row:

                        if self.tables[table_name]["columns"][column].lower() == 'integer':

                            row[column] = int(value)

                        elif self.tables[table_name]["columns"][column].lower() == 'float':

                            row[column] = float(value)

                        elif self.tables[table_name]["columns"][column].lower() == 'string':

                            row[column] = str(value)

    def evaluate_update_condition(self, row, column, operator, value):

        if column not in row:
            return False

        # Retrieve the actual value from the row

        row_value = row[column]

        # Comparison operations

        if operator == "=":

            return row_value == value

        elif operator == "<":
            return row_value < value

        elif operator == ">":

            return row_value > value

        elif operator == "<=":

            return row_value <= value

        elif operator == ">=":

            return row_value >= value

        elif operator == "!=":

            return row_value != value

        else:

            return False


    def extract_select_data(self, statement):

        select_found = False

        columns = []

        table_next = False

        table_found = False

        table_name = None

        operator = None

        distinct_condition = False

        condition_found = False

        column_for_condition = None

        operator_found = False

        logical_operator = None

        condition = None

        conditions = []

        join_found = False

        aggregate_function = False

        for token in statement.flatten():

            if token.ttype is Keyword.DML and token.value.upper() == 'SELECT':
                select_found = True

                continue

            if token.value.upper() == 'AVG' or token.value.upper() == 'COUNT' or token.value.upper() == 'MAX' or token.value.upper() == 'MIN' or token.value.upper() == 'SUM':
                # print('AGG OPERATION FOUND')

                aggregate_function = True

                break

            if select_found and token.ttype is Keyword and token.value.upper() == 'DISTINCT':
                # print("DISTINCT SELECT  :  ")

                distinct_condition = True

            if token.ttype is not Punctuation and not token.is_whitespace and token.ttype is not Keyword and not table_next:
                column = token.value

                # print(column)

                columns.append(column)

                continue

            if token.ttype is Keyword and token.value == "FROM":
                table_next = True

                continue

            if table_next and not table_found and token.ttype is not Punctuation and not token.is_whitespace:
                table_name = token.value

                # print("FOUND TABLE NAME  :  ", table_name)

                table_found = True

                continue

            if table_found and token.ttype is Keyword and token.value.upper() == "NATURAL JOIN":
                # print("I AM IN HERE AND CALLING RETURN  :  ")

                self.extract_join_table_data(statement)

                return

            if table_found and token.ttype is Keyword and token.value.upper() == "JOIN":
                # print("I AM IN HERE AND CALLING RETURN  :  ")

                self.extract_join_table_data(statement)

                return

            if table_found and token.ttype is Keyword and token.value.upper() == 'WHERE':
                condition_found = True

                continue

            if condition_found and token.ttype is Keyword and (

                    token.value.upper() == 'OR' or token.value.upper() == 'AND'):
                logical_operator = token.value.upper()

                continue

            if condition_found and not operator_found and token.ttype is not Punctuation and token.ttype is not Operator.Comparison and not token.is_whitespace:
                column_for_condition = token.value

                continue

            if condition_found and token.ttype is Operator.Comparison:
                operator = token.value

                operator_found = True

                continue

            if condition_found and operator_found and token.ttype is not Punctuation and not token.is_whitespace:
                condition = token.value

                # Add the condition tuple to the conditions list

                conditions.append((column_for_condition, operator, condition))

                operator_found = False  # Reset for next condition

        if not distinct_condition and not condition_found and not aggregate_function:

            self.simple_select(columns, table_name)







        elif aggregate_function:

            self.aggregate_operations(statement)





        elif condition_found:

            # print("condition found...")

            self.condition_select(columns, table_name, conditions, logical_operator)

            # self.condition_select(columns, table_name, column_for_condition, operator, condition)







        elif distinct_condition:

            self.distinct_select(columns, table_name)

    def aggregate_operations(self, statement):

        avg_oper = False

        count_oper = False

        max_oper = False

        min_oper = False

        sum_oper = False

        aggreate_operator_found = False

        column_found = False

        column = None

        from_found = True

        table_found = False

        table = None

        for token in statement.flatten():

            if token.value.upper() == 'AVG':
                aggreate_operator_found = True

                # print("Computing", token.value)

                avg_oper = True

                continue

                # print("Computing :  ", token.value)

            if token.value.upper() == 'COUNT':
                aggreate_operator_found = True

                # print("Computing :  ", token.value)

                count_oper = True

                continue

            if token.value.upper() == 'MAX':
                # print("Computing :  ", token.value)

                aggreate_operator_found = True

                max_oper = True

                continue

            if token.value.upper() == 'MIN':
                # print("Computing :  ", token.value)

                aggreate_operator_found = True

                min_oper = True

                continue

            if token.value.upper() == 'SUM':
                # print("Computing :  ", token.value)

                aggreate_operator_found = True

                sum_oper = True

                continue

            if aggreate_operator_found and token.ttype is not Punctuation and not token.is_whitespace and not column_found:
                column_found = True

                column = token.value.strip("'")

                # print("Column is : ", column)

                continue

            if column_found and token.ttype is Keyword and token.value.upper() == 'FROM':
                from_found = True

                continue

            if from_found and column_found and not table_found and token.ttype is not Punctuation and not token.is_whitespace:
                table = token.value

                # print("Table is : ", table)

                continue

        if avg_oper:

            self.avg_calc(column, table)







        elif count_oper:

            self.count(column, table)







        elif min_oper:

            self.min_calc(column, table)







        elif max_oper:

            self.max_calc(column, table)







        elif sum_oper:

            self.sum_calc(column, table)

    def avg_calc(self, column, table):

        if table not in self.tables:
            print(f"Table {table} does not exist.")

            return

        data = self.tables[table]["data"]

        total = 0

        count = 0  # Initialize count to 0

        for row in data:

            if column in row:

                value = row[column]

                # Check if the value is a digit (either int or float)

                if str(value).replace(".", "", 1).isdigit():

                    if "." in str(value):

                        total += float(value)  # Convert to float if it has a decimal point







                    else:

                        total += int(value)  # Convert to int if it's a whole number

                    count += 1  # Increment count for valid values

        avg = total / count if count > 0 else 0

        print(f"Average of {column} in {table}: {avg}")

    def count(self, column, table):

        if table not in self.tables:
            print(f"Table {table} does not exist.")

            return

        count = sum(1 for row in self.tables[table]["data"] if column in row)

        print(f"Count of {column} in {table}: {count}")

    def max_calc(self, column, table):

        if table not in self.tables:
            print(f"Table {table} does not exist.")

            return

        max_value = max((row[column] for row in self.tables[table]["data"] if column in row), default=None)

        print(f"Max of {column} in {table}: {max_value}")

    def min_calc(self, column, table):

        if table not in self.tables:
            print(f"Table {table} does not exist.")

            return

        min_value = min((row[column] for row in self.tables[table]["data"] if column in row), default=None)

        print(f"Min of {column} in {table}: {min_value}")

    def sum_calc(self, column, table):

        if table not in self.tables:
            print(f"Table {table} does not exist.")

            return

        total = 0  # Initialize the total to 0

        for row in self.tables[table]["data"]:

            if column in row:

                value = row[column]

                # Check if the value is a digit (either int or float)

                if str(value).replace(".", "", 1).isdigit():

                    if "." in str(value):

                        total += float(value)  # Convert to float if it has a decimal point

                    else:

                        total += int(value)  # Convert to int if it's a whole number

        print(f"Sum of {column} in {table}: {total}")

    def simple_select(self, columns, table_name):
        # Handle the '*' wildcard for selecting all columns
        if columns == ['*']:
            columns = list(self.tables[table_name]['columns'].keys())
        else:
            # Here, you can add additional checks to ensure that the specified columns exist in the table
            pass

        # Check if the table exists

        if table_name not in self.tables:
            print(f"Table {table_name} does not exist.")

            return

        # Retrieve the table data

        table_data = self.tables[table_name]["data"]

        # Debug: Print a sample row to check actual column names

        # print("Sample row:", table_data[0] if table_data else "No data")

        # Print the specified columns for each row

        for row in table_data:
            selected_data = {col: row.get(col, None) for col in columns}

            print(selected_data)

    def evaluate_condition(self, row, column, operator, value):

        if column not in row:
            return False

        # Retrieve the actual value from the row

        row_value = row[column]

        try:
            # First, try to convert it to an integer if it's a whole number
            if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                value = int(value)
            else:
                # If not an int, try to convert it to a float
                value = float(value)
        except ValueError:
            # If it raises a ValueError, it's neither an int nor a float, so keep it as a string
            value = value

        # Comparison operations

        if operator == "=":

            return row_value == value

        elif operator == "<":
            return row_value < value

        elif operator == ">":

            return row_value > value

        elif operator == "<=":

            return row_value <= value

        elif operator == ">=":

            return row_value >= value

        elif operator == "!=":

            return row_value != value

        else:

            return False

    def condition_select(self, columns, table_name, conditions, logical_operator=None):

        if table_name not in self.tables:
            print(f"Table {table_name} does not exist.")

            return

        # Handle the '*' wildcard for selecting all columns
        if columns == ['*']:
            columns = list(self.tables[table_name]['columns'].keys())
        else:
            # Here, you can add additional checks to ensure that the specified columns exist in the table
            pass
        # Retrieve the table data

        table_data = self.tables[table_name]["data"]

        # Check if there's only one condition and no logical operator

        if len(conditions) == 1 and not logical_operator:

            column, operator, value = conditions[0]

            value = value.strip("'\"")  # Strip extra quotes

            for row in table_data:

                if self.evaluate_condition(row, column, operator, value):
                    self.print_row(columns, row)

            return

        # If there are multiple conditions or a logical operator is present

        # Calculate selectivity for each condition

        conditions_with_selectivity = []

        for condition in conditions:
            column = condition[0]

            selectivity = self.find_selectivity(table_name, column)

            print(f"Selectivity for column {column}: {selectivity}")

            conditions_with_selectivity.append((condition, selectivity))

        # Sort conditions based on selectivity

        if logical_operator == 'AND':

            conditions_with_selectivity.sort(key=lambda x: x[1])  # Sort by selectivity, ascending



        elif logical_operator == 'OR':

            conditions_with_selectivity.sort(key=lambda x: x[1], reverse=True)  # Sort by selectivity, descending

        print("Order of evaluation based on selectivity:", [cond[0] for cond, _ in conditions_with_selectivity])

        # Evaluate conditions in the sorted order

        for row in table_data:

            condition_results = []

            for condition, _ in conditions_with_selectivity:

                column, operator, value = condition

                value = value.strip("'\"")  # Strip extra quotes

                result = self.evaluate_condition(row, column, operator, value)

                condition_results.append(result)

                # Short-circuit evaluation

                if logical_operator == 'AND' and not result:

                    break  # No need to evaluate further as one false condition is enough for AND



                elif logical_operator == 'OR' and result:

                    break  # No need to evaluate further as one true condition is enough for OR

            # Print row if condition is met

            if logical_operator == 'AND' and all(condition_results):

                self.print_row(columns, row)



            elif logical_operator == 'OR' and any(condition_results):

                self.print_row(columns, row)



            elif not logical_operator:

                if condition_results[0]:
                    self.print_row(columns, row)

    def print_row(self, columns, row):

        selected_data = {col: row.get(col, None) for col in columns}

        print(selected_data)

    def distinct_select(self, columns, table_name):

        # Check if the table exists

        if table_name not in self.tables:
            print(f"Table {table_name} does not exist.")

            return

        # Retrieve the table data

        table_data = self.tables[table_name]["data"]

        # Use a set to store distinct values (as tuples for multiple columns)

        distinct_values = set()

        # Extract distinct values or combinations from the specified columns

        for row in table_data:
            row_values = tuple(row.get(col, None) for col in columns)

            distinct_values.add(row_values)

        # Print the distinct values or combinations

        for value in distinct_values:
            print(value)

    def extract_join_table_data(self, statement):

        select_clause_active = False

        select_found = False

        table_next = False

        columns = []

        table1_found = False

        table2_next = False

        table2_found = False

        table_name = None

        table2_name = None

        join_found = False

        natural_found = False

        column1_next = False

        column1_found = False

        column2_next = True

        column1_name = None

        column2_name = None

        operator_found = False

        operator = None

        skip_table_name = True

        for token in statement.flatten():

            # print("token value: ", token.value)

            if token.ttype is Keyword.DML and token.value.upper() == 'SELECT':
                select_clause_active = True

                select_found = True

                continue

            if token.ttype is not Punctuation and not token.is_whitespace and token.ttype is not Keyword and not table_next and skip_table_name:
                skip_table_name = False

                continue

            if token.ttype is not Punctuation and not token.is_whitespace and token.ttype is not Keyword and not table_next:
                column = token.value

                # print("column: ", column)

                columns.append(column)

                skip_table_name = True

                continue

            if token.ttype is Keyword and token.value == "FROM":
                table_next = True

                continue

            if table_next and not table1_found and token.ttype is not Punctuation and not token.is_whitespace:
                table_name = token.value

                table1_found = True

                continue

            if table1_found and token.ttype is Keyword and token.value.upper() == "NATURAL JOIN":
                natural_found = True

                continue

            if table1_found and not table2_found and not table2_next and token.ttype is Keyword and token.value.upper() == "JOIN":
                join_found = True

                table2_next = True

                continue

            if table2_next and not table2_found and token.ttype is not Punctuation and not token.is_whitespace:
                table2_name = token.value

                table2_found = True

                continue

            if table1_found and token.value == table_name:
                column1_next = True

                continue

            if column1_next and not column1_found and token.ttype is not Punctuation and token.ttype is not Operator.Comparison and not token.is_whitespace:
                column1_name = token.value

                column1_found = True

                continue

            if column1_found and not operator_found and token.ttype is Operator.Comparison:
                operator = token.value

                operator_found = True

                continue

            if table2_found and token.value == table2_name:
                column2_next = True

                continue

            if column2_next and token.ttype is not Punctuation and not token.is_whitespace and token.value != "ON":
                column2_name = token.value

                continue

        if natural_found:

            self.extract_natural_join(statement)

        else:

            self.join_tables(table_name, table2_name, column1_name, column2_name, operator, columns)

        pass

        # Use index for joining

    def extract_natural_join(self, statement):

        from_found = False

        table1_next = False

        table2_next = False

        columns = []

        skip_table_name = True

        for token in statement.flatten():

            # print("TOKEN VALUE  :  ", token.value,  "  :  TYPE  :", token.ttype)

            if token.ttype is Keyword.DML and token.value.upper() == "SELECT":
                continue

            if token.ttype is not Punctuation and not token.is_whitespace and token.ttype is not Keyword and not table1_next and skip_table_name:
                skip_table_name = False

                continue

            if not from_found and token.ttype is not Punctuation and not token.is_whitespace and token.value.upper() != 'FROM':
                column = token.value

                columns.append(column)

                skip_table_name = True

                # print("COLUMN  :  ", column)

                continue

            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_found = True

                # print("FROM")

                table1_next = True

                continue

            if table1_next and token.ttype is not Punctuation and not token.is_whitespace:
                table1_name = token.value

                table1_next = False

                continue

            if not table1_next and token.ttype is Keyword and token.value.upper() == "NATURAL JOIN":
                table2_next = True

                skip_table_name = False

                continue

            if table2_next and token.ttype is not Punctuation and not token.is_whitespace:
                table2_name = token.value

                continue

        self.join_natural_tables(table1_name, table2_name, columns)

    def join_tables(self, table1_name, table2_name, column1_name, column2_name, operator, columns):

        if table1_name not in self.tables or table2_name not in self.tables:
            print("One or both tables do not exist.")

            return []

        index_strat = False
        Sort_Merge = False
        Nested_Loop = False
        table1issmaller = False

        table1 = self.tables[table1_name]

        table2 = self.tables[table2_name]

        # Determine columns to include in the join

        if columns == ['*']:

            join_columns = set(table1['columns'].keys()).union(table2['columns'].keys())

        else:

            join_columns = set(columns)

        # Check for suitable index in table1 or table2

        suitable_index1 = self.find_suitable_index(table1.get('indexes', {}), column1_name)

        suitable_index2 = self.find_suitable_index(table2.get('indexes', {}), column2_name)

        joined_data = []

        if suitable_index1:
            index_strat = True

            # Use index from table1

            for row2 in table2["data"]:

                index_key = self.create_index_key(row2, column2_name, suitable_index1)

                if index_key in suitable_index1:

                    for row_id in suitable_index1[index_key]:
                        combined_row = self.combine_rows(table1["data"][row_id], row2, columns)

                        joined_data.append(combined_row)

        elif suitable_index2:
            index_strat = True
            # Use index from table2

            for row1 in table1["data"]:

                index_key = self.create_index_key(row1, column1_name, suitable_index2)

                if index_key in suitable_index2:

                    for row_id in suitable_index2[index_key]:
                        combined_row = self.combine_rows(row1, table2["data"][row_id], columns)

                        joined_data.append(combined_row)

        else:

            table1_data = table1["data"]

            table2_data = table2["data"]

            if len(table1_data) > 2 * len(table2_data) or len(table2_data) > 2 * len(table1_data):

                # Nested Loop Join
                # Assume table1_data, table2_data, column1_name, column2_name are defined earlier in the code.
                # Compare the lengths of the two tables to determine which one is smaller.
                if len(table1_data) < len(table2_data):
                    smaller_table = table1_data
                    larger_table = table2_data
                    smaller_column = column1_name
                    larger_column = column2_name
                    table1issmaller = True
                else:
                    smaller_table = table2_data
                    larger_table = table1_data
                    smaller_column = column2_name
                    larger_column = column1_name
                joined_data = self.nested_loop_join(smaller_table, larger_table, smaller_column, larger_column,
                                                    operator, columns)
                Nested_Loop = True
            else:
                # Merge Sort Join
                Sort_Merge = True
                joined_data = self.merge_sort_join(table1_data, table2_data, column1_name, column2_name, operator,
                                                   columns)

        self.print_joined_data(joined_data)
        if Sort_Merge:
            print("Joined Using Sort Merge...")
        elif Nested_Loop:
            print("Joined using Nested Loop...")
            if table1issmaller:
                print(f"Outer relation: {table1_name}")
                print(f"Inner relation: {table2_name}")
            else:
                print(f"Outer relation: {table2_name}")
                print(f"Inner relation: {table1_name}")
        elif index_strat:
            print("Joined Using Indices")

    def find_suitable_index(self, indexes, column_name):

        for index_name, index_data in indexes.items():

            if column_name in index_data['columns']:
                return index_data['index']

        return None

    def create_index_key(self, row, column_name, index):

        if isinstance(list(index.keys())[0], tuple):
            return (row[column_name],)

        return row[column_name]

    def combine_rows(self, row1, row2, columns):

        combined_row = {}

        for col in columns:
            combined_row[col] = row1.get(col) or row2.get(col)

        return combined_row

    def merge_sort_join(self, table1_data, table2_data, column1_name, column2_name, operator, join_columns):

        # Implement the merge sort join logic

        # Merge Sort Join

        print("MERGE SORT JOIN")

        joined_data = []

        sorted_table1 = sorted(table1_data, key=lambda x: int(x[column1_name]))

        sorted_table2 = sorted(table2_data, key=lambda x: int(x[column2_name]))

        print("SORTING COMPLETE")

        i, j, runs = 0, 0, 0

        while i < len(sorted_table1) and j < len(sorted_table2):

            if sorted_table1[i][column1_name] == sorted_table2[j][column2_name]:

                combined_row = self.combine_rows(sorted_table1[i], sorted_table2[j], join_columns)

                joined_data.append(combined_row)

                i += 1

                j += 1







            elif sorted_table1[i][column1_name] < sorted_table2[j][column2_name]:

                i += 1







            else:

                j += 1

        return joined_data

    def nested_loop_join(self, smaller_table, larger_table, smaller_column, larger_column, operator, join_columns):

        print("NESTED LOOP JOIN")

        joined_data = []

        for row1 in smaller_table:

            for row2 in larger_table:

                if self.evaluate_join_condition(row1, row2, smaller_column, larger_column, operator):
                    combined_row = self.combine_rows(row1, row2, join_columns)

                    joined_data.append(combined_row)

        return joined_data

    def join_natural_tables(self, table1_name, table2_name, columns):

        # Check if the tables exist in the database

        if table1_name not in self.tables:

            raise ValueError(f"{table1_name} is not in tables")

        elif table2_name not in self.tables:

            raise ValueError(f"{table2_name} is not in tables")

        table1 = self.tables[table1_name]["data"]

        table2 = self.tables[table2_name]["data"]

        # print(f"table1: {table1}")

        # print(f"table2: {table2}")

        for col in columns:
            print(f"column name = {col} | column type = {type(col)}")

        # Find common columns

        common_columns = set(self.tables[table1_name]["columns"]).intersection(set(self.tables[table2_name]["columns"]))

        if not common_columns:
            raise ValueError("No common columns to perform a natural join.")

        joined_data = []

        # Decide join strategy based on table sizes

        if len(table1) > 2 * len(table2) or len(table2) > 2 * len(table1):

            # Size difference is significant, use nested loop with smaller table outside

            smaller_table, larger_table = (table1, table2) if len(table1) < len(table2) else (table2, table1)

            print("USING OPTIMIZER WITH NESTED FOR LOOP SMALL CONDITION  : OUTER")

            for row1 in smaller_table:

                for row2 in larger_table:

                    if all(row1[col] == row2[col] for col in common_columns):
                        combined_row = self.merge_rows_for_natural_join(row1, row2, common_columns, columns)

                        joined_data.append(combined_row)

        else:

            # Sizes are similar, sort tables and use merge-join

            print("USING SORT MERGE TO COMBINE THESE TWO TABLES")

            sorted_table1 = sorted(table1, key=lambda x: tuple(int(x[col]) for col in common_columns))

            sorted_table2 = sorted(table2, key=lambda x: tuple(int(x[col]) for col in common_columns))

            # print(f"sorted_table1: {sorted_table1}")

            # print(f"sorted_table2: {sorted_table2}")

            i, j = 0, 0

            while i < len(sorted_table1) and j < len(sorted_table2):

                # print(f"sorted_table i col: {[sorted_table1[i][col] for col in common_columns]}")

                # print(f"sorted_table j col: {[sorted_table2[j][col] for col in common_columns]}")

                comparison = [int(sorted_table1[i][col]) - int(sorted_table2[j][col]) for col in common_columns]

                if all(x == 0 for x in comparison):

                    # Rows match, merge and add to joined_data

                    combined_row = self.merge_rows_for_natural_join(sorted_table1[i], sorted_table2[j], common_columns,

                                                                    columns)

                    joined_data.append(combined_row)

                    i += 1

                    j += 1

                elif any(x < 0 for x in comparison):

                    # sorted_table1's row comes before sorted_table2's, advance in sorted_table1

                    i += 1

                else:

                    # sorted_table2's row comes before sorted_table1's, advance in sorted_table2

                    j += 1

        self.print_joined_data(joined_data)

    def merge_rows_for_natural_join(self, row1, row2, common_columns, join_columns):

        # Combine rows for a natural join

        combined_row = {}

        for col in join_columns:

            if col in row1:

                combined_row[col] = row1[col]







            elif col in row2:

                combined_row[col] = row2[col]

        return combined_row

    def evaluate_join_condition(self, row1, row2, column1_name, column2_name, operator):

        # Convert both values to strings for a generic comparison

        value1 = str(row1.get(column1_name, ''))

        value2 = str(row2.get(column2_name, ''))

        if operator == "=":

            return value1 == value2







        elif operator == "<":

            return value1 < value2







        elif operator == ">":

            return value1 > value2







        elif operator == "<=":

            return value1 <= value2







        elif operator == ">=":

            return value1 >= value2







        elif operator == "!=":

            return value1 != value2







        else:

            return False

    def print_joined_data(self, joined_data):

        if not joined_data:
            print("No data to display.")

            return

        # Determine the column names (keys) from the first row

        columns = list(joined_data[0].keys())

        # Calculate the maximum width for each column for proper alignment

        column_widths = {col: max(len(col), max(len(str(row[col])) for row in joined_data)) for col in columns}

        # Print header row

        header_row = " | ".join(col.ljust(column_widths[col]) for col in columns)

        print(header_row)

        print("-" * len(header_row))

        # Print each row in joined data

        for row in joined_data:
            row_str = " | ".join(str(row[col]).ljust(column_widths[col]) for col in columns)

            print(row_str)

        pass

    def print_table(self, table_name):

        # Check if the table exists

        if table_name not in self.tables:
            print(f"Table {table_name} does not exist.")

            return

        # Retrieve the table data

        table_data = self.tables[table_name]

        columns = table_data['columns']

        rows = table_data['data']

        # Print column headers

        column_headers = ' | '.join(columns.keys())

        print(column_headers)

        print('-' * len(column_headers))  # Print a separator line

        # Print each row of data

        for row in rows:
            row_data = [str(row[col]) for col in columns]

            print(' | '.join(row_data))


# Example usage


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
