# Single-User Relational Database Management System (RDBMS)

**Authors:** Jack Mancini & Tagan Farrell

A single-user Relational Database Management System (RDBMS). Key features include custom SQL parsing, indexing with B-Trees for efficient data retrieval, join optimization using Sort-Merge and Nested-Loop strategies, aggregation functions, and execution time measurement.

## Project Overview

The core components of our RDBMS include:
1. **SQL Parser:** A crucial component that interprets and parses SQL queries, transforming user commands into actionable instructions for the database.
2. **Indexing Structure:** This serves as the backbone for efficient data retrieval, significantly reducing search operations' time complexity.
3. **Query Optimizer:** A sophisticated element that evaluates and optimizes SQL queries, ensuring the most efficient execution path is chosen.
4. **Execution Engine:** The driving force that executes SQL commands, manipulating and retrieving data as the user requests.

Our RDBMS aims to support essential functionalities such as:
- **Schema Definition:** To establish and maintain relationships between different data sets with primary and foreign key identification.
- **Data Types:** At least integer and string types to cater to basic data representation needs.
- **Logical Operators:** Supporting at least a single two-clause logical conjunction (AND) and disjunction (OR).
- **Aggregation Operators:** Implementing at least one aggregation operator like MIN, MAX, SUM, etc.
- **Join Operations:** Capable of performing a minimum of a 2-table equal-join, crucial for relational data operations.

Further, our system embraces the complexities of index support and query optimization, specifically focusing on Sort-Merge versus Nested-Loop join selection. The execution engine is not only designed to perform tasks but also to measure the execution time of any query, providing valuable insights into the efficiency of the system.

## Design Decisions

The architecture of our single-user Relational Database Management System (RDBMS) is crafted to balance functionality, efficiency, and a deep understanding of database principles. It is structured modularly, ensuring each component operates independently and harmoniously within the greater system framework. This modularity aids future scalability and maintainability, allowing targeted updates or optimizations to individual components without overarching system disruptions.

### System Architecture

1. **SQL Parser:**
   - Central to the RDBMS, this component interprets SQL queries. It is tailored to handle a specific subset of SQL commands, aligning with the project's scope.
   - Its primary role is to tokenize SQL queries, categorize them (DDL, DML, etc.), and route them to the appropriate system component for processing.

2. **Indexing Structure:**
   - Chosen for its efficiency in managing large datasets, the `OOBTree` (Object-Oriented BTree) is the foundational indexing structure.
   - This balanced tree structure is crucial for optimizing search operations and data retrieval, especially beneficial for SELECT queries within large datasets.

3. **Query Optimizer:**
   - Although basic in its current form, the query optimizer is pivotal in selecting efficient execution paths for SQL queries, focusing on join strategy optimization (Sort-Merge vs. Nested-Loop).
   - It analyzes queries to determine the most efficient execution strategy based on existing indexes and data distribution patterns.

4. **Execution Engine:**
   - This engine is the operational core of the RDBMS, translating SQL commands into actionable data operations.
   - It is responsible for executing the processed SQL commands, interfacing with the data storage, and returning query results or statuses.

### Data Structure and Management

- **Data Types and Storage:** The RDBMS supports fundamental data types like integers and strings. Data is stored in a simple yet efficient format, with tables as collections of rows and rows as key-value pairs representing columns.
- **Table Management:** Tables are managed using dictionaries for quick access and efficient data manipulation.

### Future Expansion Considerations

- **Extensibility:** The system is designed with future expansions in mind, accommodating new SQL commands, data types, and features.
- **Performance Monitoring:** Incorporation of advanced metrics and tools for detailed performance analysis and optimization could be a future enhancement.
- **Security Features:** Future iterations of the system may include enhanced security measures like user authentication and data encryption.

This architectural framework of the RDBMS meets the immediate project requirements and lays a robust foundation for exploring and understanding the intricacies of database systems.

## Implementation

Implementing our single-user Relational Database Management System (RDBMS) is guided by a strategic selection of features and design decisions. These choices aim to create a system that not only meets the functional requirements of a basic RDBMS but also demonstrates the application of core database concepts in a practical setting.

### Key Features

- **SQL Command Support:** The system supports a range of SQL commands, including DDL commands like CREATE and DROP, and DML commands such as INSERT, SELECT, DELETE, and UPDATE. This range ensures the system's capability to perform essential database operations.

- **Basic Data Types and Constraints:** Supports integer, string, and float data types, catering to fundamental data representation. Primary and foreign key constraints are implemented, establishing relationships between tables.

- **Join Operations:** Performing 2-table equal joins is a vital feature for relational databases. The system decides between Sort-Merge and Nested-Loop join strategies based on the query optimizer's input.

- **Aggregation Functions:** Implements basic aggregation functions such as MIN, MAX, SUM, and AVERAGE, providing the capability to perform essential data analysis operations.

- **Execution Time Measurement:** The system measures and reports the execution time for each query, providing insights into the system's performance and efficiency.

- **Error Handling:** Basic error handling is integrated to manage common issues like syntax errors, invalid commands, and referencing non-existent tables or columns.

- **Foreign and Primary Key Constraint Management:** The system recognizes and enforces primary key uniqueness and foreign key referential integrity, ensuring data consistency and valid relational links between tables. During data insertion, it checks for primary and foreign key constraint adherence, rejecting inserts that violate these constraints to maintain database integrity and providing the user with an appropriate error message.

While basic, implementing this RDBMS encapsulates the fundamental aspects of database management systems. It is a testament to our understanding and application of core database principles, setting a solid foundation for future enhancements and deeper exploration into more advanced database functionalities.

## Optimization Methods

In our RDBMS implementation, we employed several optimization methods to enhance query efficiency, particularly focusing on join operations and logical condition evaluations. We implemented two distinct strategies for join operations: Nested-Loop joins and Sort-Merge joins.

When joining two tables, the system checks the table sizes, and if one is at least twice as large as the other, it performs a Nested-Loop join with the smaller relation on the outside. If the tables are closer in size, Sort-Merge is used to join the tables by sorting them on the join key and then merging them, which is more efficient for datasets of similar sizes.

Additionally, we incorporated indexing

 using `OOBTree`, which significantly improves query performance, especially for operations such as DELETE and JOIN, where we use indexing for efficient lookups. Indexing allows the system to rapidly locate and retrieve records without scanning the entire dataset, effectively reducing the query execution time. When JOINs are performed on primary keys, the join is optimized to be done using the indices as it is far more efficient.

For optimizing logical conditions, particularly those involving AND and OR statements, we utilized a selectivity function, as seen in our code. This function calculates the selectivity of individual conditions, estimating how many records will satisfy the condition. This estimation plays a crucial role in optimizing AND and OR operations. In AND operations, where all conditions must be met, the system prioritizes conditions with lower selectivity as they are more likely to be false, reducing the total number of conditions it will have to evaluate. In the reverse, the system prioritizes conditions with higher selectivity for OR operations, which require only one condition to be met. This selective approach ensures that the system processes fewer evaluations, thus speeding up query execution and enhancing overall performance. These optimized AND and OR logical operators work with SELECT, UPDATE, and DELETE.

Through these optimization methods, our RDBMS achieves a more efficient query processing mechanism, handling various queries in a manner that balances accuracy and performance, particularly for complex queries involving joins and logical conditions.

## Deficiencies

While our RDBMS implementation showcases foundational aspects of database systems, it is not without its deficiencies:

- **In-Memory Data Storage:** The system's in-memory data storage approach, while suitable for a single-user system, restricts scalability and persistence capabilities, making it unsuitable for handling large datasets or ensuring data durability across sessions.

- **Limited Error Handling:** The system's error handling is basic and lacks the sophistication to robustly manage a wider range of potential database errors and user input issues. This can lead to challenges in maintaining data integrity and handling unexpected scenarios during operation.

- **SQL Parser Limitations:** The custom SQL parser may not fully comply with all SQL standards, potentially leading to limitations in SQL command support and syntax flexibility.

- **Limited Query Optimizer:** The query optimizer focuses primarily on join operations, overlooking other potential optimization areas such as subquery optimization, more advanced index utilization, and query caching.

- **Deletion Referential Integrity:** We did not implement deletion referential integrity with Foreign Keys; when you delete a primary key that is a foreign key in a different table, all of those values in the table that reference the foreign key should also be deleted.

- **Joining Tables After Deletion:** Joining tables after deleting tuples from them may return errors, which is an issue we identified but couldn't resolve.

- **Indexing for SELECT Statements:** We did not implement indexing for SELECT statements, missing an opportunity to further optimize query performance.

## Conclusion

In summary, our project involved the development of a single-user Relational Database Management System (RDBMS), designed as an educational tool to deepen understanding and practical skills in database management. This RDBMS, implemented primarily in Python, is a foundational exploration into the intricacies of database systems, covering fundamental aspects like SQL parsing, data storage, query optimization, and basic transaction handling.

While showcasing a basic but functional RDBMS, this project underlines the complexities involved in developing a fully-fledged database system. It offers a solid foundation for future exploration and enhancements in database management technologies. The learning experience from conceptualizing to implementing various database components provides invaluable insights into the potential and challenges of database system development.
