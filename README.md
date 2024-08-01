# Salesforce Integration Extension for KNIME

## Overview

The Salesforce Integration Extension for KNIME provides a comprehensive set of nodes to facilitate seamless integration with Salesforce. These nodes allow users to perform data operations such as fetching, inserting, updating, and deleting records, as well as retrieving metadata information from Salesforce objects (sObjects). This extension is ideal for users looking to integrate their Salesforce data directly into KNIME workflows for enhanced data processing and analysis.

## Features

**Salesforce Data Loader Node:** Load data into Salesforce with support for insert, update, and delete operations. The node handles data from KNIME tables and performs the specified operations on Salesforce records.

**Salesforce Metadata Reader Node:** Retrieve metadata information about Salesforce objects. This includes details about the structure and fields of available sObjects, helping users understand their Salesforce schema.

**Salesforce Data Fetcher Node:** Extract data from Salesforce based on a specified Salesforce object (sObject) or custom SOQL query. This node allows users to fetch Salesforce data into KNIME for further processing and analysis.


## Usage

***Salesforce Data Loader Node***

#### Inputs:

- Salesforce Credentials: Enter your Salesforce username, password, and security token.
- Domain: Specify the Salesforce domain (e.g., "login").
- sObject Name: Enter the name of the Salesforce object you want to interact with.
- Operation: Choose the operation to perform (insert, update, delete).
- Input Data: Provide a KNIME table with the data to be loaded. Ensure the table contains an 'Id' column for update and delete operations.

#### Outputs:

- Successful Records: A table of records that were successfully processed.
- Failed Records: A table of records that failed to process, including error messages.
  

***Salesforce Metadata Reader Node***
#### Inputs:

- Salesforce Credentials: Enter your Salesforce username, password, and security token.
- Domain: Specify the Salesforce domain.

#### Outputs:

Output Data: A KNIME table containing the metadata of the retrieved Salesforce objects in JSON format.

***Salesforce Data Fetcher Node***
#### Inputs:

- Salesforce Credentials: Enter your Salesforce username, password, and security token.
- Domain: Specify the Salesforce domain.
- sObject Name: Specify the Salesforce object name from which data is to be fetched.
- Custom SOQL Query (Optional): Provide a custom SOQL query to retrieve specific data. This overrides the sObject name if provided.

#### Outputs:

- Output Data: A KNIME table containing the fetched Salesforce data.
Contribution


## License
This project is licensed under the MIT License.
