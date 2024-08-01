import logging
import knime.extension as knext
import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin

LOGGER = logging.getLogger(__name__)

__category = knext.category(
    path="/community/",
    level_id="salesforce_catergery",
    name="Salesforce Integration Extension",
    description="Nodes for data integration with Salesforce.",
    icon="icons/salesforce.png",
    after="",
)


@knext.node(name="Salesforce Data Loader", 
            node_type=knext.NodeType.SINK,
            icon_path="icons/upload.png",
            category=__category,
            after="",)

@knext.input_table(name="Input Data", description="Data to be loaded into Salesforce, requires an 'Id' column for update and delete operations")
class SalesforceDataLoaderNode:
    """Loads data into Salesforce: insert, update, or delete based on the specified operation."""

    user_name = knext.StringParameter("User Name", "Salesforce User Name", "")
    password = knext.StringParameter("Password", "Salesforce Password", "")
    security_token = knext.StringParameter("Security Token", "Salesforce Security Token", "")
    domain = knext.StringParameter("Domain", "Salesforce Domain", "login")
    sobject_name = knext.StringParameter("sObject Name", "Salesforce sObject Name", "")
    operation = knext.StringParameter("Operation", "Operation to perform (insert, update, delete)", "")

    def configure(self, configure_context, input_schema):
        # No specific configuration needed for input schema
        pass

    def execute(self, exec_context, input_table):
        # Login to Salesforce
        session_id, instance = SalesforceLogin(
            username=self.user_name,
            password=self.password,
            security_token=self.security_token,
            domain=self.domain
        )
        sf = Salesforce(instance=instance, session_id=session_id)
        salesforce_object = getattr(sf, self.sobject_name)

        # Convert KNIME table to pandas dataframe
        input_df = input_table.to_pandas()

        # Determine the operation to perform
        if self.operation.lower() == "insert":
            self._insert_records(salesforce_object, input_df)
        elif self.operation.lower() == "update":
            self._update_records(salesforce_object, input_df)
        elif self.operation.lower() == "delete":
            self._delete_records(salesforce_object, input_df)
        else:
            LOGGER.error("Unsupported operation specified. Use 'insert', 'update', or 'delete'.")

    def _insert_records(self, salesforce_object, input_df):
        try:
            records = input_df.to_dict(orient='records')
            for record in records:
                salesforce_object.create(record)
            LOGGER.info("All records inserted successfully into Salesforce.")
        except Exception as e:
            LOGGER.error(f"An error occurred while inserting records into Salesforce: {e}")

    def _update_records(self, salesforce_object, input_df):
        for _, row in input_df.iterrows():
            record = row.dropna().to_dict()
            record_id = record.pop('Id', None)
            if record_id is None:
                LOGGER.error("Missing 'Id' column in the input data.")
                continue
            try:
                salesforce_object.update(record_id, record)
                LOGGER.info(f"Updated record ID: {record_id} with data: {record}")
            except Exception as e:
                LOGGER.error(f"Failed to update record with ID {record_id}: {e}")

    def _delete_records(self, salesforce_object, input_df):
        for _, row in input_df.iterrows():
            record_id = row.get('Id')
            if not record_id:
                LOGGER.error("Missing 'Id' column in the input data.")
                continue
            try:
                salesforce_object.delete(record_id)
                LOGGER.info(f"Deleted record ID: {record_id}")
            except Exception as e:
                LOGGER.error(f"Failed to delete record with ID {record_id}: {e}")

        LOGGER.info("Data deletion from Salesforce completed successfully.")
