import logging
import knime.extension as knext
from simple_salesforce import Salesforce, SalesforceLogin
import pandas as pd

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
            after="")
@knext.input_table(name="Input Data", description="Data to be loaded into Salesforce, requires an 'Id' column for update and delete operations")
@knext.output_table(name="Success Records", description="Records successfully processed.")
@knext.output_table(name="Failed Records", description="Records that failed to process.")
class SalesforceDataLoaderNode:
    """alesforce Data Loader Node
    
    The Salesforce Data Loader Node facilitates the seamless integration of data between KNIME and Salesforce.
    This node supports operations such as insert, update, and delete on Salesforce objects, making it essential for managing CRM data.
    Users need to provide Salesforce credentials, specify the Salesforce object, and define the desired operation.
    The node requires an 'Id' column in the input data for update and delete operations to function correctly.
    """

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

        # Prepare dataframes for success and failure
        success_records = pd.DataFrame()
        failed_records = pd.DataFrame()

        # Determine the operation to perform
        if self.operation.lower() == "insert":
            success_records, failed_records = self._insert_records(salesforce_object, input_df)
        elif self.operation.lower() == "update":
            success_records, failed_records = self._update_records(salesforce_object, input_df)
        elif self.operation.lower() == "delete":
            success_records, failed_records = self._delete_records(salesforce_object, input_df)
        else:
            LOGGER.error("Unsupported operation specified. Use 'insert', 'update', or 'delete'.")

        # Convert the success and failed records DataFrames to KNIME tables and return them
        return knext.Table.from_pandas(success_records), knext.Table.from_pandas(failed_records)

    def _insert_records(self, salesforce_object, input_df):
        success_records = []
        failed_records = []
        try:
            records = input_df.to_dict(orient='records')
            for record in records:
                try:
                    salesforce_object.create(record)
                    success_records.append(record)
                except Exception as e:
                    LOGGER.error(f"An error occurred while inserting record into Salesforce: {e}")
                    failed_records.append(record)
            LOGGER.info("Insert operation completed.")
        except Exception as e:
            LOGGER.error(f"An error occurred during the insert operation: {e}")
        return pd.DataFrame(success_records), pd.DataFrame(failed_records)

    def _update_records(self, salesforce_object, input_df):
        success_records = []
        failed_records = []
        for _, row in input_df.iterrows():
            record = row.dropna().to_dict()
            record_id = record.pop('Id', None)
            if record_id is None:
                LOGGER.error("Missing 'Id' column in the input data.")
                failed_records.append(record)
                continue
            try:
                salesforce_object.update(record_id, record)
                success_records.append(record)
                LOGGER.info(f"Updated record ID: {record_id}")
            except Exception as e:
                LOGGER.error(f"Failed to update record with ID {record_id}: {e}")
                failed_records.append(record)
        return pd.DataFrame(success_records), pd.DataFrame(failed_records)

    def _delete_records(self, salesforce_object, input_df):
        success_records = []
        failed_records = []
        for _, row in input_df.iterrows():
            record_id = row.get('Id')
            if not record_id:
                LOGGER.error("Missing 'Id' column in the input data.")
                failed_records.append(row.to_dict())
                continue
            try:
                salesforce_object.delete(record_id)
                success_records.append(row.to_dict())
                LOGGER.info(f"Deleted record ID: {record_id}")
            except Exception as e:
                LOGGER.error(f"Failed to delete record with ID {record_id}: {e}")
                failed_records.append(row.to_dict())
        LOGGER.info("Delete operation completed.")
        return pd.DataFrame(success_records), pd.DataFrame(failed_records)
