import logging
import knime.extension as knext
import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin
import json

LOGGER = logging.getLogger(__name__)

__category = knext.category(
    path="/community/",
    level_id="salesforce_catergery",
    name="Salesforce Integration Extension",
    description="Nodes for data integration with Salesforce.",
    icon="icons/salesforce.png",
    after="",
)
# Salesforce Data Fetcher Node
@knext.node(name="Salesforce Data Extracter",
            node_type=knext.NodeType.SOURCE,
            icon_path="icons/download.png",
            category=__category,
            after="",)

@knext.output_table(name="Output Data", description="Extract data for the specified sObject")
class SalesforceDataExtracterNode:
    """Fetches data for the specified sObject."""

    user_name = knext.StringParameter("User Name", "Salesforce User Name", "")
    password = knext.StringParameter("Password", "Salesforce Password", "")
    security_token = knext.StringParameter("Security Token", "Salesforce Security Token", "")
    domain = knext.StringParameter("Domain", "Salesforce Domain", "login")
    sobject_name = knext.StringParameter("sObject Name", "Name of the sObject", "")
    custom_query = knext.MultilineStringParameter("Custom SOQL Query", "Custom SOQL Query (if provided, overrides sObject Name)", "")

    def configure(self, configure_context):
        # No input schema needed as we are using input field for sObject name or custom query
        pass

    def execute(self, exec_context):
        try:
            # Login to Salesforce
            session_id, instance = SalesforceLogin(
                username=self.user_name,
                password=self.password,
                security_token=self.security_token,
                domain=self.domain
            )
            sf_instance = Salesforce(instance=instance, session_id=session_id)

            # Determine the query to use
            if self.custom_query:
                querySOQL = self.custom_query
                LOGGER.info(f"Using custom query: {querySOQL}")
            else:
                # Get the sObject name from the input field
                sobject_name = self.sobject_name
                LOGGER.info(f"Fetching data for sObject: {sobject_name}")

                # Describe the sObject to get field names
                desc = getattr(sf_instance, sobject_name).describe()
                field_names = [field['name'] for field in desc['fields']]
                querySOQL = f"SELECT {','.join(field_names)} FROM {sobject_name}"
            
            # Execute the query and fetch all records
            response = sf_instance.query(querySOQL)
            records = response.get('records', [])
            next_records_url = response.get('nextRecordsUrl')

            if 'done' in response and response['done']:
                LOGGER.info("Initial query completed. No more records.")
            else:
                LOGGER.warning("Initial query did not fetch all records. Fetching more.")

            while not response.get('done'):
                response = sf_instance.query_more(next_records_url, identifier_is_url=True)
                records.extend(response.get('records', []))
                next_records_url = response.get('nextRecordsUrl')

                if not response.get('done'):
                    LOGGER.info("More records fetched.")
                else:
                    LOGGER.info("All records fetched successfully.")
            
            # Convert records to JSON string
            json_records = json.dumps(records)
            json_df = pd.DataFrame([json_records], columns=["json_records"])
            LOGGER.info("Data converted to DataFrame successfully.")
            
            return knext.Table.from_pandas(json_df)
        
        except Exception as e:
            LOGGER.error(f"An error occurred: {e}", exc_info=True)
            raise
