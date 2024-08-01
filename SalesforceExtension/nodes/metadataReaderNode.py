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

# Salesforce Metadata Reader Node
@knext.node(name="Salesforce Metadata Reader ", 
            node_type=knext.NodeType.SOURCE,
            icon_path="icons/metadata.png",
            category=__category,
            after="",)

@knext.output_table(name="Output Data", description="Metadata retrieved from Salesforce")
class SalesforceMetadataReaderNode:
    """Connects to Salesforce using provided credentials and domain and reads metadata."""

    user_name = knext.StringParameter("User Name", "Salesforce User Name", "")
    password = knext.StringParameter("Password", "Salesforce Password", "")
    security_token = knext.StringParameter("Security Token", "Salesforce Security Token", "")
    domain = knext.StringParameter("Domain", "Salesforce Domain", "login")
    def configure(self, configure_context):
        # No need to enforce schema in configure
        pass

    def execute(self, exec_context):
        try:
            LOGGER.info("Attempting to connect to Salesforce")
            
            # Login to Salesforce
            session_id, instance = SalesforceLogin(
                username=self.user_name,
                password=self.password,
                security_token=self.security_token,
                domain=self.domain
            )
            sf_instance = Salesforce(instance=instance, session_id=session_id)
            LOGGER.info("Successfully connected to Salesforce")

            # Access Metadata
            LOGGER.info("Fetching Salesforce metadata")
            all_metadata = sf_instance.describe()
            
            if all_metadata:
                LOGGER.info("Metadata fetched successfully")
                sobjects = all_metadata.get('sobjects', [])
                
                if sobjects:
                    LOGGER.info(f"Retrieved metadata for {len(sobjects)} sObjects")
                else:
                    LOGGER.warning("No sObjects metadata found in the response")
            else:
                LOGGER.warning("Metadata response is empty")
            
            # Convert metadata to JSON string
            json_metadata = json.dumps(sobjects)
            json_df = pd.DataFrame([json_metadata], columns=["metadata_json"])
            LOGGER.info("Metadata converted to DataFrame successfully")
            
            return knext.Table.from_pandas(json_df)
        
        except Exception as e:
            LOGGER.error(f"An error occurred: {e}", exc_info=True)
            raise
