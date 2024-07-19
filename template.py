from CoreEngine import engine

url = "https://api.sandbox.business.gov.au/v1/agency/911026"
snowflake_db = "BINTI_TESTING"
snowflake_schema = "BINTI_TESTING_SCHEMA"
snowflake_table = "AGENCY_WORKERS"
SB_mapping = {
    "id": "ID",
    "type": "TYPE",
    "attributes.created-at": "ATTRIBUTES_CREATED_AT",
    "attributes.updated-at": "ATTRIBUTES_UPDATED_AT",
    "attributes.name": "ATTRIBUTES_NAME",
    "attributes.email": "ATTRIBUTES_EMAIL",
    "attributes.agency-role": "ATTRIBUTES_AGENCY_ROLE",
    "attributes.role-for-permissions.id": "ATTRIBUTES_ROLE_FOR_PERMISSIONS_ID",
    "attributes.role-for-permissions.name": "ATTRIBUTES_ROLE_FOR_PERMISSIONS_NAME",
    "attributes.external-identifier": "ATTRIBUTES_EXTERNAL_IDENTIFIER",
    "attributes.sign-in-count": "ATTRIBUTES_SIGN_IN_COUNT",
    "relationships.agency-admin-assignment.data.id": "RELATIONSHIPS_AGENCY_ADMIN_ASSIGNMENT_ID",
    "relationships.agency-admin-assignment.data.type": "RELATIONSHIPS_AGENCY_ADMIN_ASSIGNMENT_TYPE"
}
operation_name = "update_insert"
key_identifier = "id"
engine_obj = engine.Engine(url, snowflake_db, snowflake_schema, snowflake_table, SB_mapping, operation_name, key_identifier)

engine_obj.start_engine()
