Overview
========

Get cost information relating to external systems your Airflow pipelines interact with. This helps you attribute cost across different levels of granularity, from the individual task-level up to data product-level in Astro Observe.

We do this by providing a cost DAG that you can deploy to your Astro Hosted deployment to start capturing cost. On an hourly schedule, the cost attribution DAG:
- Fetch query metadata from Astronomer’s API
- Query Snowflake for cost metrics
- Post cost and query attribution data back to Astronomer

❄️ **We currently support Snowflake**, with plans to expand and support other data warehouses.

Setup
================
The cost attribution DAGs can be found in the /dags folder of this repository.

The following steps will help you get started:

1. Copy and paste the snowflake_cost_attribution DAG file into your existing Astro project in the same folder where the rest of your DAGs are located. 
2. Then deploy that DAG to your existing Astro Hosted deployment (instructions here to install DAG in Astro Hosted https://www.astronomer.io/docs/astro/deploy-dags)
3. Within your Astro Hosted deployment, you'll need to set up a few required environment variables. You can set this up by going clicking on Deployment > Environment tab:
    - `ASTRO_ORGANIZATION_ID`: Your Astronomer organization ID. If you don't know it, go to Organization > Organization Settings and you'll find it on the General page.
        - Set this under *Environment Variables*
    - `AIRFLOW_VAR_AUTH_TOKEN`: API token for authenticating with Astronomer. This needs to be an Organization-level API key, not at the deployment level.
        - Set this under *Airflow Variables*
4. Verify your data warehouse connection
    - Snowflake: Setup a Snowflake connection if you haven't already under Deployment > Environment > Connections. Make sure the connection ID here matches the connection ID name used in the cost DAG (e.g. "snowflake")
5. Test the DAG
    - Trigger the DAG manually either in Astro Hosted or the Airflow UI to ensure the DAG executes successfully. Monitor the task execution in the Graph View or logs to verify successful execution.  

