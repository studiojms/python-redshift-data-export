import os
import psycopg2
import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection information
db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

# Connect to Redshift
conn = psycopg2.connect(**db_params)

# Start date
start_date = datetime(2023, 1, 1)

# End date (current date)
end_date = datetime.now()

# Define the query with a parameter for the date
QUERY = """
WITH ams_metadata AS
         (SELECT coalesce(api_app_organization.agency_id, api_app_organization.id) AS billing_entity_id,
                 billing_entity.name                                               AS billing_entity_name,
                 billing_entity.locked                                             AS billing_entity_locked,
                 api_app_calendar.date                                             AS report_date_day,
                 api_app_campaign.id                                               AS campaign_id
          FROM api_app_campaign
                   JOIN
               (SELECT DISTINCT api_app_account_integration.organization_id     AS organization_id,
                                api_app_account_integration_profiles.profile_id AS profile_id
                FROM api_app_account_integration
                         JOIN api_app_account_integration_profiles ON api_app_account_integration.id =
                                                                      api_app_account_integration_profiles.account_integration_id
                WHERE api_app_account_integration_profiles.status = 'active'
                  AND api_app_account_integration_profiles.selected_status = 'active'
                  AND api_app_account_integration.active_int = 1) AS distinct_org_profiles
               ON distinct_org_profiles.profile_id = api_app_campaign.profile_id
                   JOIN api_app_organization ON distinct_org_profiles.organization_id = api_app_organization.id
                   JOIN api_app_organization AS billing_entity
                        ON billing_entity.id = coalesce(api_app_organization.agency_id, api_app_organization.id)
                   JOIN api_app_calendar ON TRUE
          WHERE api_app_calendar.date >= %(date)s
            AND api_app_calendar.date <= %(date)s
          GROUP BY coalesce(api_app_organization.agency_id, api_app_organization.id),
                   billing_entity.name,
                   billing_entity.locked,
                   api_app_calendar.date,
                   api_app_campaign.id
          ORDER BY campaign_id ASC),
     ams_facts AS
         (SELECT api_app_campaign_fact_redshift.report_date                                  AS report_date_day,
                 api_app_campaign_fact_redshift.campaign_id                                  AS campaign_id,
                 sum(api_app_campaign_fact_redshift.combined_attributed_sales_14_day *
                     api_app_currency_conversion.rate)                                       AS attributed_sales_14_day__sum,
                 sum(api_app_campaign_fact_redshift.cost * api_app_currency_conversion.rate) AS cost__sum
          FROM api_app_campaign_fact_redshift
                   JOIN api_app_currency_conversion
                        ON api_app_currency_conversion.date = api_app_campaign_fact_redshift.report_date
                            AND api_app_currency_conversion.from_currency_id =
                                api_app_campaign_fact_redshift.currency_code_id
          WHERE api_app_currency_conversion.to_currency_id = 'USD'
            AND api_app_campaign_fact_redshift.report_date >= %(date)s
            AND api_app_campaign_fact_redshift.report_date <= %(date)s
          GROUP BY api_app_campaign_fact_redshift.report_date,
                   api_app_campaign_fact_redshift.campaign_id),
     facts_base AS
         (SELECT ams_metadata.billing_entity_id              AS billing_entity_id,
                 ams_metadata.billing_entity_name            AS billing_entity_name,
                 ams_metadata.billing_entity_locked          AS billing_entity_locked,
                 ams_metadata.report_date_day                AS report_date_day,
                 sum(ams_facts.attributed_sales_14_day__sum) AS attributed_sales_14_day__sum,
                 sum(ams_facts.cost__sum)                    AS cost__sum
          FROM ams_metadata
                   LEFT OUTER JOIN ams_facts ON ams_metadata.campaign_id = ams_facts.campaign_id
              AND ams_metadata.report_date_day = ams_facts.report_date_day
          GROUP BY ams_metadata.billing_entity_id, ams_metadata.billing_entity_name, ams_metadata.billing_entity_locked,
                   ams_metadata.report_date_day
          ORDER BY ams_metadata.billing_entity_id ASC NULLS LAST, ams_metadata.report_date_day ASC NULLS LAST,
                   ams_metadata.billing_entity_name ASC NULLS LAST, ams_metadata.billing_entity_locked ASC NULLS LAST)
SELECT facts_base.billing_entity_id,
       facts_base.billing_entity_name,
       facts_base.billing_entity_locked,
       facts_base.report_date_day,
       facts_base.attributed_sales_14_day__sum,
       facts_base.cost__sum
FROM facts_base
WHERE facts_base.attributed_sales_14_day__sum > 0
   OR facts_base.cost__sum > 0
ORDER BY facts_base.billing_entity_id ASC NULLS LAST,
         facts_base.report_date_day ASC NULLS LAST
        """

# Cursor
cursor = conn.cursor()

# Iterate over dates
while start_date <= end_date:
    # Execute the query with the current date
    formatted_date = start_date.strftime("%Y-%m-%d")
    print(f"Exporting data for {formatted_date}")
    cursor.execute(QUERY, {"date": start_date})

    # Fetch the results
    results = cursor.fetchall()

    # Export results to CSV
    filename = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "exports/daily_billing_entity_ad_data",
        f"daily_billing_entity_ad_data_{formatted_date}.csv",
    )
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([desc[0] for desc in cursor.description])  # Write headers
        csv_writer.writerows(results)  # Write data

    # Move to the next date
    start_date += timedelta(days=1)

# Close cursor and connection
cursor.close()
conn.close()
