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
start_date = datetime(2023, 1, 31)

# End date (current date)
end_date = datetime(2023, 12, 31)

# Define the query with a parameter for the date
query = """
WITH enabled_orgs AS (SELECT id,
                             name,
                             salesforce_id,
                             agency_id,
                             asin_cap
                      FROM api_app_organization
                      WHERE enabled = 'true'
                      AND locked = 0),
     orgs_with_access_to_segments AS (SELECT organization_id
                                      FROM api_app_organization_permissions
                                      WHERE permission_id in (SELECT id
                                                              FROM auth_permission
                                                              WHERE codename IN ('feature_manage_org_segments'))
                                      UNION
                                      select organizationgroup_id
                                      from api_app_organizationgroup_permissions
                                      WHERE permission_id in (SELECT id
                                                              FROM auth_permission
                                                              WHERE codename IN ('feature_manage_org_segments'))),
     segment_org AS (SELECT org.id             AS org_id,
                            org.name,
                            org.salesforce_id,
                            org.agency_id,
                            org.asin_cap,
                            CASE
                                WHEN org.id IN (SELECT organization_id FROM orgs_with_access_to_segments)
                                    THEN TRUE
                                ELSE FALSE
                            END AS has_access_to_segment
                     FROM enabled_orgs org),
     segments_per_org AS (
         SELECT organization_id, COUNT(1)
         FROM api_app_segment
         WHERE enabled = 1
         GROUP BY organization_id
     ),
     asins_per_org AS (SELECT segment.organization_id,
                              org.name,
                              org.salesforce_id,
                              org.asin_cap,
                              count(DISTINCT sv.product_id) asin_count
                       FROM api_app_segment_version sv
                                INNER JOIN api_app_segment segment ON (sv.segment_id = segment.id)
                                INNER JOIN api_app_organization org ON org.id = segment.organization_id
                       WHERE (segment.is_demo = 0 AND
                              (sv.inactive_at IS NULL OR
                               sv.inactive_at > date_trunc('month', %(date)s)) AND
                              sv.created_at <=
                              date_trunc('month', %(date)s) + interval '1 month' - interval '1 second' AND
                              NOT (sv.product_id IN (SELECT DISTINCT inactive_version.product_id
                                                     FROM api_app_segment_version inactive_version
                                                              INNER JOIN api_app_segment segment_1
                                                                         ON (inactive_version.segment_id = segment_1.id)
                                                     WHERE (segment_1.is_demo = 0 AND
                                                            inactive_version.inactive_at >=
                                                            date_trunc('month', %(date)s) AND
                                                            inactive_version.inactive_at <=
                                                            date_trunc('month', %(date)s) + interval '1 month' -
                                                            interval '1 second'))) AND
                              NOT (sv.product_id IN (SELECT DISTINCT paused_version.product_id
                                                     FROM api_app_segment_version paused_version
                                                              INNER JOIN api_app_segment segment_2
                                                                         ON (paused_version.segment_id = segment_2.id)
                                                     WHERE (segment_2.is_demo = 0 AND
                                                            (paused_version.inactive_at IS NULL OR
                                                             paused_version.inactive_at >
                                                             date_trunc('month', %(date)s)) AND
                                                            paused_version.created_at <=
                                                            date_trunc('month', %(date)s) + interval '1 month' -
                                                            interval '1 second' AND
                                                            paused_version.paused_at <
                                                            date_trunc('month', %(date)s) AND
                                                            (paused_version.inactive_at IS NULL OR
                                                             paused_version.inactive_at >
                                                             date_trunc('month', %(date)s) + interval '1 month' - interval '1 second')
                                                            AND NOT (paused_version.version_id IN (SELECT version.version_id
                                                                                               FROM api_app_segment_version version
                                                                                                        INNER JOIN api_app_segment segment_3
                                                                                                                   ON (version.segment_id = segment_3.id)
                                                                                               WHERE (segment_3.is_demo =
                                                                                                      0 AND
                                                                                                      (version.inactive_at IS NULL OR
                                                                                                       version.inactive_at >
                                                                                                       date_trunc('month', %(date)s)) AND
                                                                                                      version.created_at <=
                                                                                                      date_trunc('month', %(date)s) +
                                                                                                      interval '1 month' -
                                                                                                      interval '1 second' AND
                                                                                                      segment_3.cobalt_segment_id IS NOT NULL AND
                                                                                                      version.created_at =
                                                                                                      (SELECT migrated_version.created_at
                                                                                                       FROM api_app_segment_version migrated_version
                                                                                                         INNER JOIN api_app_segment segment_4
                                                                                                           ON migrated_version.segment_id = segment_4.id
                                                                                                       WHERE (segment_4.is_demo =
                                                                                                              0 AND
                                                                                                              (migrated_version.inactive_at IS NULL OR
                                                                                                               migrated_version.inactive_at >
                                                                                                               date_trunc('month', %(date)s)) AND
                                                                                                              migrated_version.created_at <=
                                                                                                              date_trunc('month', %(date)s) +
                                                                                                              interval '1 month' -
                                                                                                              interval '1 second' AND
                                                                                                              migrated_version.segment_id =
                                                                                                              (version.segment_id))
                                                                                                       ORDER BY migrated_version.created_at ASC
                                                                                                       LIMIT 1)))))) AND
                                   sv.segment_id IN (SELECT DISTINCT paused_version_1.segment_id
                                                     FROM api_app_segment_version paused_version_1
                                                              INNER JOIN api_app_segment segment_5
                                                                         ON (paused_version_1.segment_id = segment_5.id)
                                                     WHERE (segment_5.is_demo = 0 AND
                                                            (paused_version_1.inactive_at IS NULL OR
                                                             paused_version_1.inactive_at >
                                                             date_trunc('month', %(date)s)) AND
                                                            paused_version_1.created_at <=
                                                            date_trunc('month', %(date)s) + interval '1 month' -
                                                            interval '1 second' AND
                                                            paused_version_1.paused_at <
                                                            date_trunc('month', %(date)s) AND
                                                            (paused_version_1.inactive_at IS NULL OR
                                                             paused_version_1.inactive_at >
                                                             date_trunc('month', %(date)s) +
                                                             interval '1 month' -
                                                             interval '1 second') AND
                                                            NOT (paused_version_1.version_id IN
                                                                 (SELECT migrated_version.version_id
                                                                  FROM api_app_segment_version migrated_version
                                                                           INNER JOIN api_app_segment segment_6
                                                                                      ON (migrated_version.segment_id = segment_6.id)
                                                                  WHERE (segment_6.is_demo = 0 AND
                                                                         (migrated_version.inactive_at IS NULL OR
                                                                          migrated_version.inactive_at >
                                                                          date_trunc('month', %(date)s)) AND
                                                                         migrated_version.created_at <=
                                                                         date_trunc('month', %(date)s) +
                                                                         interval '1 month' -
                                                                         interval '1 second' AND
                                                                         segment_6.cobalt_segment_id IS NOT NULL AND
                                                                         migrated_version.created_at =
                                                                         (SELECT sversion.created_at
                                                                          FROM api_app_segment_version sversion
                                                                                   INNER JOIN api_app_segment seg
                                                                                              ON (sversion.segment_id = seg.id)
                                                                          WHERE (seg.is_demo =
                                                                                 0 AND
                                                                                 (sversion.inactive_at IS NULL OR
                                                                                  sversion.inactive_at >
                                                                                  date_trunc('month', %(date)s)) AND
                                                                                 sversion.created_at <=
                                                                                 date_trunc('month', %(date)s) +
                                                                                 interval '1 month' -
                                                                                 interval '1 second' AND
                                                                                 sversion.segment_id =
                                                                                 (migrated_version.segment_id))
                                                                          ORDER BY sversion.created_at ASC
                                                                          LIMIT 1))))))) AND
                              NOT (sv.version_id IN (SELECT mv.version_id
                                                     FROM api_app_segment_version mv
                                                              INNER JOIN api_app_segment s
                                                                         ON (mv.segment_id = s.id)
                                                     WHERE (s.is_demo = 0 AND
                                                            (mv.inactive_at IS NULL OR
                                                             mv.inactive_at > date_trunc('month', %(date)s)) AND
                                                            mv.created_at <=
                                                            date_trunc('month', %(date)s) + interval '1 month' -
                                                            interval '1 second' AND
                                                            s.cobalt_segment_id IS NOT NULL AND
                                                            mv.created_at = (SELECT mv1.created_at
                                                                             FROM api_app_segment_version mv1
                                                                                      INNER JOIN api_app_segment seg
                                                                                                 ON (mv1.segment_id = seg.id)
                                                                             WHERE (seg.is_demo = 0 AND
                                                                                    (mv1.inactive_at IS NULL OR
                                                                                     mv1.inactive_at >
                                                                                     date_trunc('month', %(date)s)) AND
                                                                                    mv1.created_at <=
                                                                                    date_trunc('month', %(date)s) +
                                                                                    interval '1 month' -
                                                                                    interval '1 second' AND
                                                                                    mv1.segment_id = (mv.segment_id))
                                                                             ORDER BY mv1.created_at ASC
                                                                             LIMIT 1)))))
                       GROUP BY segment.organization_id,
                                org.name,
                                org.salesforce_id,
                                org.asin_cap
                       ORDER BY segment.organization_id)
SELECT segment_org.org_id,
       segment_org.name,
       segment_org.salesforce_id,
       segment_org.agency_id,
       segment_org.asin_cap,
       segment_org.has_access_to_segment,
       segments_per_org.count as segment_count,
       asins_per_org.asin_count,
       %(date)s as date
FROM segment_org
         LEFT JOIN asins_per_org ON segment_org.org_id = asins_per_org.organization_id
         LEFT JOIN segments_per_org ON segment_org.org_id = segments_per_org.organization_id
ORDER BY segment_org.has_access_to_segment DESC, segment_org.name
        """


# Cursor
cursor = conn.cursor()


# Define start and end dates for the iteration
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

# Iterate over dates
while start_date <= end_date:
    # Calculate the last day of the current month
    last_day_of_month = start_date.replace(day=28) + timedelta(days=4)
    last_day_of_month = last_day_of_month - timedelta(days=last_day_of_month.day)

    # Execute the query with the last day of the current month
    formatted_date = last_day_of_month.strftime("%Y-%m-%d")
    print(f"Exporting data for {formatted_date}")
    cursor.execute(query, {"date": start_date})

    # Fetch the results
    results = cursor.fetchall()

    # Export results to CSV
    filename = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "exports/monthly",
        f"results_{formatted_date}.csv",
    )
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([desc[0] for desc in cursor.description])  # Write headers
        csv_writer.writerows(results)  # Write data

    # Move to the next month
    start_date = last_day_of_month + timedelta(days=1)

# Close cursor and connection
cursor.close()
conn.close()
