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
start_date = datetime(2024, 1, 1)

# End date (current date)
end_date = datetime.now()

# Define the query with a parameter for the date
QUERY = """
    with org_advertising_integrations as (select ai.organization_id,
                                             count(*) as num_integrations
                                      from api_app_account_integration ai
                                      where updated_at >= %(date)s
                                        and updated_at <= %(date)s
                                      group by ai.organization_id),
     org_advertising_accounts as (SELECT api_app_account_integration.organization_id AS organization_id,
                                         count(distinct p.id)                        AS num_advertising_accounts,
                                         count(distinct p.country_code)              AS num_advertising_marketplaces,
                                         count(c.id)                                 AS num_campaigns
                                  FROM api_app_account_integration
                                           JOIN api_app_account_integration_profiles ON api_app_account_integration.id =
                                                                                        api_app_account_integration_profiles.account_integration_id
                                           join api_app_profile p
                                                on api_app_account_integration_profiles.profile_id = p.id
                                           join api_app_campaign c on c.profile_id = p.id
                                  WHERE api_app_account_integration_profiles.status = 'active'
                                    and api_app_account_integration_profiles.updated_at >= %(date)s
                                    and api_app_account_integration_profiles.updated_at <= %(date)s
                                    AND api_app_account_integration_profiles.selected_status = 'active'
                                    AND api_app_account_integration.active_int = 1
                                  GROUP BY api_app_account_integration.organization_id),
     org_automations as (select at.organization_id as organization_id,
                                sum(
                                        case
                                            when at.enabled_int = 1 then 1
                                            ELSE 0
                                            end
                                )                  as enabled_automations,
                                sum(
                                        case
                                            when at.capability_id = 'time_parting'
                                                and enabled_int = 1
                                                then 1
                                            ELSE 0
                                            end
                                )                  as enabled_time_parting_automations,
                                sum(
                                        case
                                            when at.capability_id = 'roi_optimization'
                                                and enabled_int = 1
                                                then 1
                                            ELSE 0
                                            end
                                )                  as enabled_roi_optimization_automations,
                                sum(
                                        case
                                            when at.capability_id = 'advanced_budget_control'
                                                and enabled_int = 1
                                                then 1
                                            ELSE 0
                                            end
                                )                  as enabled_advanced_budget_control_automations,
                                sum(
                                        case
                                            when at.capability_id = 'asin_harvesting'
                                                and enabled_int = 1
                                                then 1
                                            ELSE 0
                                            end
                                )                  as enabled_asin_harvesting_automations,
                                sum(
                                        case
                                            when at.capability_id = 'budget_pacing'
                                                and enabled_int = 1
                                                then 1
                                            ELSE 0
                                            end
                                )                  as enabled_budget_pacing_automations,
                                sum(
                                        case
                                            when at.capability_id = 'keyword_harvesting'
                                                and enabled_int = 1
                                                then 1
                                            ELSE 0
                                            end
                                )                  as enabled_keyword_harvesting_automations,
                                sum(
                                        case
                                            when at.capability_id = 'sov_targeting'
                                                and enabled_int = 1
                                                then 1
                                            ELSE 0
                                            end
                                )                  as enabled_sov_targeting_automations
                         FROM api_app_automation_task at
                         where at.updated_at >= %(date)s
                           and at.updated_at <= %(date)s
                         GROUP BY at.organization_id),
     sp_integrations as (select scai.organization_id,
                                sum(case when sca.integration_type = 'seller_central' then 1 ELSE 0 end) as num_seller_central_accounts,
                                sum(case when sca.integration_type = 'vendor_central' then 1 ELSE 0 end) as num_vendor_central_accounts,
                                count(distinct coalesce(sca.country, sca.region))                        AS spapi_marketplaces
                         from api_app_seller_central_account_integration scai
                                  join api_app_seller_central_account sca on scai.seller_central_account_id = sca.id
                         where scai.updated_at >= %(date)s
                           and scai.updated_at <= %(date)s
                         group by scai.organization_id),
     non_free_sov_keywords as (select sks.organization_id,
                                      sum(case when sks.subscription_state = 'enabled' then 1 ELSE 0 end)  as num_enabled_sov_keywords,
                                      sum(case when sks.subscription_state != 'enabled' then 1 ELSE 0 end) as num_disabled_sov_keywords
                               from api_app_sov_keyword_subscription sks
                                        inner join api_app_sov_keyword kwd on kwd.id = sks.keyword_id
                               where kwd.downstream_managed is false
                                 and sks.updated_date >= %(date)s
                                 and sks.updated_date <= %(date)s
                               group by sks.organization_id),
     sov_keywords as (select sks.organization_id,
                             sum(case when sks.subscription_state = 'enabled' then 1 ELSE 0 end)  as num_enabled_sov_keywords,
                             sum(case when sks.subscription_state != 'enabled' then 1 ELSE 0 end) as num_disabled_sov_keywords
                      from api_app_sov_keyword_subscription sks
                      where sks.updated_date >= %(date)s
                        and sks.updated_date <= %(date)s
                      group by sks.organization_id),
     rulebooks as (select r.organization_id,
                          count(*) as num_rulebooks
                   from api_app_rulebook r
                   where r.updated_at >= %(date)s
                     and r.updated_at <= %(date)s
                   group by r.organization_id),
     dashboards as (select d.organization_id,
                           count(*) as num_dashboards
                    from api_app_dashboard d
                    where d.updated_at >= %(date)s
                      and d.updated_at <= %(date)s
                    group by d.organization_id),
     org_members as (select organization_id,
                            count(*) as num_members
                     from api_app_organization_members
                     group by organization_id),
     org_dsp_advertising_accounts as (SELECT api_app_account_integration.organization_id AS organization_id,
                                             count(distinct p.id)                        AS num_dsp_advertising_accounts,
                                             count(distinct da.id)                       AS num_dsp_advertisers
                                      FROM api_app_account_integration
                                               JOIN api_app_account_integration_profiles
                                                    ON api_app_account_integration.id =
                                                       api_app_account_integration_profiles.account_integration_id
                                               join api_app_profile p
                                                    on api_app_account_integration_profiles.profile_id = p.id
                                               join api_app_dsp_advertiser da on da.profile_id = p.id
                                      WHERE api_app_account_integration_profiles.status = 'active'
                                        AND api_app_account_integration_profiles.selected_status = 'active'
                                        AND api_app_account_integration.active_int = 1

                                      GROUP BY api_app_account_integration.organization_id),
     asin_usage as (SELECT segment.organization_id,
                           count(DISTINCT sv.product_id) total_usage
                    FROM api_app_segment_version sv
                             INNER JOIN api_app_segment segment ON (sv.segment_id = segment.id)
                             INNER JOIN api_app_organization org ON org.id = segment.organization_id
                    WHERE (segment.is_demo = 0
                        AND (sv.inactive_at IS NULL
                            OR sv.inactive_at > %(date)s)
                        AND sv.created_at <=
                            %(date)s
                        AND NOT (sv.product_id IN (SELECT DISTINCT inactive_version.product_id
                                                   FROM api_app_segment_version inactive_version
                                                            INNER JOIN api_app_segment segment_1
                                                                       ON (inactive_version.segment_id = segment_1.id)
                                                   WHERE (segment_1.is_demo = 0
                                                       AND inactive_version.inactive_at >= %(date)s
                                                       AND inactive_version.inactive_at <= %(date)s)))
                        AND NOT (sv.product_id IN (SELECT DISTINCT paused_version.product_id
                                                   FROM api_app_segment_version paused_version
                                                            INNER JOIN api_app_segment segment_2
                                                                       ON (paused_version.segment_id = segment_2.id)
                                                   WHERE (segment_2.is_demo = 0
                                                       AND (paused_version.inactive_at IS NULL
                                                           OR paused_version.inactive_at >
                                                              %(date)s)
                                                       AND paused_version.created_at <=
                                                           %(date)s
                                                       AND paused_version.paused_at <
                                                           %(date)s
                                                       AND (paused_version.inactive_at IS NULL
                                                           OR paused_version.inactive_at >
                                                              %(date)s)
                                                       AND NOT (paused_version.version_id IN
                                                                (SELECT version.version_id
                                                                 FROM api_app_segment_version version
                                                                          INNER JOIN api_app_segment segment_3
                                                                                     ON (version.segment_id = segment_3.id)
                                                                 WHERE (segment_3.is_demo = 0
                                                                     AND (version.inactive_at IS NULL
                                                                         OR version.inactive_at >
                                                                            %(date)s)
                                                                     AND version.created_at <=
                                                                         %(date)s
                                                                     AND
                                                                        segment_3.cobalt_segment_id IS NOT NULL
                                                                     AND version.created_at =
                                                                         (SELECT migrated_version.created_at
                                                                          FROM api_app_segment_version migrated_version
                                                                                   INNER JOIN api_app_segment segment_4
                                                                                              ON (migrated_version.segment_id = segment_4.id)
                                                                          WHERE (segment_4.is_demo = 0
                                                                              AND
                                                                                 (migrated_version.inactive_at IS NULL
                                                                                     OR
                                                                                  migrated_version.inactive_at >
                                                                                  %(date)s)
                                                                              AND
                                                                                 migrated_version.created_at <=
                                                                                 %(date)s
                                                                              AND
                                                                                 migrated_version.segment_id =
                                                                                 (version.segment_id))
                                                                          ORDER BY migrated_version.created_at ASC
                                                                          LIMIT 1))))))
                            AND sv.segment_id IN (SELECT DISTINCT paused_version_1.segment_id
                                                  FROM api_app_segment_version paused_version_1
                                                           INNER JOIN api_app_segment segment_5
                                                                      ON (paused_version_1.segment_id = segment_5.id)
                                                  WHERE (segment_5.is_demo = 0
                                                      AND (paused_version_1.inactive_at IS NULL
                                                          OR paused_version_1.inactive_at >
                                                             %(date)s)
                                                      AND paused_version_1.created_at <=
                                                          %(date)s
                                                      AND paused_version_1.paused_at <
                                                          %(date)s
                                                      AND (paused_version_1.inactive_at IS NULL
                                                          OR paused_version_1.inactive_at >
                                                             %(date)s)
                                                      AND NOT (paused_version_1.version_id IN
                                                               (SELECT migrated_version.version_id
                                                                FROM api_app_segment_version migrated_version
                                                                         INNER JOIN api_app_segment segment_6
                                                                                    ON (migrated_version.segment_id = segment_6.id)
                                                                WHERE (segment_6.is_demo = 0
                                                                    AND (migrated_version.inactive_at IS NULL
                                                                        OR migrated_version.inactive_at >
                                                                           %(date)s)
                                                                    AND migrated_version.created_at <=
                                                                        %(date)s
                                                                    AND
                                                                       segment_6.cobalt_segment_id IS NOT NULL
                                                                    AND migrated_version.created_at =
                                                                        (SELECT sversion.created_at
                                                                         FROM api_app_segment_version sversion
                                                                                  INNER JOIN api_app_segment seg
                                                                                             ON (sversion.segment_id = seg.id)
                                                                         WHERE (seg.is_demo = 0
                                                                             AND
                                                                                (sversion.inactive_at IS NULL
                                                                                    OR sversion.inactive_at >
                                                                                       %(date)s)
                                                                             AND sversion.created_at <=
                                                                                 %(date)s
                                                                             AND sversion.segment_id =
                                                                                 (migrated_version.segment_id))
                                                                         ORDER BY sversion.created_at ASC
                                                                         LIMIT 1)))))))
                        AND NOT (sv.version_id IN (SELECT mv.version_id
                                                   FROM api_app_segment_version mv
                                                            INNER JOIN api_app_segment s
                                                                       ON (mv.segment_id = s.id)
                                                   WHERE (s.is_demo = 0
                                                       AND (mv.inactive_at IS NULL
                                                           OR
                                                            mv.inactive_at > %(date)s)
                                                       AND mv.created_at <=
                                                           %(date)s
                                                       AND s.cobalt_segment_id IS NOT NULL
                                                       AND mv.created_at = (SELECT mv1.created_at
                                                                            FROM api_app_segment_version mv1
                                                                                     INNER JOIN api_app_segment seg
                                                                                                ON (mv1.segment_id = seg.id)
                                                                            WHERE (seg.is_demo = 0
                                                                                AND (mv1.inactive_at IS NULL
                                                                                    OR mv1.inactive_at >
                                                                                       %(date)s)
                                                                                AND mv1.created_at <=
                                                                                    %(date)s
                                                                                AND mv1.segment_id =
                                                                                    (mv.segment_id))
                                                                            ORDER BY mv1.created_at ASC
                                                                            LIMIT 1)))))
                    GROUP BY segment.organization_id)
select %(date)s::TIMESTAMP                             as day,
       o.id                                                as organization_id,
       o.name                                              as organization_name,
       o.locked                                            as organization_locked,
       o.enabled                                           as organization_enabled,
       coalesce(o.agency_id, o.id)                         as billing_entity_id,
       org_n.name                                          as billing_entity_name,
       org_n.locked                                        as billing_entity_locked,
       org_n.enabled                                       as billing_entity_enabled,
       org_n.salesforce_id                                 as billing_entity_salesforce_id,
       sum(oai.num_integrations)                           as advertising_integrations,
       sum(oaa.num_advertising_accounts)                   as advertising_accounts,
       sum(oaa.num_advertising_marketplaces)               as advertising_marketplaces,
       sum(oaa.num_campaigns)                              as advertising_campaigns,
       sum(odaa.num_dsp_advertising_accounts)              as dsp_advertising_accounts,
       sum(odaa.num_dsp_advertisers)                       as dsp_advertisers,
       sum(spi.num_seller_central_accounts)                as seller_central_accounts,
       sum(spi.num_vendor_central_accounts)                as vendor_central_accounts,
       max(spi.spapi_marketplaces)                         as spapi_marketplaces,
       max(o.sov_keyword_cap)                              as sov_keyword_cap,
       max(o.asin_cap)                                     as asin_cap,
       max(o.ad_account_cap)                               as ad_account_cap,
       sum(sk.num_enabled_sov_keywords)                    as sov_keywords_enabled,
       sum(sk.num_disabled_sov_keywords)                   as sov_keywords_disabled,
       sum(nsk.num_enabled_sov_keywords)                   as non_free_sov_keywords_enabled,
       sum(nsk.num_disabled_sov_keywords)                  as non_free_sov_keywords_disabled,
       sum(r.num_rulebooks)                                as rulebooks,
       max(o.dashboard_cap)                                as dashboard_cap,
       sum(d.num_dashboards)                               as dashboards,
       max(o.user_cap)                                     as user_cap,
       sum(om.num_members)                                 as org_members,
       sum(oa.enabled_automations)                         as enabled_automations,
       sum(oa.enabled_time_parting_automations)            as enabled_time_parting_automations,
       sum(oa.enabled_roi_optimization_automations)        as enabled_roi_optimization_automations,
       sum(oa.enabled_advanced_budget_control_automations) as enabled_advanced_budget_control_automations,
       sum(oa.enabled_asin_harvesting_automations)         as enabled_asin_harvesting_automations,
       sum(oa.enabled_budget_pacing_automations)           as enabled_budget_pacing_automations,
       sum(oa.enabled_keyword_harvesting_automations)      as enabled_keyword_harvesting_automations,
       sum(oa.enabled_sov_targeting_automations)           as enabled_sov_targeting_automations,
       cmu.total_usage                                     as asin_usage
from api_app_organization o
         join api_app_organization org_n on '' || coalesce(o.agency_id, o.id) = '' || org_n.id
         left outer join org_advertising_integrations oai on oai.organization_id = o.id
         left outer join org_advertising_accounts oaa on oaa.organization_id = o.id
         left outer join sp_integrations spi on spi.organization_id = o.id
         left outer join sov_keywords sk on sk.organization_id = o.id
         left outer join non_free_sov_keywords nsk on nsk.organization_id = o.id
         left outer join rulebooks r on r.organization_id = o.id
         left outer join dashboards d on d.organization_id = o.id
         left outer join org_members om on om.organization_id = o.id
         left outer join org_automations oa on oa.organization_id = o.id
         left outer join org_dsp_advertising_accounts odaa on odaa.organization_id = o.id
         left outer join asin_usage cmu on o.id = cmu.organization_id
where o.created_date <= %(date)s
group by o.id,
         o.name,
         o.locked,
         o.enabled,
         coalesce(o.agency_id, o.id),
         org_n.name, org_n.locked,
         org_n.enabled,
         org_n.salesforce_id,
         cmu.total_usage
order by coalesce(o.agency_id, o.id) asc
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
        "exports/daily_org_resource_report",
        f"daily_org_resource_report_{formatted_date}.csv",
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
