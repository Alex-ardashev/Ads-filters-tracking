CREATE TABLE IF NOT EXISTS hive.odyn_css.fact_ads_filters_neo
(
  ad_id                         varchar,
  site_code                     varchar,
  filter_id                     varchar,
  filter_version                varchar,
  filter_body                   varchar,
  filter_default_decision       varchar, --decision for each filter standalone
  filter_legacy                 array<varchar>,
  moderator_decision            varchar,
  filter_decision               varchar, --decision for filters combination
  date_diff_min                 varchar,
  trigger_timestamp             timestamp,
  moderator_decision_timestamp  timestamp,
  HOURS                         varchar,
  YEAR                          varchar,
  MONTH                         varchar,
  DAY                           varchar

)

WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY ['YEAR', 'MONTH', 'DAY']
    );

------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

DELETE
FROM hive.odyn_css.fact_ads_filters_neo
WHERE YEAR  = '${vars:year}'
      AND MONTH = '${vars:month}'
      AND DAY = '${vars:day}';


------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

INSERT INTO hive.odyn_css.fact_ads_filters_neo

WITH t1 AS
(
       SELECT
              CAST(json_parse(params_content_event_extras_matchescompressed) AS ARRAY<VARCHAR>) AS filter,
              params_content_event_type   AS filter_decision,
              params_content_event_adid   AS ad_id,
              CASE
                     WHEN params_content_event_sitecode = 'bg' THEN Cast( Replace(Substr(params_content_meta_received, 1, 19), 'T', ' ') || ' ' || 'UTC' AS TIMESTAMP WITH time zone) at time zone 'Europe/Sofia'
                     WHEN params_content_event_sitecode = 'kz' THEN Cast( Replace(Substr(params_content_meta_received, 1, 19), 'T', ' ') || ' ' || 'UTC' AS TIMESTAMP WITH time zone) at time zone 'Asia/Almaty'
                     WHEN params_content_event_sitecode = 'pl' THEN Cast( Replace(Substr(params_content_meta_received, 1, 19), 'T', ' ') || ' ' || 'UTC' AS TIMESTAMP WITH time zone) at time zone 'Europe/Warsaw'
                     WHEN params_content_event_sitecode = 'pt' THEN Cast( Replace(Substr(params_content_meta_received, 1, 19), 'T', ' ') || ' ' || 'UTC' AS TIMESTAMP WITH time zone) at time zone 'Europe/Lisbon'
                     WHEN params_content_event_sitecode = 'ro' THEN Cast( Replace(Substr(params_content_meta_received, 1, 19), 'T', ' ') || ' ' || 'UTC' AS TIMESTAMP WITH time zone) at time zone 'Europe/Bucharest'
                     WHEN params_content_event_sitecode = 'ua' THEN Cast( Replace(Substr(params_content_meta_received, 1, 19), 'T', ' ') || ' ' || 'UTC' AS TIMESTAMP WITH time zone) at time zone 'Europe/Kiev'
                     WHEN params_content_event_sitecode = 'uz' THEN Cast( Replace(Substr(params_content_meta_received, 1, 19), 'T', ' ') || ' ' || 'UTC' AS TIMESTAMP WITH time zone) at time zone 'Asia/Tashkent'
              END                                                                     AS event_time_local,
              params_content_event_sitecode                                           AS site_code,
                     Concat(params_content_event_adid, params_content_event_sitecode) AS ad_id_sk
       FROM   hive.hydra.global_css_decisions
       WHERE  params_content_event_source = 'ad-filters'
       AND    Cast(Concat(year,'-',month,'-',day) AS DATE) BETWEEN Date('${vars:date}') - interval '1' day AND    date('${vars:date}') ),

    t2 AS
(
       SELECT event_type AS moderator_decision,
              CASE
                     WHEN site_code = 'bg' THEN cast( meta_received_timestamp AS timestamp WITH time zone) at time zone 'Europe/Sofia'
                     WHEN site_code = 'kz' THEN cast( meta_received_timestamp AS timestamp WITH time zone) at time zone 'Asia/Almaty'
                     WHEN site_code = 'pl' THEN cast( meta_received_timestamp AS timestamp WITH time zone) at time zone 'Europe/Warsaw'
                     WHEN site_code = 'pt' THEN cast( meta_received_timestamp AS timestamp WITH time zone) at time zone 'Europe/Lisbon'
                     WHEN site_code = 'ro' THEN cast( meta_received_timestamp AS timestamp WITH time zone) at time zone 'Europe/Bucharest'
                     WHEN site_code = 'ua' THEN cast( meta_received_timestamp AS timestamp WITH time zone) at time zone 'Europe/Kiev'
                     WHEN site_code = 'uz' THEN cast( meta_received_timestamp AS timestamp WITH time zone) at time zone 'Asia/Tashkent'
              END                                              AS event_created_at_timestamp,
                     concat(cast(ad_id AS varchar), site_code) AS ad_id_dequeued_sk
       FROM   hive.odyn_css.fact_ads_dequeued
       WHERE  cast(concat(year,'-',month,'-',day) AS date) BETWEEN date('${vars:date}') - interval '1' day AND    date('${vars:date}')
       AND    event_type IN ('accept', 'edit', 'reject') ), -- for edit ratio

    t3 AS
           (
SELECT *,
    date_diff('minute', event_time_local, event_created_at_timestamp ) AS date_diff_min
FROM t1
    LEFT JOIN t2
ON t1.ad_id_sk = t2.ad_id_dequeued_sk
    AND t1.event_time_local < t2.event_created_at_timestamp
    AND t1.filter_decision in ('postmoderate', 'premoderate') -- postmoderate
    ),
    t4 AS
(
         SELECT   ad_id,
                  site_code,
                  filter,
                  moderator_decision,
                  filter_decision,
                  date_diff_min,
                  row_number() OVER ( partition BY ad_id_sk, event_created_at_timestamp ORDER BY date_diff_min) AS rownumber,
                  event_time_local                                                                              AS trigger_timestamp,
                  event_created_at_timestamp                                                                    AS moderator_decision_timestamp,
                  year(event_created_at_timestamp) as year,
                  month(event_created_at_timestamp) as month,
                  day(event_created_at_timestamp) as day
         FROM     t3
         ),

    t5 AS
(
       SELECT ad_id,
              site_code,
              filter,
              moderator_decision,
              filter_decision,
              date_diff_min,
              rownumber,
              trigger_timestamp,
              moderator_decision_timestamp,
              hour(moderator_decision_timestamp) AS hours,
              year,
              month,
              day
       FROM   t4
       WHERE  rownumber = 1
        AND DATE(moderator_decision_timestamp) = date('${vars:date}')
       UNION ALL
       SELECT ad_id,
              site_code,
              filter,
              NULL,
              filter_decision,
              NULL,
              NULL,
              event_time_local,
              NULL,
              hour(event_time_local) as hours,
              year(event_time_local) as year,
              month(event_time_local) as month,
              day(event_time_local) as day
       FROM   t1
       WHERE  filter_decision IN ( 'reject' )
        AND date(event_time_local) = date('${vars:date}') ),

    t6 AS
(
           SELECT     ad_id,
                      site_code,
                      element_at(cast(split(filter_split, '|') as ARRAY<VARCHAR>) ,1) AS filter_id,
                      element_at(cast(split(filter_split, '|') as ARRAY<VARCHAR>) ,2) AS filter_version,
                      element_at(cast(split(filter_split, '|') as ARRAY<VARCHAR>) ,3) AS filter_body,
                      element_at(cast(split(filter_split, '|') as ARRAY<VARCHAR>) ,4) AS filter_default_decision, --decision for each filter standalone

                      filter AS filter_legacy,
                      CASE
                                 WHEN filter_decision = 'reject' THEN 'autoreject'
                                 ELSE moderator_decision
                      END AS moderator_decision,
                      filter_decision,                                             --decision for filters combination
                      cast(date_diff_min as varchar) as date_diff_min,
                      trigger_timestamp,
                      moderator_decision_timestamp,
                      cast(hours as varchar) as hours,
                      cast(year as varchar) as year,
                      CASE WHEN month <10 THEN '0'||cast(month as varchar) else cast(month as varchar) end as "month",
                      CASE WHEN day <10 THEN '0'||cast(day as varchar) else cast(day as varchar) end as "day"

           FROM       t5
                      CROSS JOIN UNNEST(t5.filter) AS t(filter_split)

           )
SELECT *
FROM   t6

;
