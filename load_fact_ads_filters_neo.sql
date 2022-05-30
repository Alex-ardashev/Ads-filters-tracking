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


-- REDUNDANCY SCORE --

CREATE TABLE IF not EXISTS hive.odyn_css.fact_ads_filters_neo_dashboard_redundancy_score
             (
                          site_code                     VARCHAR,
                          filter_id                     VARCHAR,
                          filter_version                VARCHAR,
                          filter_body                   VARCHAR,
                          filter_default_decision       VARCHAR,
                          filter_detection              INTEGER,
                          filter_detection_only         INTEGER,
                          avg_filter_false_positive     DOUBLE,
                          filter_false_positive_only    INTEGER,
                          YEAR                          VARCHAR,
                          MONTH                         VARCHAR,
                          DAY                           VARCHAR
             )
WITH
             (
                          format = 'PARQUET',
                          partitioned_by = ARRAY ['YEAR', 'MONTH', 'DAY']
             );

------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

DELETE
FROM   hive.odyn_css.fact_ads_filters_neo_dashboard_redundancy_score
WHERE     YEAR = '${vars:year}'
      AND MONTH = '${vars:month}'
      AND DAY = '${vars:day}';

------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

INSERT INTO hive.odyn_css.fact_ads_filters_neo_dashboard_redundancy_score

WITH filternormal_first AS
(
         SELECT   params_content_event_adid                                                        AS adid,
                  params_content_event_sitecode                                                    AS site_code,
                  cast(Json_parse(params_content_event_extra_matchescompressed) AS ARRAY<varchar>) AS filter,
                  year,
                  month,
                  day,
                  max(
                  CASE
                           WHEN params_content_event_extra_verdictsrules IS NOT NULL
                           AND      params_content_event_extra_verdictsrules != '[]'
                           AND      params_content_event_extra_verdictsrules != '["ad-filters"]' THEN 1
                           ELSE 0
                  END) AS ad_scorer_detection
                  -- ad scorer or some other rule detection to be precise, it happens only
                  -- when verdict rules contains something else than
                  -- empty collection / null / just ad-filters
         FROM     hive.hydra.global_css_verdicts
         WHERE    params_content_event_source = 'ad-filters'
         AND      cast(concat(year,'-',month,'-',day) AS date) = date('${vars:date}')
         GROUP BY 1,2,3,4,5,6
    ), 

filternormal AS
(
           SELECT     adid,
                      site_code,
                      ad_scorer_detection,
                      element_at(cast(split(filter_split, '|') AS array<varchar>) ,1) AS filter_id,
                      element_at(cast(split(filter_split, '|') as ARRAY<VARCHAR>) ,2) AS filter_version,
                      element_at(cast(split(filter_split, '|') as ARRAY<VARCHAR>) ,3) AS filter_body,
                      element_at(cast(split(filter_split, '|') as ARRAY<VARCHAR>) ,4) AS filter_default_decision,

                      year,
                      month,
                      day
           FROM       filternormal_first v
           CROSS JOIN unnest(v.filter) AS t(filter_split) 
    ), 

filterotherverdicts_first AS
(
         SELECT   params_content_event_adid AS adid,
                  params_content_event_sitecode AS site_code,
                  cast(
                      COALESCE( 
                      json_extract(params_content_event_extra_otherverdicts, '$[0].extra.matchesComressed'), 
                      json_extract(params_content_event_extra_otherverdicts, '$[1].extra.matchesCompressed'), 
                      json_extract(params_content_event_extra_otherverdicts, '$[2].extra.matchesCompressed'), 
                      json_extract(params_content_event_extra_otherverdicts, '$[3].extra.matchesCompressed') ) 
                      AS array<varchar>) AS filter,
                  year,
                  month,
                  day,
                  max(
                  CASE
                      WHEN params_content_event_source = 'ad-scorer' THEN 1
                      ELSE 0
                  END) AS ad_scorer_detection
         FROM     hive.hydra.global_css_verdicts
         WHERE    params_content_event_source != 'ad-filters'
         AND      cast(concat(year,'-',month,'-',day) AS date) = date('${vars:date}')
         GROUP BY 1,2,3,4,5,6), 

filterotherverdicts AS
(
           SELECT     adid,
                      site_code,
                      ad_scorer_detection,
                      element_at(cast(split(filter_split, '|') AS array<varchar>) ,1) AS filter_id,
                      element_at(cast(split(filter_split, '|') as ARRAY<VARCHAR>) ,2) AS filter_version,
                      element_at(cast(split(filter_split, '|') as ARRAY<VARCHAR>) ,3) AS filter_body,
                      element_at(cast(split(filter_split, '|') as ARRAY<VARCHAR>) ,4) AS filter_default_decision,

                      year,
                      month,
                      day
           FROM       filterotherverdicts_first v
           CROSS JOIN unnest(v.filter) AS t(filter_split) ), 

filter AS
(
       SELECT *
       FROM   filternormal
       UNION ALL
       SELECT *
       FROM   filterotherverdicts ), 
       
adscorerpremod AS
(
                SELECT DISTINCT params_content_event_adid     AS adid,
                                params_content_event_sitecode AS site_code
                FROM            hive.hydra.global_css_verdicts
                WHERE           params_content_event_source = 'ad-scorer'
                AND             params_content_meta_type IN ('ad.verdict.reject',
                                                             'ad.verdict.premoderate')
                AND             cast(concat(year,'-',month,'-',day) AS date) = date('${vars:date}') ), 

moderatordecisions AS
(
                SELECT DISTINCT params_content_event_adid     AS adid,
                                params_content_event_sitecode AS site_code
                FROM            hive.hydra.global_css_decisions
                WHERE           params_content_event_moderationcategory = 'default'
                AND             params_content_event_type = 'reject'
                AND             cast(concat(year,'-',month,'-',day) AS date) = date('${vars:date}') ), 

data AS
(
          SELECT    f.filter_id,
                    f.filter_version,
                    f.filter_body,
                    f.filter_default_decision,
                    f.site_code,
                    f.year,
                    f.month,
                    f.day,
                    CASE
                              WHEN m.adid IS NOT NULL
                              AND       f.adid IS NOT NULL THEN 1
                              ELSE 0
                    END AS filter_detection,
                    CASE
                              WHEN m.adid IS NOT NULL
                              AND       f.adid IS NOT NULL
                              AND       a.adid IS NULL
                              AND       f.ad_scorer_detection = 0 THEN 1
                              ELSE 0
                    END AS filter_detection_only,
                    CASE
                              WHEN m.adid IS NULL
                              AND       f.adid IS NOT NULL THEN 1
                              ELSE 0
                    END AS filter_fp,
                    CASE
                              WHEN m.adid IS NULL
                              AND       f.adid IS NOT NULL
                              AND       a.adid IS NULL THEN 1
                              ELSE 0
                    END AS filter_fp_only
          FROM      filter f
          LEFT JOIN moderatordecisions m
          ON        m.adid = f.adid
          AND       m.site_code = f.site_code
          LEFT JOIN adscorerpremod a
          ON        m.adid = a.adid
          AND       m.site_code = a.site_code )
SELECT   site_code,
         filter_id,
         filter_version,
         filter_body,
         filter_default_decision,
         sum(filter_detection)      filter_detection,
         sum(filter_detection_only) filter_detection_only,
         avg(filter_fp)             AVG_filter_false_positive,
         sum(filter_fp_only)        filter_false_positive_only,
         year,
         month,
         day
         -- 100.0 * (sum(filter_detection) - sum(filter_detection_only)) / sum(filter_detection) as coverage,
         -- 100.0 - 100.0*avg(filter_fp) as precision
FROM     data
GROUP BY 1,2,3,4,5,10,11,12

;


-- Dashboard --

CREATE TABLE IF not EXISTS hive.odyn_css.fact_ads_filters_neo_dashboard
             (
                          site_code          VARCHAR,
                          filter_id          VARCHAR,
                          filter_version     VARCHAR,
                          filter_body        VARCHAR,
                          filter_default_decision       VARCHAR, --decision for each filter standalone
                          moderator_decision VARCHAR,
                          filter_decision    VARCHAR,            --decision for filters combination
                          trigger_timestamp  TIMESTAMP,
                          hours              VARCHAR,
                          total_decisions    INTEGER,
                          YEAR               VARCHAR,
                          MONTH              VARCHAR,
                          DAY                VARCHAR
             )
WITH
             (
                          format = 'PARQUET',
                          partitioned_by = ARRAY ['YEAR', 'MONTH', 'DAY']
             );

------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

DELETE
FROM   hive.odyn_css.fact_ads_filters_neo_dashboard
WHERE YEAR = '${vars:year}'
      AND MONTH = '${vars:month}'
      AND DAY = '${vars:day}';

------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

INSERT INTO hive.odyn_css.fact_ads_filters_neo_dashboard
SELECT site_code,
       filter_id,
       filter_version,
       filter_body,
       filter_default_decision, --decision for each filter standalone
       moderator_decision,
       filter_decision, --decision for filters combination
       trigger_timestamp,
       hours,
       count(ad_id) as total_decisions,
       YEAR,
       MONTH,
       DAY
FROM   hive.odyn_css.fact_ads_filters_neo
WHERE  cast(concat(year,'-',month,'-',day) AS DATE) = date('${vars:date}')
group by 1,2,3,4,5,6,7,8,9,11,12,13
;
            


-- Sampling --

CREATE TABLE IF not EXISTS hive.odyn_css.fact_ads_filters_neo_dashboard_sampled
             (
                          site_code          VARCHAR,
                          filter_id          VARCHAR,
                          filter_version     VARCHAR,
                          filter_body        VARCHAR,
                          filter_default_decision       VARCHAR, --decision for each filter standalone
                          moderator_decision VARCHAR,
                          filter_decision    VARCHAR,            --decision for filters combination
                          trigger_timestamp  TIMESTAMP,
                          ad_id              VARCHAR,
                          hours              VARCHAR,
                          YEAR               VARCHAR,
                          MONTH              VARCHAR,
                          DAY                VARCHAR
             )
WITH
             (
                          format = 'PARQUET',
                          partitioned_by = ARRAY ['YEAR', 'MONTH', 'DAY']
             );

------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

DELETE
FROM   hive.odyn_css.fact_ads_filters_neo_dashboard_sampled
WHERE YEAR <= '${vars:year}'
      AND MONTH <= '${vars:month}'
      AND DAY <= '${vars:day}';

------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

INSERT INTO hive.odyn_css.fact_ads_filters_neo_dashboard_sampled


WITH tab1 AS (
                     SELECT site_code,
                            filter_id,
                            filter_version,
                            filter_body,
                            filter_default_decision, --decision for each filter standalone
                            moderator_decision,
                            filter_decision, --decision for filters combination
                            trigger_timestamp,
                            ad_id,
                            ROW_NUMBER() OVER (PARTITION BY site_code,filter_id, filter_version order by trigger_timestamp desc) AS rn,
                            hours,
                            YEAR,
                            MONTH,
                            DAY
                     FROM   hive.odyn_css.fact_ads_filters_neo
                     WHERE  filter_decision != 'reject' AND moderator_decision = 'accept'
                     AND    cast(concat(year,'-',month,'-',day) AS DATE) BETWEEN Date('${vars:date}') - interval '2' day AND Date('${vars:date}')
            )

SELECT                      site_code,
                            filter_id,
                            filter_version,
                            filter_body,
                            filter_default_decision, --decision for each filter standalone
                            moderator_decision,
                            filter_decision, --decision for filters combination
                            trigger_timestamp,
                            ad_id,
                            hours,
                            YEAR,
                            MONTH,
                            DAY
FROM tab1
WHERE rn < 6
order by site_code, filter_id, filter_version
;


