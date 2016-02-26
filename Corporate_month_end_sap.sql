DROP VIEW RPTVW_CORPORATE_MONTH_END_SAP;
CREATE VIEW
    MM_CORPORATE_MONTH_END_SAP
    (
        CORPORATENAME,
        SUBS_ID,
        SUPPLY,
        GROSS_AMOUNT,
        VAT_AMOUNT,
        NET_AMOUNT,
        DISCOUNT_GROSS_AMOUNT,
        DISCOUNT_VAT_AMOUNT,
        DISCOUNT_NET_AMOUNT,
        OTHER_DISCOUNT_PERCENTAGE,
        COST_TO_IT,
        REVENUE_TO_IT
    ) AS

SELECT
    CORPORATENAME,
    SUBS_ID,
    SUM(SUPPLY)                    AS SUPPLY,
    SUM(GROSS_AMOUNT)              AS GROSS_AMOUNT,
    SUM(VAT_AMOUNT)                AS VAT_AMOUNT,
    SUM(NET_AMOUNT)                AS NET_AMOUNT,
    SUM(DISCOUNT_GROSS_AMOUNT)     AS DISCOUNT_GROSS_AMOUNT,
    SUM(DISCOUNT_VAT_AMOUNT)       AS DISCOUNT_VAT_AMOUNT,
    SUM(DISCOUNT_NET_AMOUNT)       AS DISCOUNT_NET_AMOUNT,
    AVG(OTHER_DISCOUNT_PERCENTAGE) AS OTHER_DISCOUNT_PERCENTAGE,
    SUM(COST_TO_IT)                AS COST_TO_IT,
    SUM(REVENUE_TO_IT)             AS REVENUE_TO_IT
FROM
    (
        SELECT
            CASE
                WHEN TITLE_NAME LIKE '%Irish Times%'
                THEN 'The Irish Times'
                ELSE 'Other'
            END AS title_grouping,
            DISC_RATE,
            SORD_ID,
            SUBS_ID,
            VATRATE,
            SUPPLY,
            RATE_PRICE,
            DISCOUNTRATE,
            CORPORATENAME,
            DAY_OF_WEEK,
            ISSUE_DATE,
            gross_amount,
            gross_amount*vatrate/100                          AS vat_amount,
            gross_amount*(1-vatrate/100)                      AS net_amount,
            gross_amount*(1-discountrate/100)                AS Discount_gross_amount,
            (gross_amount*vatrate/100)*(1-discountrate/100)   AS Discount_vat_amount,
            gross_amount*(1-vatrate/100)*(1-discountrate/100) AS Discount_net_amount,
            CASE
                WHEN title_name = 'The Financial Times'
                AND DAY_OF_WEEK='SAT'
                THEN 0.3117
                ELSE DISC_RATE
            END AS other_discount_percentage,
            (gross_amount*(1-vatrate/100))-(gross_amount*(1-vatrate/100)*(
                CASE
                    WHEN title_name = 'The Financial Times'
                    AND DAY_OF_WEEK='SAT'
                    THEN 0.3117
                    ELSE DISC_RATE
                END)) AS cost_to_it,
            (gross_amount*(1-vatrate/100)*(1-discountrate/100))-((gross_amount*(1-vatrate/100))-
            (gross_amount*(1-vatrate/100)*(
                CASE
                    WHEN title_name = 'The Financial Times'
                    AND DAY_OF_WEEK='SAT'
                    THEN 0.3117
                    ELSE DISC_RATE
                END))) AS revenue_to_it
        FROM
            (
                SELECT
                    RATEHEAD_ID,
                    SORD_ID,
                    SUBS_ID,
                    VATRATE,
                    SUPPLY,
                    RATE_PRICE,
                    DISCOUNTRATE,
                    CORPORATENAME,
                    DAY_OF_WEEK,
                    ISSUE_DATE,
                    SUPPLY*RATE_PRICE               AS gross_amount,
                    EXTRACT (MONTH FROM ISSUE_DATE) AS month_corp,
                    EXTRACT (MONTH FROM SYSDATE)    AS current_month
                FROM
                    TBL_IT_CORPORATE_BILLING a
                INNER JOIN
                    TBL_IT_CORPORATE_BILLING_HEAD b
                ON
                    a.SEQUENCENO=b.SEQUENCENO) a
        INNER JOIN
            (
                SELECT
                    DISC_RATE,
                    TITLE_NAME,
                    RATEHEAD_ID
                FROM
                    RATEHEAD_ID_RENAME a
                LEFT JOIN
                    TBL_IT_CORPORATE_3P_DISC_REC
                ON
                    TITLE_NAME=TITLE) b
        ON
            b.RATEHEAD_ID=a.RATEHEAD_ID
        WHERE
            month_corp = current_month-1 ) a
GROUP BY
    CORPORATENAME,
    SUBS_ID;