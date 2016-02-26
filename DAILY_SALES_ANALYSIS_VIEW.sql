CREATE VIEW
    DAILY_ANALYSIS_2015_2016
    (
        WEEK_PREVIOUS_YEAR,
        WEEK_THIS_YEAR,
        WEEK_ENDING_PREVIOUS_YEAR,
        WEEK_ENDING_THIS_YEAR,
        LASTYEARMON,
        LASTYEARTUE,
        LASTYEARWED,
        LASTYEARTHU,
        LASTYEARFRI,
        LASTYEARSAT,
        LASTYEARTOTAL,
        CURRENTYEARMON,
        CURRENTYEARTUE,
        CURRENTYEARWED,
        CURRENTYEARTHU,
        CURRENTYEARFRI,
        CURRENTYEARSAT,
        CURRENTYEARTOTAL
    ) AS
SELECT
    a.WEEK_PREVIOUS_YEAR AS WEEK_PREVIOUS_YEAR,
    b.WEEK_THIS_YEAR     AS WEEK_THIS_YEAR,
    a.week_ending        AS week_ending_previous_year,
    b.week_ending        AS week_ending_this_year,
    lastyearmon,
    lastyeartue,
    lastyearwed,
    lastyearthu,
    lastyearfri,
    lastyearsat,
    lastyeartotal,
    CASE
        WHEN currentyearmon = 0
        THEN NULL
        ELSE currentyearmon
    END AS currentyearmon,
    CASE
        WHEN currentyeartue = 0
        THEN NULL
        ELSE currentyeartue
    END AS currentyeartue,
    CASE
        WHEN currentyearwed = 0
        THEN NULL
        ELSE currentyearwed
    END AS currentyearwed,
    CASE
        WHEN currentyearthu = 0
        THEN NULL
        ELSE currentyearthu
    END AS currentyearthu,
    CASE
        WHEN currentyearfri = 0
        THEN NULL
        ELSE currentyearfri
    END AS currentyearfri,
    CASE
        WHEN currentyearsat = 0
        THEN NULL
        ELSE currentyearsat
    END AS currentyearsat,
    CASE
        WHEN currentyeartotal = 0
        THEN NULL
        ELSE currentyeartotal
    END AS currentyeartotal
FROM
    (
        SELECT
            WEEK_NO AS week_previous_year,
            week_ending,
            CASE
                WHEN SUM(MON) = 0
                THEN NULL
                ELSE SUM(MON)
            END AS lastyearmon,
            CASE
                WHEN SUM(TUE) = 0
                THEN NULL
                ELSE SUM(TUE)
            END AS lastyeartue,
            CASE
                WHEN SUM(WED) = 0
                THEN NULL
                ELSE SUM(WED)
            END AS lastyearwed,
            CASE
                WHEN SUM(THU) = 0
                THEN NULL
                ELSE SUM(THU)
            END AS lastyearthu,
            CASE
                WHEN SUM(FRI) = 0
                THEN NULL
                ELSE SUM(FRI)
            END AS lastyearfri,
            CASE
                WHEN SUM(SAT) = 0
                THEN NULL
                ELSE SUM(SAT)
            END AS lastyearsat,
            CASE
                WHEN SUM(TOTAL) = 0
                THEN NULL
                ELSE SUM(TOTAL)
            END AS lastyeartotal
        FROM
            DAILY_ANALYSIS_2015 a
        GROUP BY
            WEEK_NO,
            week_ending) a
LEFT OUTER JOIN
    (
        SELECT
            WEEK_NO AS WEEK_THIS_YEAR,
            week_ending,
            CASE
                WHEN SUM(MON) = 0
                THEN NULL
                ELSE SUM(MON)
            END AS currentyearmon,
            CASE
                WHEN SUM(TUE) = 0
                THEN NULL
                ELSE SUM(TUE)
            END AS currentyeartue,
            CASE
                WHEN SUM(WED) = 0
                THEN NULL
                ELSE SUM(WED)
            END AS currentyearwed,
            CASE
                WHEN SUM(THU) = 0
                THEN NULL
                ELSE SUM(THU)
            END AS currentyearthu,
            CASE
                WHEN SUM(FRI) = 0
                THEN NULL
                ELSE SUM(FRI)
            END AS currentyearfri,
            CASE
                WHEN SUM(SAT) = 0
                THEN NULL
                ELSE SUM(SAT)
            END AS currentyearsat,
            CASE
                WHEN SUM(TOTAL) = 0
                THEN NULL
                ELSE SUM(TOTAL)
            END AS currentyeartotal
        FROM
            DAILY_ANALYSIS_2016 a
        GROUP BY
            WEEK_NO,
            week_ending) b
ON
    a.week_previous_year=b.WEEK_THIS_YEAR;