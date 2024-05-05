use ma;
set profiling = 1;
CREATE TABLE ma_profile_final (
    QUERY_ID INT,
    SEQ INT,
    STATE VARCHAR(500),
    DURATION DECIMAL(10,2),
    CPU_USER DECIMAL(10,2),
    CPU_SYSTEM DECIMAL(10,2),
    CONTEXT_VOLUNTARY INT,
    CONTEXT_INVOLUNTARY INT,
    BLOCK_OPS_IN INT,
    BLOCK_OPS_OUT INT,
    MESSAGES_SENT INT,
    MESSAGES_RECEIVED INT,
    PAGE_FAULTS_MAJOR INT,
    PAGE_FAULTS_MINOR INT,
    SWAPS INT,
    SOURCE_FUNCTION VARCHAR(500), -- Adjusted length to 50 characters
    SOURCE_FILE VARCHAR(500),
    SOURCE_LINE INT
);

-- start query 1 in stream 0 using template query1.tpl 
WITH customer_total_return 
     AS (SELECT sr_customer_sk     AS ctr_customer_sk, 
                sr_store_sk        AS ctr_store_sk, 
                Sum(sr_return_amt) AS ctr_total_return 
         FROM   store_returns, 
                date_dim 
         WHERE  sr_returned_date_sk = d_date_sk 
                AND d_year = 2001 
         GROUP  BY sr_customer_sk, 
                   sr_store_sk) 
SELECT c_customer_id 
FROM   customer_total_return ctr1, 
       store, 
       customer 
WHERE  ctr1.ctr_total_return > (SELECT Avg(ctr_total_return) * 1.2 
                                FROM   customer_total_return ctr2 
                                WHERE  ctr1.ctr_store_sk = ctr2.ctr_store_sk) 
       AND s_store_sk = ctr1.ctr_store_sk 
       AND s_state = 'TN' 
       AND ctr1.ctr_customer_sk = c_customer_sk 
ORDER  BY c_customer_id
LIMIT 100;

-- start query 2 in stream 0 using template query2.tpl  
WITH wscs AS (
    SELECT
        ws_sold_date_sk AS sold_date_sk,
        ws_ext_sales_price AS sales_price
    FROM
        web_sales
    UNION ALL
    SELECT
        cs_sold_date_sk AS sold_date_sk,
        cs_ext_sales_price AS sales_price
    FROM
        catalog_sales
),
wswscs AS (
    SELECT
        d_week_seq,
        SUM(CASE WHEN d_day_name = 'Sunday' THEN sales_price ELSE NULL END) AS sun_sales,
        SUM(CASE WHEN d_day_name = 'Monday' THEN sales_price ELSE NULL END) AS mon_sales,
        SUM(CASE WHEN d_day_name = 'Tuesday' THEN sales_price ELSE NULL END) AS tue_sales,
        SUM(CASE WHEN d_day_name = 'Wednesday' THEN sales_price ELSE NULL END) AS wed_sales,
        SUM(CASE WHEN d_day_name = 'Thursday' THEN sales_price ELSE NULL END) AS thu_sales,
        SUM(CASE WHEN d_day_name = 'Friday' THEN sales_price ELSE NULL END) AS fri_sales,
        SUM(CASE WHEN d_day_name = 'Saturday' THEN sales_price ELSE NULL END) AS sat_sales
    FROM
        wscs
    JOIN
        date_dim ON date_dim.d_date_sk = wscs.sold_date_sk
    GROUP BY
        d_week_seq
)
SELECT
    d_week_seq1,
    ROUND(sun_sales1 / sun_sales2, 2) AS sun_sales_ratio,
    ROUND(mon_sales1 / mon_sales2, 2) AS mon_sales_ratio,
    ROUND(tue_sales1 / tue_sales2, 2) AS tue_sales_ratio,
    ROUND(wed_sales1 / wed_sales2, 2) AS wed_sales_ratio,
    ROUND(thu_sales1 / thu_sales2, 2) AS thu_sales_ratio,
    ROUND(fri_sales1 / fri_sales2, 2) AS fri_sales_ratio,
    ROUND(sat_sales1 / sat_sales2, 2) AS sat_sales_ratio
FROM
    (
        SELECT
            wswscs.d_week_seq AS d_week_seq1,
            sun_sales AS sun_sales1,
            mon_sales AS mon_sales1,
            tue_sales AS tue_sales1,
            wed_sales AS wed_sales1,
            thu_sales AS thu_sales1,
            fri_sales AS fri_sales1,
            sat_sales AS sat_sales1
        FROM
            wswscs
        JOIN
            date_dim ON date_dim.d_week_seq = wswscs.d_week_seq
        WHERE
            d_year = 1998
    ) AS y
JOIN
    (
        SELECT
            wswscs.d_week_seq AS d_week_seq2,
            sun_sales AS sun_sales2,
            mon_sales AS mon_sales2,
            tue_sales AS tue_sales2,
            wed_sales AS wed_sales2,
            thu_sales AS thu_sales2,
            fri_sales AS fri_sales2,
            sat_sales AS sat_sales2
        FROM
            wswscs
        JOIN
            date_dim ON date_dim.d_week_seq = wswscs.d_week_seq
        WHERE
            d_year = 1999
    ) AS z ON y.d_week_seq1 = z.d_week_seq2 - 53
ORDER BY
    d_week_seq1;



-- start query 3 in stream 0 using template query3.tpl 
SELECT dt.d_year,
       item.i_brand_id AS brand_id,
       item.i_brand AS brand,
       SUM(ss_ext_discount_amt) AS sum_agg
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manufact_id = 427
      AND dt.d_moy = 11
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id
LIMIT 100;

-- query 4
WITH wscs AS (
    SELECT
        ws_sold_date_sk AS sold_date_sk,
        ws_ext_sales_price AS sales_price
    FROM
        web_sales
    UNION ALL
    SELECT
        cs_sold_date_sk AS sold_date_sk,
        cs_ext_sales_price AS sales_price
    FROM
        catalog_sales
),
wswscs AS (
    SELECT
        d_week_seq,
        SUM(CASE WHEN DAYNAME(d_date) = 'Sunday' THEN sales_price ELSE NULL END) AS sun_sales,
        SUM(CASE WHEN DAYNAME(d_date) = 'Monday' THEN sales_price ELSE NULL END) AS mon_sales,
        SUM(CASE WHEN DAYNAME(d_date) = 'Tuesday' THEN sales_price ELSE NULL END) AS tue_sales,
        SUM(CASE WHEN DAYNAME(d_date) = 'Wednesday' THEN sales_price ELSE NULL END) AS wed_sales,
        SUM(CASE WHEN DAYNAME(d_date) = 'Thursday' THEN sales_price ELSE NULL END) AS thu_sales,
        SUM(CASE WHEN DAYNAME(d_date) = 'Friday' THEN sales_price ELSE NULL END) AS fri_sales,
        SUM(CASE WHEN DAYNAME(d_date) = 'Saturday' THEN sales_price ELSE NULL END) AS sat_sales
    FROM
        wscs
    JOIN
        date_dim ON date_dim.d_date_sk = wscs.sold_date_sk
    GROUP BY
        d_week_seq
)
SELECT
    d_week_seq1,
    ROUND(sun_sales1 / sun_sales2, 2) AS sun_sales_ratio,
    ROUND(mon_sales1 / mon_sales2, 2) AS mon_sales_ratio,
    ROUND(tue_sales1 / tue_sales2, 2) AS tue_sales_ratio,
    ROUND(wed_sales1 / wed_sales2, 2) AS wed_sales_ratio,
    ROUND(thu_sales1 / thu_sales2, 2) AS thu_sales_ratio,
    ROUND(fri_sales1 / fri_sales2, 2) AS fri_sales_ratio,
    ROUND(sat_sales1 / sat_sales2, 2) AS sat_sales_ratio
FROM
    (
        SELECT
            wswscs.d_week_seq AS d_week_seq1,
            sun_sales AS sun_sales1,
            mon_sales AS mon_sales1,
            tue_sales AS tue_sales1,
            wed_sales AS wed_sales1,
            thu_sales AS thu_sales1,
            fri_sales AS fri_sales1,
            sat_sales AS sat_sales1
        FROM
            wswscs
        JOIN
            date_dim ON date_dim.d_week_seq = wswscs.d_week_seq
        WHERE
            YEAR(d_date) = 1998
    ) AS y
JOIN
    (
        SELECT
            wswscs.d_week_seq AS d_week_seq2,
            sun_sales AS sun_sales2,
            mon_sales AS mon_sales2,
            tue_sales AS tue_sales2,
            wed_sales AS wed_sales2,
            thu_sales AS thu_sales2,
            fri_sales AS fri_sales2,
            sat_sales AS sat_sales2
        FROM
            wswscs
        JOIN
            date_dim ON date_dim.d_week_seq = wswscs.d_week_seq
        WHERE
            YEAR(d_date) = 1999
    ) AS z ON y.d_week_seq1 = z.d_week_seq2 - 53
ORDER BY
    d_week_seq1;


-- quey 5
WITH ssr AS (
    SELECT
        s_store_id,
        SUM(sales_price) AS sales,
        SUM(profit) AS profit,
        SUM(return_amt) AS returns1,
        SUM(net_loss) AS profit_loss
    FROM (
        SELECT
            ss_store_sk AS store_sk,
            ss_sold_date_sk AS date_sk,
            ss_ext_sales_price AS sales_price,
            ss_net_profit AS profit,
            CAST(0 AS DECIMAL(7,2)) AS return_amt,
            CAST(0 AS DECIMAL(7,2)) AS net_loss
        FROM
            store_sales
        UNION ALL
        SELECT
            sr_store_sk AS store_sk,
            sr_returned_date_sk AS date_sk,
            CAST(0 AS DECIMAL(7,2)) AS sales_price,
            CAST(0 AS DECIMAL(7,2)) AS profit,
            sr_return_amt AS return_amt,
            sr_net_loss AS net_loss
        FROM
            store_returns
    ) AS salesreturns
    JOIN date_dim ON date_sk = d_date_sk
    JOIN store ON store_sk = s_store_sk
    WHERE
        d_date BETWEEN CAST('2002-08-22' AS DATE) AND (CAST('2002-08-22' AS DATE) + INTERVAL '14' day)
    GROUP BY
        s_store_id
),
csr AS (
    SELECT
        cp_catalog_page_id,
        SUM(sales_price) AS sales,
        SUM(profit) AS profit,
        SUM(return_amt) AS returns1,
        SUM(net_loss) AS profit_loss
    FROM (
        SELECT
            cs_catalog_page_sk AS page_sk,
            cs_sold_date_sk AS date_sk,
            cs_ext_sales_price AS sales_price,
            cs_net_profit AS profit,
            CAST(0 AS DECIMAL(7,2)) AS return_amt,
            CAST(0 AS DECIMAL(7,2)) AS net_loss
        FROM
            catalog_sales
        UNION ALL
        SELECT
            cr_catalog_page_sk AS page_sk,
            cr_returned_date_sk AS date_sk,
            CAST(0 AS DECIMAL(7,2)) AS sales_price,
            CAST(0 AS DECIMAL(7,2)) AS profit,
            cr_return_amount AS return_amt,
            cr_net_loss AS net_loss
        FROM
            catalog_returns
    ) AS salesreturns
    JOIN date_dim ON date_sk = d_date_sk
    JOIN catalog_page ON page_sk = cp_catalog_page_sk
    WHERE
        d_date BETWEEN CAST('2002-08-22' AS DATE) AND (CAST('2002-08-22' AS DATE) + INTERVAL '14' day)
    GROUP BY
        cp_catalog_page_id
),
wsr AS (
    SELECT
        web_site_id,
        SUM(sales_price) AS sales,
        SUM(profit) AS profit,
        SUM(return_amt) AS returns1,
        SUM(net_loss) AS profit_loss
    FROM (
        SELECT
            ws_web_site_sk AS wsr_web_site_sk,
            ws_sold_date_sk AS date_sk,
            ws_ext_sales_price AS sales_price,
            ws_net_profit AS profit,
            CAST(0 AS DECIMAL(7,2)) AS return_amt,
            CAST(0 AS DECIMAL(7,2)) AS net_loss
        FROM
            web_sales
        UNION ALL
        SELECT
            ws_web_site_sk AS wsr_web_site_sk,
            wr_returned_date_sk AS date_sk,
            CAST(0 AS DECIMAL(7,2)) AS sales_price,
            CAST(0 AS DECIMAL(7,2)) AS profit,
            wr_return_amt AS return_amt,
            wr_net_loss AS net_loss
        FROM
            web_returns
        LEFT OUTER JOIN web_sales ON wr_item_sk = ws_item_sk AND wr_order_number = ws_order_number
    ) AS salesreturns
    JOIN date_dim ON date_sk = d_date_sk
    JOIN web_site ON wsr_web_site_sk = web_site_sk
    WHERE
        d_date BETWEEN CAST('2002-08-22' AS DATE) AND (CAST('2002-08-22' AS DATE) + INTERVAL '14' day)
    GROUP BY
        web_site_id
)
SELECT 
    channel,
    id,
    SUM(sales) AS sales,
    SUM(returns1) AS returns1,
    SUM(profit) AS profit
FROM (
    SELECT 'store channel' AS channel,
           'store' || s_store_id AS id,
           sales,
           returns1,
           (profit - profit_loss) AS profit
    FROM ssr
    UNION ALL
    SELECT 'catalog channel' AS channel,
           'catalog_page' || cp_catalog_page_id AS id,
           sales,
           returns1,
           (profit - profit_loss) AS profit
    FROM csr
    UNION ALL
    SELECT 'web channel' AS channel,
           'web_site' || web_site_id AS id,
           sales,
           returns1,
           (profit - profit_loss) AS profit
    FROM wsr
) AS x
GROUP BY
    channel,
    id
ORDER BY
    channel,
    id
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 1 and QUERY_ID < 7);

-- query 6
SELECT
    a.ca_state AS state,
    COUNT(*) AS cnt
FROM
    customer_address a
    INNER JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk
    INNER JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
    INNER JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
    INNER JOIN item i ON s.ss_item_sk = i.i_item_sk
WHERE
    d.d_month_seq = (
        SELECT DISTINCT d_month_seq
        FROM date_dim
        WHERE d_year = 1998
        AND d_moy = 7
    )
    AND i.i_current_price > 1.2 * (
        SELECT AVG(j.i_current_price)
        FROM item j
        WHERE j.i_category = i.i_category
    )
GROUP BY
    a.ca_state
HAVING
    COUNT(*) >= 10
ORDER BY
    cnt DESC;

-- query 7
SELECT i_item_id, 
               Avg(ss_quantity)    agg1, 
               Avg(ss_list_price)  agg2, 
               Avg(ss_coupon_amt)  agg3, 
               Avg(ss_sales_price) agg4 
FROM   store_sales, 
       customer_demographics, 
       date_dim, 
       item, 
       promotion 
WHERE  ss_sold_date_sk = d_date_sk 
       AND ss_item_sk = i_item_sk 
       AND ss_cdemo_sk = cd_demo_sk 
       AND ss_promo_sk = p_promo_sk 
       AND cd_gender = 'F' 
       AND cd_marital_status = 'W' 
       AND cd_education_status = '2 yr Degree' 
       AND ( p_channel_email = 'N' 
              OR p_channel_event = 'N' ) 
       AND d_year = 1998 
GROUP  BY i_item_id 
ORDER  BY i_item_id
LIMIT 100;

-- start query 8 in stream 0 using template query8.tpl 
SELECT s_store_name, 
               Sum(ss_net_profit) 
FROM   store_sales, 
       date_dim, 
       store, 
       (SELECT ca_zip 
        FROM   (SELECT Substr(ca_zip, 1, 5) ca_zip 
                FROM   customer_address 
                WHERE  Substr(ca_zip, 1, 5) IN ( '67436', '26121', '38443', 
                                                 '63157', 
                                                 '68856', '19485', '86425', 
                                                 '26741', 
                                                 '70991', '60899', '63573', 
                                                 '47556', 
                                                 '56193', '93314', '87827', 
                                                 '62017', 
                                                 '85067', '95390', '48091', 
                                                 '10261', 
                                                 '81845', '41790', '42853', 
                                                 '24675', 
                                                 '12840', '60065', '84430', 
                                                 '57451', 
                                                 '24021', '91735', '75335', 
                                                 '71935', 
                                                 '34482', '56943', '70695', 
                                                 '52147', 
                                                 '56251', '28411', '86653', 
                                                 '23005', 
                                                 '22478', '29031', '34398', 
                                                 '15365', 
                                                 '42460', '33337', '59433', 
                                                 '73943', 
                                                 '72477', '74081', '74430', 
                                                 '64605', 
                                                 '39006', '11226', '49057', 
                                                 '97308', 
                                                 '42663', '18187', '19768', 
                                                 '43454', 
                                                 '32147', '76637', '51975', 
                                                 '11181', 
                                                 '45630', '33129', '45995', 
                                                 '64386', 
                                                 '55522', '26697', '20963', 
                                                 '35154', 
                                                 '64587', '49752', '66386', 
                                                 '30586', 
                                                 '59286', '13177', '66646', 
                                                 '84195', 
                                                 '74316', '36853', '32927', 
                                                 '12469', 
                                                 '11904', '36269', '17724', 
                                                 '55346', 
                                                 '12595', '53988', '65439', 
                                                 '28015', 
                                                 '63268', '73590', '29216', 
                                                 '82575', 
                                                 '69267', '13805', '91678', 
                                                 '79460', 
                                                 '94152', '14961', '15419', 
                                                 '48277', 
                                                 '62588', '55493', '28360', 
                                                 '14152', 
                                                 '55225', '18007', '53705', 
                                                 '56573', 
                                                 '80245', '71769', '57348', 
                                                 '36845', 
                                                 '13039', '17270', '22363', 
                                                 '83474', 
                                                 '25294', '43269', '77666', 
                                                 '15488', 
                                                 '99146', '64441', '43338', 
                                                 '38736', 
                                                 '62754', '48556', '86057', 
                                                 '23090', 
                                                 '38114', '66061', '18910', 
                                                 '84385', 
                                                 '23600', '19975', '27883', 
                                                 '65719', 
                                                 '19933', '32085', '49731', 
                                                 '40473', 
                                                 '27190', '46192', '23949', 
                                                 '44738', 
                                                 '12436', '64794', '68741', 
                                                 '15333', 
                                                 '24282', '49085', '31844', 
                                                 '71156', 
                                                 '48441', '17100', '98207', 
                                                 '44982', 
                                                 '20277', '71496', '96299', 
                                                 '37583', 
                                                 '22206', '89174', '30589', 
                                                 '61924', 
                                                 '53079', '10976', '13104', 
                                                 '42794', 
                                                 '54772', '15809', '56434', 
                                                 '39975', 
                                                 '13874', '30753', '77598', 
                                                 '78229', 
                                                 '59478', '12345', '55547', 
                                                 '57422', 
                                                 '42600', '79444', '29074', 
                                                 '29752', 
                                                 '21676', '32096', '43044', 
                                                 '39383', 
                                                 '37296', '36295', '63077', 
                                                 '16572', 
                                                 '31275', '18701', '40197', 
                                                 '48242', 
                                                 '27219', '49865', '84175', 
                                                 '30446', 
                                                 '25165', '13807', '72142', 
                                                 '70499', 
                                                 '70464', '71429', '18111', 
                                                 '70857', 
                                                 '29545', '36425', '52706', 
                                                 '36194', 
                                                 '42963', '75068', '47921', 
                                                 '74763', 
                                                 '90990', '89456', '62073', 
                                                 '88397', 
                                                 '73963', '75885', '62657', 
                                                 '12530', 
                                                 '81146', '57434', '25099', 
                                                 '41429', 
                                                 '98441', '48713', '52552', 
                                                 '31667', 
                                                 '14072', '13903', '44709', 
                                                 '85429', 
                                                 '58017', '38295', '44875', 
                                                 '73541', 
                                                 '30091', '12707', '23762', 
                                                 '62258', 
                                                 '33247', '78722', '77431', 
                                                 '14510', 
                                                 '35656', '72428', '92082', 
                                                 '35267', 
                                                 '43759', '24354', '90952', 
                                                 '11512', 
                                                 '21242', '22579', '56114', 
                                                 '32339', 
                                                 '52282', '41791', '24484', 
                                                 '95020', 
                                                 '28408', '99710', '11899', 
                                                 '43344', 
                                                 '72915', '27644', '62708', 
                                                 '74479', 
                                                 '17177', '32619', '12351', 
                                                 '91339', 
                                                 '31169', '57081', '53522', 
                                                 '16712', 
                                                 '34419', '71779', '44187', 
                                                 '46206', 
                                                 '96099', '61910', '53664', 
                                                 '12295', 
                                                 '31837', '33096', '10813', 
                                                 '63048', 
                                                 '31732', '79118', '73084', 
                                                 '72783', 
                                                 '84952', '46965', '77956', 
                                                 '39815', 
                                                 '32311', '75329', '48156', 
                                                 '30826', 
                                                 '49661', '13736', '92076', 
                                                 '74865', 
                                                 '88149', '92397', '52777', 
                                                 '68453', 
                                                 '32012', '21222', '52721', 
                                                 '24626', 
                                                 '18210', '42177', '91791', 
                                                 '75251', 
                                                 '82075', '44372', '45542', 
                                                 '20609', 
                                                 '60115', '17362', '22750', 
                                                 '90434', 
                                                 '31852', '54071', '33762', 
                                                 '14705', 
                                                 '40718', '56433', '30996', 
                                                 '40657', 
                                                 '49056', '23585', '66455', 
                                                 '41021', 
                                                 '74736', '72151', '37007', 
                                                 '21729', 
                                                 '60177', '84558', '59027', 
                                                 '93855', 
                                                 '60022', '86443', '19541', 
                                                 '86886', 
                                                 '30532', '39062', '48532', 
                                                 '34713', 
                                                 '52077', '22564', '64638', 
                                                 '15273', 
                                                 '31677', '36138', '62367', 
                                                 '60261', 
                                                 '80213', '42818', '25113', 
                                                 '72378', 
                                                 '69802', '69096', '55443', 
                                                 '28820', 
                                                 '13848', '78258', '37490', 
                                                 '30556', 
                                                 '77380', '28447', '44550', 
                                                 '26791', 
                                                 '70609', '82182', '33306', 
                                                 '43224', 
                                                 '22322', '86959', '68519', 
                                                 '14308', 
                                                 '46501', '81131', '34056', 
                                                 '61991', 
                                                 '19896', '87804', '65774', 
                                                 '92564' ) 
                INTERSECT 
                SELECT ca_zip 
                FROM   (SELECT Substr(ca_zip, 1, 5) ca_zip, 
                               Count(*)             cnt 
                        FROM   customer_address, 
                               customer 
                        WHERE  ca_address_sk = c_current_addr_sk 
                               AND c_preferred_cust_flag = 'Y' 
                        GROUP  BY ca_zip 
                        HAVING Count(*) > 10)A1)A2) V1 
WHERE  ss_store_sk = s_store_sk 
       AND ss_sold_date_sk = d_date_sk 
       AND d_qoy = 2 
       AND d_year = 2000 
       AND ( Substr(s_zip, 1, 2) = Substr(V1.ca_zip, 1, 2) ) 
GROUP  BY s_store_name 
ORDER  BY s_store_name
LIMIT 100;


-- start query 9 in stream 0 using template query9.tpl 
SELECT CASE 
         WHEN (SELECT Count(*) 
               FROM   store_sales 
               WHERE  ss_quantity BETWEEN 1 AND 20) > 3672 THEN 
         (SELECT Avg(ss_ext_list_price) 
          FROM   store_sales 
          WHERE 
         ss_quantity BETWEEN 1 AND 20) 
         ELSE (SELECT Avg(ss_net_profit) 
               FROM   store_sales 
               WHERE  ss_quantity BETWEEN 1 AND 20) 
       END bucket1, 
       CASE 
         WHEN (SELECT Count(*) 
               FROM   store_sales 
               WHERE  ss_quantity BETWEEN 21 AND 40) > 3392 THEN 
         (SELECT Avg(ss_ext_list_price) 
          FROM   store_sales 
          WHERE 
         ss_quantity BETWEEN 21 AND 40) 
         ELSE (SELECT Avg(ss_net_profit) 
               FROM   store_sales 
               WHERE  ss_quantity BETWEEN 21 AND 40) 
       END bucket2, 
       CASE 
         WHEN (SELECT Count(*) 
               FROM   store_sales 
               WHERE  ss_quantity BETWEEN 41 AND 60) > 32784 THEN 
         (SELECT Avg(ss_ext_list_price) 
          FROM   store_sales 
          WHERE 
         ss_quantity BETWEEN 41 AND 60) 
         ELSE (SELECT Avg(ss_net_profit) 
               FROM   store_sales 
               WHERE  ss_quantity BETWEEN 41 AND 60) 
       END bucket3, 
       CASE 
         WHEN (SELECT Count(*) 
               FROM   store_sales 
               WHERE  ss_quantity BETWEEN 61 AND 80) > 26032 THEN 
         (SELECT Avg(ss_ext_list_price) 
          FROM   store_sales 
          WHERE 
         ss_quantity BETWEEN 61 AND 80) 
         ELSE (SELECT Avg(ss_net_profit) 
               FROM   store_sales 
               WHERE  ss_quantity BETWEEN 61 AND 80) 
       END bucket4, 
       CASE 
         WHEN (SELECT Count(*) 
               FROM   store_sales 
               WHERE  ss_quantity BETWEEN 81 AND 100) > 23982 THEN 
         (SELECT Avg(ss_ext_list_price) 
          FROM   store_sales 
          WHERE 
         ss_quantity BETWEEN 81 AND 100) 
         ELSE (SELECT Avg(ss_net_profit) 
               FROM   store_sales 
               WHERE  ss_quantity BETWEEN 81 AND 100) 
       END bucket5 
FROM   reason 
WHERE  r_reason_sk = 1;


-- start query 10 in stream 0 using template query10.tpl 
SELECT cd_gender, 
               cd_marital_status, 
               cd_education_status, 
               Count(*) cnt1, 
               cd_purchase_estimate, 
               Count(*) cnt2, 
               cd_credit_rating, 
               Count(*) cnt3, 
               cd_dep_count, 
               Count(*) cnt4, 
               cd_dep_employed_count, 
               Count(*) cnt5, 
               cd_dep_college_count, 
               Count(*) cnt6 
FROM   customer c, 
       customer_address ca, 
       customer_demographics 
WHERE  c.c_current_addr_sk = ca.ca_address_sk 
       AND ca_county IN ( 'Lycoming County', 'Sheridan County', 
                          'Kandiyohi County', 
                          'Pike County', 
                                           'Greene County' ) 
       AND cd_demo_sk = c.c_current_cdemo_sk 
       AND EXISTS (SELECT * 
                   FROM   store_sales, 
                          date_dim 
                   WHERE  c.c_customer_sk = ss_customer_sk 
                          AND ss_sold_date_sk = d_date_sk 
                          AND d_year = 2002 
                          AND d_moy BETWEEN 4 AND 4 + 3) 
       AND ( EXISTS (SELECT * 
                     FROM   web_sales, 
                            date_dim 
                     WHERE  c.c_customer_sk = ws_bill_customer_sk 
                            AND ws_sold_date_sk = d_date_sk 
                            AND d_year = 2002 
                            AND d_moy BETWEEN 4 AND 4 + 3) 
              OR EXISTS (SELECT * 
                         FROM   catalog_sales, 
                                date_dim 
                         WHERE  c.c_customer_sk = cs_ship_customer_sk 
                                AND cs_sold_date_sk = d_date_sk 
                                AND d_year = 2002 
                                AND d_moy BETWEEN 4 AND 4 + 3) ) 
GROUP  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating, 
          cd_dep_count, 
          cd_dep_employed_count, 
          cd_dep_college_count 
ORDER  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating, 
          cd_dep_count, 
          cd_dep_employed_count, 
          cd_dep_college_count
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 7 and QUERY_ID < 13);
-- start query 11 in stream 0 using template query11.tpl 
WITH year_total 
     AS (SELECT c_customer_id                                customer_id, 
                c_first_name                                 customer_first_name 
                , 
                c_last_name 
                customer_last_name, 
                c_preferred_cust_flag 
                   customer_preferred_cust_flag 
                    , 
                c_birth_country 
                    customer_birth_country, 
                c_login                                      customer_login, 
                c_email_address 
                customer_email_address, 
                d_year                                       dyear, 
                Sum(ss_ext_list_price - ss_ext_discount_amt) year_total, 
                's'                                          sale_type 
         FROM   customer, 
                store_sales, 
                date_dim 
         WHERE  c_customer_sk = ss_customer_sk 
                AND ss_sold_date_sk = d_date_sk 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   c_preferred_cust_flag, 
                   c_birth_country, 
                   c_login, 
                   c_email_address, 
                   d_year 
         UNION ALL 
         SELECT c_customer_id                                customer_id, 
                c_first_name                                 customer_first_name 
                , 
                c_last_name 
                customer_last_name, 
                c_preferred_cust_flag 
                customer_preferred_cust_flag 
                , 
                c_birth_country 
                customer_birth_country, 
                c_login                                      customer_login, 
                c_email_address 
                customer_email_address, 
                d_year                                       dyear, 
                Sum(ws_ext_list_price - ws_ext_discount_amt) year_total, 
                'w'                                          sale_type 
         FROM   customer, 
                web_sales, 
                date_dim 
         WHERE  c_customer_sk = ws_bill_customer_sk 
                AND ws_sold_date_sk = d_date_sk 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   c_preferred_cust_flag, 
                   c_birth_country, 
                   c_login, 
                   c_email_address, 
                   d_year) 
SELECT t_s_secyear.customer_id, 
               t_s_secyear.customer_first_name, 
               t_s_secyear.customer_last_name, 
               t_s_secyear.customer_birth_country 
FROM   year_total t_s_firstyear, 
       year_total t_s_secyear, 
       year_total t_w_firstyear, 
       year_total t_w_secyear 
WHERE  t_s_secyear.customer_id = t_s_firstyear.customer_id 
       AND t_s_firstyear.customer_id = t_w_secyear.customer_id 
       AND t_s_firstyear.customer_id = t_w_firstyear.customer_id 
       AND t_s_firstyear.sale_type = 's' 
       AND t_w_firstyear.sale_type = 'w' 
       AND t_s_secyear.sale_type = 's' 
       AND t_w_secyear.sale_type = 'w' 
       AND t_s_firstyear.dyear = 2001 
       AND t_s_secyear.dyear = 2001 + 1 
       AND t_w_firstyear.dyear = 2001 
       AND t_w_secyear.dyear = 2001 + 1 
       AND t_s_firstyear.year_total > 0 
       AND t_w_firstyear.year_total > 0 
       AND CASE 
             WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / 
                                                    t_w_firstyear.year_total 
             ELSE 0.0 
           END > CASE 
                   WHEN t_s_firstyear.year_total > 0 THEN 
                   t_s_secyear.year_total / 
                   t_s_firstyear.year_total 
                   ELSE 0.0 
                 END 
ORDER  BY t_s_secyear.customer_id, 
          t_s_secyear.customer_first_name, 
          t_s_secyear.customer_last_name, 
          t_s_secyear.customer_birth_country
LIMIT 100;


-- start query 12 in stream 0 using template query12.tpl 
SELECT
         i_item_id , 
         i_item_desc , 
         i_category , 
         i_class , 
         i_current_price , 
         Sum(ws_ext_sales_price)                                                              AS itemrevenue ,
         Sum(ws_ext_sales_price)*100/Sum(Sum(ws_ext_sales_price)) OVER (partition BY i_class) AS revenueratio
FROM     web_sales , 
         item , 
         date_dim 
WHERE    ws_item_sk = i_item_sk 
AND      i_category IN ('Home', 
                        'Men', 
                        'Women') 
AND      ws_sold_date_sk = d_date_sk 
AND      d_date BETWEEN Cast('2000-05-11' AS DATE) AND      ( 
                  Cast('2000-05-11' AS DATE) + INTERVAL '30' day) 
GROUP BY i_item_id , 
         i_item_desc , 
         i_category , 
         i_class , 
         i_current_price 
ORDER BY i_category , 
         i_class , 
         i_item_id , 
         i_item_desc , 
         revenueratio 
LIMIT 100;


-- start query 13 in stream 0 using template query13.tpl 
SELECT Avg(ss_quantity), 
       Avg(ss_ext_sales_price), 
       Avg(ss_ext_wholesale_cost), 
       Sum(ss_ext_wholesale_cost) 
FROM   store_sales, 
       store, 
       customer_demographics, 
       household_demographics, 
       customer_address, 
       date_dim 
WHERE  s_store_sk = ss_store_sk 
       AND ss_sold_date_sk = d_date_sk 
       AND d_year = 2001 
       AND ( ( ss_hdemo_sk = hd_demo_sk 
               AND cd_demo_sk = ss_cdemo_sk 
               AND cd_marital_status = 'U' 
               AND cd_education_status = 'Advanced Degree' 
               AND ss_sales_price BETWEEN 100.00 AND 150.00 
               AND hd_dep_count = 3 ) 
              OR ( ss_hdemo_sk = hd_demo_sk 
                   AND cd_demo_sk = ss_cdemo_sk 
                   AND cd_marital_status = 'M' 
                   AND cd_education_status = 'Primary' 
                   AND ss_sales_price BETWEEN 50.00 AND 100.00 
                   AND hd_dep_count = 1 ) 
              OR ( ss_hdemo_sk = hd_demo_sk 
                   AND cd_demo_sk = ss_cdemo_sk 
                   AND cd_marital_status = 'D' 
                   AND cd_education_status = 'Secondary' 
                   AND ss_sales_price BETWEEN 150.00 AND 200.00 
                   AND hd_dep_count = 1 ) ) 
       AND ( ( ss_addr_sk = ca_address_sk 
               AND ca_country = 'United States' 
               AND ca_state IN ( 'AZ', 'NE', 'IA' ) 
               AND ss_net_profit BETWEEN 100 AND 200 ) 
              OR ( ss_addr_sk = ca_address_sk 
                   AND ca_country = 'United States' 
                   AND ca_state IN ( 'MS', 'CA', 'NV' ) 
                   AND ss_net_profit BETWEEN 150 AND 300 ) 
              OR ( ss_addr_sk = ca_address_sk 
                   AND ca_country = 'United States' 
                   AND ca_state IN ( 'GA', 'TX', 'NJ' ) 
                   AND ss_net_profit BETWEEN 50 AND 250 ) );


-- start query 14 in stream 0 using template query14.tpl 
WITH cross_items 
     AS (SELECT i_item_sk ss_item_sk 
         FROM   item, 
                (SELECT iss.i_brand_id    brand_id, 
                        iss.i_class_id    class_id, 
                        iss.i_category_id category_id 
                 FROM   store_sales, 
                        item iss, 
                        date_dim d1 
                 WHERE  ss_item_sk = iss.i_item_sk 
                        AND ss_sold_date_sk = d1.d_date_sk 
                        AND d1.d_year BETWEEN 1999 AND 1999 + 2 
                 INTERSECT 
                 SELECT ics.i_brand_id, 
                        ics.i_class_id, 
                        ics.i_category_id 
                 FROM   catalog_sales, 
                        item ics, 
                        date_dim d2 
                 WHERE  cs_item_sk = ics.i_item_sk 
                        AND cs_sold_date_sk = d2.d_date_sk 
                        AND d2.d_year BETWEEN 1999 AND 1999 + 2 
                 INTERSECT 
                 SELECT iws.i_brand_id, 
                        iws.i_class_id, 
                        iws.i_category_id 
                 FROM   web_sales, 
                        item iws, 
                        date_dim d3 
                 WHERE  ws_item_sk = iws.i_item_sk 
                        AND ws_sold_date_sk = d3.d_date_sk 
                        AND d3.d_year BETWEEN 1999 AND 1999 + 2) x 
         WHERE  i_brand_id = brand_id 
                AND i_class_id = class_id 
                AND i_category_id = category_id), 
     avg_sales 
     AS (SELECT Avg(quantity * list_price) average_sales 
         FROM   (SELECT ss_quantity   quantity, 
                        ss_list_price list_price 
                 FROM   store_sales, 
                        date_dim 
                 WHERE  ss_sold_date_sk = d_date_sk 
                        AND d_year BETWEEN 1999 AND 1999 + 2 
                 UNION ALL 
                 SELECT cs_quantity   quantity, 
                        cs_list_price list_price 
                 FROM   catalog_sales, 
                        date_dim 
                 WHERE  cs_sold_date_sk = d_date_sk 
                        AND d_year BETWEEN 1999 AND 1999 + 2 
                 UNION ALL 
                 SELECT ws_quantity   quantity, 
                        ws_list_price list_price 
                 FROM   web_sales, 
                        date_dim 
                 WHERE  ws_sold_date_sk = d_date_sk 
                        AND d_year BETWEEN 1999 AND 1999 + 2) x) 
SELECT  * 
FROM   (SELECT 'store'                          channel, 
               i_brand_id, 
               i_class_id, 
               i_category_id, 
               Sum(ss_quantity * ss_list_price) sales, 
               Count(*)                         number_sales 
        FROM   store_sales, 
               item, 
               date_dim 
        WHERE  ss_item_sk IN (SELECT ss_item_sk 
                              FROM   cross_items) 
               AND ss_item_sk = i_item_sk 
               AND ss_sold_date_sk = d_date_sk 
               AND d_week_seq = (SELECT d_week_seq 
                                 FROM   date_dim 
                                 WHERE  d_year = 1999 + 1 
                                        AND d_moy = 12 
                                        AND d_dom = 25) 
        GROUP  BY i_brand_id, 
                  i_class_id, 
                  i_category_id 
        HAVING Sum(ss_quantity * ss_list_price) > (SELECT average_sales 
                                                   FROM   avg_sales)) this_year, 
       (SELECT 'store'                          channel, 
               i_brand_id, 
               i_class_id, 
               i_category_id, 
               Sum(ss_quantity * ss_list_price) sales, 
               Count(*)                         number_sales 
        FROM   store_sales, 
               item, 
               date_dim 
        WHERE  ss_item_sk IN (SELECT ss_item_sk 
                              FROM   cross_items) 
               AND ss_item_sk = i_item_sk 
               AND ss_sold_date_sk = d_date_sk 
               AND d_week_seq = (SELECT d_week_seq 
                                 FROM   date_dim 
                                 WHERE  d_year = 1999 
                                        AND d_moy = 12 
                                        AND d_dom = 25) 
        GROUP  BY i_brand_id, 
                  i_class_id, 
                  i_category_id 
        HAVING Sum(ss_quantity * ss_list_price) > (SELECT average_sales 
                                                   FROM   avg_sales)) last_year 
WHERE  this_year.i_brand_id = last_year.i_brand_id 
       AND this_year.i_class_id = last_year.i_class_id 
       AND this_year.i_category_id = last_year.i_category_id 
ORDER  BY this_year.channel, 
          this_year.i_brand_id, 
          this_year.i_class_id, 
          this_year.i_category_id
LIMIT 100;


-- start query 15 in stream 0 using template query15.tpl 
SELECT ca_zip, 
               Sum(cs_sales_price) 
FROM   catalog_sales, 
       customer, 
       customer_address, 
       date_dim 
WHERE  cs_bill_customer_sk = c_customer_sk 
       AND c_current_addr_sk = ca_address_sk 
       AND ( Substr(ca_zip, 1, 5) IN ( '85669', '86197', '88274', '83405', 
                                       '86475', '85392', '85460', '80348', 
                                       '81792' ) 
              OR ca_state IN ( 'CA', 'WA', 'GA' ) 
              OR cs_sales_price > 500 ) 
       AND cs_sold_date_sk = d_date_sk 
       AND d_qoy = 1 
       AND d_year = 1998 
GROUP  BY ca_zip 
ORDER  BY ca_zip
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 13 and QUERY_ID < 19);

-- start query 16 in stream 0 using template query16.tpl
SELECT
         Count(DISTINCT cs_order_number) AS `order count` ,
         Sum(cs_ext_ship_cost)           AS `total shipping cost` ,
         Sum(cs_net_profit)              AS `total net profit`
FROM     catalog_sales cs1 ,
         date_dim ,
         customer_address ,
         call_center
WHERE    d_date BETWEEN '2002-3-01' AND      (
                  Cast('2002-3-01' AS DATE) + INTERVAL '60' day)
AND      cs1.cs_ship_date_sk = d_date_sk
AND      cs1.cs_ship_addr_sk = ca_address_sk
AND      ca_state = 'IA'
AND      cs1.cs_call_center_sk = cc_call_center_sk
AND      cc_county IN ('Williamson County',
                       'Williamson County',
                       'Williamson County',
                       'Williamson County',
                       'Williamson County' )
AND      EXISTS
         (
                SELECT *
                FROM   catalog_sales cs2
                WHERE  cs1.cs_order_number = cs2.cs_order_number
                AND    cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
AND      NOT EXISTS
         (
                SELECT *
                FROM   catalog_returns cr1
                WHERE  cs1.cs_order_number = cr1.cr_order_number)
ORDER BY count(DISTINCT cs_order_number)
LIMIT 100;


-- start query 17 in stream 0 using template query17.tpl 
SELECT i_item_id, 
               i_item_desc, 
               s_state, 
               Count(ss_quantity)                                        AS 
               store_sales_quantitycount, 
               Avg(ss_quantity)                                          AS 
               store_sales_quantityave, 
               Stddev_samp(ss_quantity)                                  AS 
               store_sales_quantitystdev, 
               Stddev_samp(ss_quantity) / Avg(ss_quantity)               AS 
               store_sales_quantitycov, 
               Count(sr_return_quantity)                                 AS 
               store_returns_quantitycount, 
               Avg(sr_return_quantity)                                   AS 
               store_returns_quantityave, 
               Stddev_samp(sr_return_quantity)                           AS 
               store_returns_quantitystdev, 
               Stddev_samp(sr_return_quantity) / Avg(sr_return_quantity) AS 
               store_returns_quantitycov, 
               Count(cs_quantity)                                        AS 
               catalog_sales_quantitycount, 
               Avg(cs_quantity)                                          AS 
               catalog_sales_quantityave, 
               Stddev_samp(cs_quantity) / Avg(cs_quantity)               AS 
               catalog_sales_quantitystdev, 
               Stddev_samp(cs_quantity) / Avg(cs_quantity)               AS 
               catalog_sales_quantitycov 
FROM   store_sales, 
       store_returns, 
       catalog_sales, 
       date_dim d1, 
       date_dim d2, 
       date_dim d3, 
       store, 
       item 
WHERE  d1.d_quarter_name = '1999Q1' 
       AND d1.d_date_sk = ss_sold_date_sk 
       AND i_item_sk = ss_item_sk 
       AND s_store_sk = ss_store_sk 
       AND ss_customer_sk = sr_customer_sk 
       AND ss_item_sk = sr_item_sk 
       AND ss_ticket_number = sr_ticket_number 
       AND sr_returned_date_sk = d2.d_date_sk 
       AND d2.d_quarter_name IN ( '1999Q1', '1999Q2', '1999Q3' ) 
       AND sr_customer_sk = cs_bill_customer_sk 
       AND sr_item_sk = cs_item_sk 
       AND cs_sold_date_sk = d3.d_date_sk 
       AND d3.d_quarter_name IN ( '1999Q1', '1999Q2', '1999Q3' ) 
GROUP  BY i_item_id, 
          i_item_desc, 
          s_state 
ORDER  BY i_item_id, 
          i_item_desc, 
          s_state
LIMIT 100;

-- start query 18 in stream 0 using template query18.tpl 
SELECT i_item_id,
       ca_country,
       ca_state,
       ca_county,
       AVG(CAST(cs_quantity AS DECIMAL(12, 2)))      AS agg1,
       AVG(CAST(cs_list_price AS DECIMAL(12, 2)))    AS agg2,
       AVG(CAST(cs_coupon_amt AS DECIMAL(12, 2)))    AS agg3,
       AVG(CAST(cs_sales_price AS DECIMAL(12, 2)))   AS agg4,
       AVG(CAST(cs_net_profit AS DECIMAL(12, 2)))    AS agg5,
       AVG(CAST(c_birth_year AS DECIMAL(12, 2)))     AS agg6,
       AVG(CAST(cd1.cd_dep_count AS DECIMAL(12, 2))) AS agg7
FROM catalog_sales
JOIN customer_demographics cd1 ON cs_bill_cdemo_sk = cd1.cd_demo_sk
JOIN customer ON cs_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
JOIN item ON cs_item_sk = i_item_sk
JOIN customer_demographics cd2 ON c_current_cdemo_sk = cd2.cd_demo_sk
WHERE cd1.cd_gender = 'F'
  AND cd1.cd_education_status = 'Secondary'
  AND c_birth_month IN (8, 4, 2, 5, 11, 9)
  AND d_year = 2001
  AND ca_state IN ('KS', 'IA', 'AL', 'UT', 'VA', 'NC', 'TX')
GROUP BY i_item_id, ca_country, ca_state, ca_county WITH ROLLUP
ORDER BY ca_country, ca_state, ca_county, i_item_id
LIMIT 100;


-- start query 19 in stream 0 using template query19.tpl 
SELECT i_brand_id              brand_id, 
               i_brand                 brand, 
               i_manufact_id, 
               i_manufact, 
               Sum(ss_ext_sales_price) ext_price 
FROM   date_dim, 
       store_sales, 
       item, 
       customer, 
       customer_address, 
       store 
WHERE  d_date_sk = ss_sold_date_sk 
       AND ss_item_sk = i_item_sk 
       AND i_manager_id = 38 
       AND d_moy = 12 
       AND d_year = 1998 
       AND ss_customer_sk = c_customer_sk 
       AND c_current_addr_sk = ca_address_sk 
       AND Substr(ca_zip, 1, 5) <> Substr(s_zip, 1, 5) 
       AND ss_store_sk = s_store_sk 
GROUP  BY i_brand, 
          i_brand_id, 
          i_manufact_id, 
          i_manufact 
ORDER  BY ext_price DESC, 
          i_brand, 
          i_brand_id, 
          i_manufact_id, 
          i_manufact
LIMIT 100;

-- start query 20 in stream 0 using template query20.tpl 
SELECT 
         i_item_id , 
         i_item_desc , 
         i_category , 
         i_class , 
         i_current_price , 
         Sum(cs_ext_sales_price)                                                              AS itemrevenue ,
         Sum(cs_ext_sales_price)*100/Sum(Sum(cs_ext_sales_price)) OVER (partition BY i_class) AS revenueratio
FROM     catalog_sales , 
         item , 
         date_dim 
WHERE    cs_item_sk = i_item_sk 
AND      i_category IN ('Children', 
                        'Women', 
                        'Electronics') 
AND      cs_sold_date_sk = d_date_sk 
AND      d_date BETWEEN Cast('2001-02-03' AS DATE) AND      ( 
                  Cast('2001-02-03' AS DATE) + INTERVAL '30' day) 
GROUP BY i_item_id , 
         i_item_desc , 
         i_category , 
         i_class , 
         i_current_price 
ORDER BY i_category , 
         i_class , 
         i_item_id , 
         i_item_desc , 
         revenueratio 
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 19 and QUERY_ID < 25);

-- 21
SELECT
    *
FROM
    (
        SELECT
            w_warehouse_name,
            i_item_id,
            SUM(
                CASE
                    WHEN (CAST(d_date AS DATE) < CAST('2000-05-13' AS DATE)) THEN inv_quantity_on_hand
                    ELSE 0
                END
            ) AS inv_before,
            SUM(
                CASE
                    WHEN (CAST(d_date AS DATE) >= CAST('2000-05-13' AS DATE)) THEN inv_quantity_on_hand
                    ELSE 0
                END
            ) AS inv_after
        FROM
            inventory
            JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
            JOIN item ON i_item_sk = inv_item_sk
            JOIN date_dim ON inv_date_sk = d_date_sk
        WHERE
            i_current_price BETWEEN 0.99 AND 1.49
            AND d_date BETWEEN (CAST('2000-05-13' AS DATE) - INTERVAL 30 DAY) AND (CAST('2000-05-13' AS DATE) + INTERVAL 30 DAY)
        GROUP BY
            w_warehouse_name,
            i_item_id
    ) AS x
WHERE
    (
        CASE
            WHEN inv_before > 0 THEN inv_after / inv_before
            ELSE NULL
        END
    ) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0
ORDER BY
    w_warehouse_name,
    i_item_id
LIMIT 100;

-- 22
SELECT
    i_product_name,
    i_brand,
    i_class,
    i_category,
    AVG(inv_quantity_on_hand) AS qoh
FROM
    inventory
    JOIN date_dim ON inv_date_sk = d_date_sk
    JOIN item ON inv_item_sk = i_item_sk
    JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
WHERE
    d_month_seq BETWEEN 1205 AND 1216 -- 1205 + 11
GROUP BY
    i_product_name,
    i_brand,
    i_class,
    i_category WITH ROLLUP
ORDER BY
    qoh,
    i_product_name,
    i_brand,
    i_class,
    i_category
LIMIT 100;

-- 23
WITH frequent_ss_items AS (
    SELECT
        SUBSTRING(i_item_desc, 1, 30) AS itemdesc,
        i_item_sk AS item_sk,
        d_date AS solddate,
        COUNT(*) AS cnt
    FROM
        store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        JOIN item ON ss_item_sk = i_item_sk
    WHERE
        d_year IN (1998, 1998 + 1, 1998 + 2, 1998 + 3)
    GROUP BY
        SUBSTRING(i_item_desc, 1, 30),
        i_item_sk,
        d_date
    HAVING
        COUNT(*) > 4
),
max_store_sales AS (
    SELECT
        MAX(csales) AS tpcds_cmax
    FROM (
        SELECT
            c_customer_sk,
            SUM(ss_quantity * ss_sales_price) AS csales
        FROM
            store_sales
            JOIN date_dim ON ss_sold_date_sk = d_date_sk
            JOIN customer ON ss_customer_sk = c_customer_sk
        WHERE
            d_year IN (1998, 1998 + 1, 1998 + 2, 1998 + 3)
        GROUP BY
            c_customer_sk
    ) AS max_sales
),
best_ss_customer AS (
    SELECT
        c_customer_sk,
        SUM(ss_quantity * ss_sales_price) AS ssales
    FROM
        store_sales
        JOIN customer ON ss_customer_sk = c_customer_sk
    GROUP BY
        c_customer_sk
    HAVING
        SUM(ss_quantity * ss_sales_price) > (95 / 100.0) * (SELECT tpcds_cmax FROM max_store_sales)
)
SELECT
    c_last_name,
    c_first_name,
    sales
FROM
    (
        SELECT
            c_last_name,
            c_first_name,
            SUM(cs_quantity * cs_list_price) AS sales
        FROM
            catalog_sales
            JOIN date_dim ON cs_sold_date_sk = d_date_sk
            JOIN customer ON cs_bill_customer_sk = c_customer_sk
        WHERE
            d_year = 1998
            AND d_moy = 6
            AND cs_item_sk IN (SELECT item_sk FROM frequent_ss_items)
            AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
        GROUP BY
            c_last_name,
            c_first_name
        UNION ALL
        SELECT
            c_last_name,
            c_first_name,
            SUM(ws_quantity * ws_list_price) AS sales
        FROM
            web_sales
            JOIN date_dim ON ws_sold_date_sk = d_date_sk
            JOIN customer ON ws_bill_customer_sk = c_customer_sk
        WHERE
            d_year = 1998
            AND d_moy = 6
            AND ws_item_sk IN (SELECT item_sk FROM frequent_ss_items)
            AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
        GROUP BY
            c_last_name,
            c_first_name
    ) AS sales_summary
ORDER BY
    c_last_name,
    c_first_name,
    sales
LIMIT 100;


-- start query 24 in stream 0 using template query24.tpl 
WITH ssales 
     AS (SELECT c_last_name, 
                c_first_name, 
                s_store_name, 
                ca_state, 
                s_state, 
                i_color, 
                i_current_price, 
                i_manager_id, 
                i_units, 
                i_size, 
                Sum(ss_net_profit) netpaid 
         FROM   store_sales, 
                store_returns, 
                store, 
                item, 
                customer, 
                customer_address 
         WHERE  ss_ticket_number = sr_ticket_number 
                AND ss_item_sk = sr_item_sk 
                AND ss_customer_sk = c_customer_sk 
                AND ss_item_sk = i_item_sk 
                AND ss_store_sk = s_store_sk 
                AND c_birth_country = Upper(ca_country) 
                AND s_zip = ca_zip 
                AND s_market_id = 6 
         GROUP  BY c_last_name, 
                   c_first_name, 
                   s_store_name, 
                   ca_state, 
                   s_state, 
                   i_color, 
                   i_current_price, 
                   i_manager_id, 
                   i_units, 
                   i_size) 
SELECT c_last_name, 
       c_first_name, 
       s_store_name, 
       Sum(netpaid) paid 
FROM   ssales 
WHERE  i_color = 'chartreuse' 
GROUP  BY c_last_name, 
          c_first_name, 
          s_store_name 
HAVING Sum(netpaid) > (SELECT 0.05 * Avg(netpaid) 
                       FROM   ssales);

-- start query 25 in stream 0 using template query25.tpl 
SELECT i_item_id, 
               i_item_desc, 
               s_store_id, 
               s_store_name, 
               Max(ss_net_profit) AS store_sales_profit, 
               Max(sr_net_loss)   AS store_returns_loss, 
               Max(cs_net_profit) AS catalog_sales_profit 
FROM   store_sales, 
       store_returns, 
       catalog_sales, 
       date_dim d1, 
       date_dim d2, 
       date_dim d3, 
       store, 
       item 
WHERE  d1.d_moy = 4 
       AND d1.d_year = 2001 
       AND d1.d_date_sk = ss_sold_date_sk 
       AND i_item_sk = ss_item_sk 
       AND s_store_sk = ss_store_sk 
       AND ss_customer_sk = sr_customer_sk 
       AND ss_item_sk = sr_item_sk 
       AND ss_ticket_number = sr_ticket_number 
       AND sr_returned_date_sk = d2.d_date_sk 
       AND d2.d_moy BETWEEN 4 AND 10 
       AND d2.d_year = 2001 
       AND sr_customer_sk = cs_bill_customer_sk 
       AND sr_item_sk = cs_item_sk 
       AND cs_sold_date_sk = d3.d_date_sk 
       AND d3.d_moy BETWEEN 4 AND 10 
       AND d3.d_year = 2001 
GROUP  BY i_item_id, 
          i_item_desc, 
          s_store_id, 
          s_store_name 
ORDER  BY i_item_id, 
          i_item_desc, 
          s_store_id, 
          s_store_name
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 25 and QUERY_ID < 31);

-- start query 26 in stream 0 using template query26.tpl 
SELECT i_item_id, 
               Avg(cs_quantity)    agg1, 
               Avg(cs_list_price)  agg2, 
               Avg(cs_coupon_amt)  agg3, 
               Avg(cs_sales_price) agg4 
FROM   catalog_sales, 
       customer_demographics, 
       date_dim, 
       item, 
       promotion 
WHERE  cs_sold_date_sk = d_date_sk 
       AND cs_item_sk = i_item_sk 
       AND cs_bill_cdemo_sk = cd_demo_sk 
       AND cs_promo_sk = p_promo_sk 
       AND cd_gender = 'F' 
       AND cd_marital_status = 'W' 
       AND cd_education_status = 'Secondary' 
       AND ( p_channel_email = 'N' 
              OR p_channel_event = 'N' ) 
       AND d_year = 2000 
GROUP  BY i_item_id 
ORDER  BY i_item_id
LIMIT 100;

-- 27
SELECT
    i_item_id,
    s_state,
    GROUPING(s_state) AS g_state,
    AVG(ss_quantity) AS agg1,
    AVG(ss_list_price) AS agg2,
    AVG(ss_coupon_amt) AS agg3,
    AVG(ss_sales_price) AS agg4
FROM
    store_sales
    JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN store ON ss_store_sk = s_store_sk
    JOIN item ON ss_item_sk = i_item_sk
WHERE
    cd_gender = 'M'
    AND cd_marital_status = 'D'
    AND cd_education_status = 'College'
    AND d_year = 2000
    AND s_state IN ('TN')
GROUP BY
    i_item_id, s_state WITH ROLLUP
ORDER BY
    i_item_id, s_state
LIMIT 100;


-- start query 28 in stream 0 using template query28.tpl 
SELECT * 
FROM   (SELECT Avg(ss_list_price)            B1_LP, 
               Count(ss_list_price)          B1_CNT, 
               Count(DISTINCT ss_list_price) B1_CNTD 
        FROM   store_sales 
        WHERE  ss_quantity BETWEEN 0 AND 5 
               AND ( ss_list_price BETWEEN 18 AND 18 + 10 
                      OR ss_coupon_amt BETWEEN 1939 AND 1939 + 1000 
                      OR ss_wholesale_cost BETWEEN 34 AND 34 + 20 )) B1, 
       (SELECT Avg(ss_list_price)            B2_LP, 
               Count(ss_list_price)          B2_CNT, 
               Count(DISTINCT ss_list_price) B2_CNTD 
        FROM   store_sales 
        WHERE  ss_quantity BETWEEN 6 AND 10 
               AND ( ss_list_price BETWEEN 1 AND 1 + 10 
                      OR ss_coupon_amt BETWEEN 35 AND 35 + 1000 
                      OR ss_wholesale_cost BETWEEN 50 AND 50 + 20 )) B2, 
       (SELECT Avg(ss_list_price)            B3_LP, 
               Count(ss_list_price)          B3_CNT, 
               Count(DISTINCT ss_list_price) B3_CNTD 
        FROM   store_sales 
        WHERE  ss_quantity BETWEEN 11 AND 15 
               AND ( ss_list_price BETWEEN 91 AND 91 + 10 
                      OR ss_coupon_amt BETWEEN 1412 AND 1412 + 1000 
                      OR ss_wholesale_cost BETWEEN 17 AND 17 + 20 )) B3, 
       (SELECT Avg(ss_list_price)            B4_LP, 
               Count(ss_list_price)          B4_CNT, 
               Count(DISTINCT ss_list_price) B4_CNTD 
        FROM   store_sales 
        WHERE  ss_quantity BETWEEN 16 AND 20 
               AND ( ss_list_price BETWEEN 9 AND 9 + 10 
                      OR ss_coupon_amt BETWEEN 5270 AND 5270 + 1000 
                      OR ss_wholesale_cost BETWEEN 29 AND 29 + 20 )) B4, 
       (SELECT Avg(ss_list_price)            B5_LP, 
               Count(ss_list_price)          B5_CNT, 
               Count(DISTINCT ss_list_price) B5_CNTD 
        FROM   store_sales 
        WHERE  ss_quantity BETWEEN 21 AND 25 
               AND ( ss_list_price BETWEEN 45 AND 45 + 10 
                      OR ss_coupon_amt BETWEEN 826 AND 826 + 1000 
                      OR ss_wholesale_cost BETWEEN 5 AND 5 + 20 )) B5, 
       (SELECT Avg(ss_list_price)            B6_LP, 
               Count(ss_list_price)          B6_CNT, 
               Count(DISTINCT ss_list_price) B6_CNTD 
        FROM   store_sales 
        WHERE  ss_quantity BETWEEN 26 AND 30 
               AND ( ss_list_price BETWEEN 174 AND 174 + 10 
                      OR ss_coupon_amt BETWEEN 5548 AND 5548 + 1000 
                      OR ss_wholesale_cost BETWEEN 42 AND 42 + 20 )) B6
LIMIT 100;


-- start query 29 in stream 0 using template query29.tpl 
SELECT i_item_id, 
               i_item_desc, 
               s_store_id, 
               s_store_name, 
               Avg(ss_quantity)        AS store_sales_quantity, 
               Avg(sr_return_quantity) AS store_returns_quantity, 
               Avg(cs_quantity)        AS catalog_sales_quantity 
FROM   store_sales, 
       store_returns, 
       catalog_sales, 
       date_dim d1, 
       date_dim d2, 
       date_dim d3, 
       store, 
       item 
WHERE  d1.d_moy = 4 
       AND d1.d_year = 1998 
       AND d1.d_date_sk = ss_sold_date_sk 
       AND i_item_sk = ss_item_sk 
       AND s_store_sk = ss_store_sk 
       AND ss_customer_sk = sr_customer_sk 
       AND ss_item_sk = sr_item_sk 
       AND ss_ticket_number = sr_ticket_number 
       AND sr_returned_date_sk = d2.d_date_sk 
       AND d2.d_moy BETWEEN 4 AND 4 + 3 
       AND d2.d_year = 1998 
       AND sr_customer_sk = cs_bill_customer_sk 
       AND sr_item_sk = cs_item_sk 
       AND cs_sold_date_sk = d3.d_date_sk 
       AND d3.d_year IN ( 1998, 1998 + 1, 1998 + 2 ) 
GROUP  BY i_item_id, 
          i_item_desc, 
          s_store_id, 
          s_store_name 
ORDER  BY i_item_id, 
          i_item_desc, 
          s_store_id, 
          s_store_name
LIMIT 100;


-- start query 30 in stream 0 using template query30.tpl 
WITH customer_total_return 
     AS (SELECT wr_returning_customer_sk AS ctr_customer_sk, 
                ca_state                 AS ctr_state, 
                Sum(wr_return_amt)       AS ctr_total_return 
         FROM   web_returns, 
                date_dim, 
                customer_address 
         WHERE  wr_returned_date_sk = d_date_sk 
                AND d_year = 2000 
                AND wr_returning_addr_sk = ca_address_sk 
         GROUP  BY wr_returning_customer_sk, 
                   ca_state) 
SELECT c_customer_id, 
               c_salutation, 
               c_first_name, 
               c_last_name, 
               c_preferred_cust_flag, 
               c_birth_day, 
               c_birth_month, 
               c_birth_year, 
               c_birth_country, 
               c_login, 
               c_email_address, 
               c_last_review_date, 
               ctr_total_return 
FROM   customer_total_return ctr1, 
       customer_address, 
       customer 
WHERE  ctr1.ctr_total_return > (SELECT Avg(ctr_total_return) * 1.2 
                                FROM   customer_total_return ctr2 
                                WHERE  ctr1.ctr_state = ctr2.ctr_state) 
       AND ca_address_sk = c_current_addr_sk 
       AND ca_state = 'IN' 
       AND ctr1.ctr_customer_sk = c_customer_sk 
ORDER  BY c_customer_id, 
          c_salutation, 
          c_first_name, 
          c_last_name, 
          c_preferred_cust_flag, 
          c_birth_day, 
          c_birth_month, 
          c_birth_year, 
          c_birth_country, 
          c_login, 
          c_email_address, 
          c_last_review_date, 
          ctr_total_return
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 31 and QUERY_ID < 37);

-- start query 31 in stream 0 using template query31.tpl 
WITH ss 
     AS (SELECT ca_county, 
                d_qoy, 
                d_year, 
                Sum(ss_ext_sales_price) AS store_sales 
         FROM   store_sales, 
                date_dim, 
                customer_address 
         WHERE  ss_sold_date_sk = d_date_sk 
                AND ss_addr_sk = ca_address_sk 
         GROUP  BY ca_county, 
                   d_qoy, 
                   d_year), 
     ws 
     AS (SELECT ca_county, 
                d_qoy, 
                d_year, 
                Sum(ws_ext_sales_price) AS web_sales 
         FROM   web_sales, 
                date_dim, 
                customer_address 
         WHERE  ws_sold_date_sk = d_date_sk 
                AND ws_bill_addr_sk = ca_address_sk 
         GROUP  BY ca_county, 
                   d_qoy, 
                   d_year) 
SELECT ss1.ca_county, 
       ss1.d_year, 
       ws2.web_sales / ws1.web_sales     web_q1_q2_increase, 
       ss2.store_sales / ss1.store_sales store_q1_q2_increase, 
       ws3.web_sales / ws2.web_sales     web_q2_q3_increase, 
       ss3.store_sales / ss2.store_sales store_q2_q3_increase 
FROM   ss ss1, 
       ss ss2, 
       ss ss3, 
       ws ws1, 
       ws ws2, 
       ws ws3 
WHERE  ss1.d_qoy = 1 
       AND ss1.d_year = 2001 
       AND ss1.ca_county = ss2.ca_county 
       AND ss2.d_qoy = 2 
       AND ss2.d_year = 2001 
       AND ss2.ca_county = ss3.ca_county 
       AND ss3.d_qoy = 3 
       AND ss3.d_year = 2001 
       AND ss1.ca_county = ws1.ca_county 
       AND ws1.d_qoy = 1 
       AND ws1.d_year = 2001 
       AND ws1.ca_county = ws2.ca_county 
       AND ws2.d_qoy = 2 
       AND ws2.d_year = 2001 
       AND ws1.ca_county = ws3.ca_county 
       AND ws3.d_qoy = 3 
       AND ws3.d_year = 2001 
       AND CASE 
             WHEN ws1.web_sales > 0 THEN ws2.web_sales / ws1.web_sales 
             ELSE NULL 
           END > CASE 
                   WHEN ss1.store_sales > 0 THEN 
                   ss2.store_sales / ss1.store_sales 
                   ELSE NULL 
                 END 
       AND CASE 
             WHEN ws2.web_sales > 0 THEN ws3.web_sales / ws2.web_sales 
             ELSE NULL 
           END > CASE 
                   WHEN ss2.store_sales > 0 THEN 
                   ss3.store_sales / ss2.store_sales 
                   ELSE NULL 
                 END 
ORDER  BY ss1.d_year;


-- start query 32 in stream 0 using template query32.tpl 
SELECT 
       Sum(cs_ext_discount_amt) AS `excess discount amount`
FROM   catalog_sales , 
       item , 
       date_dim 
WHERE  i_manufact_id = 610 
AND    i_item_sk = cs_item_sk 
AND    d_date BETWEEN '2001-03-04' AND    ( 
              Cast('2001-03-04' AS DATE) + INTERVAL '90' day) 
AND    d_date_sk = cs_sold_date_sk 
AND    cs_ext_discount_amt > 
       ( 
              SELECT 1.3 * avg(cs_ext_discount_amt) 
              FROM   catalog_sales , 
                     date_dim 
              WHERE  cs_item_sk = i_item_sk 
              AND    d_date BETWEEN '2001-03-04' AND    ( 
                            cast('2001-03-04' AS date) + INTERVAL '90' day) 
              AND    d_date_sk = cs_sold_date_sk ) 
LIMIT 100;


-- start query 33 in stream 0 using template query33.tpl 
WITH ss 
     AS (SELECT i_manufact_id, 
                Sum(ss_ext_sales_price) total_sales 
         FROM   store_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_manufact_id IN (SELECT i_manufact_id 
                                  FROM   item 
                                  WHERE  i_category IN ( 'Books' )) 
                AND ss_item_sk = i_item_sk 
                AND ss_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 3 
                AND ss_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -5 
         GROUP  BY i_manufact_id), 
     cs 
     AS (SELECT i_manufact_id, 
                Sum(cs_ext_sales_price) total_sales 
         FROM   catalog_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_manufact_id IN (SELECT i_manufact_id 
                                  FROM   item 
                                  WHERE  i_category IN ( 'Books' )) 
                AND cs_item_sk = i_item_sk 
                AND cs_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 3 
                AND cs_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -5 
         GROUP  BY i_manufact_id), 
     ws 
     AS (SELECT i_manufact_id, 
                Sum(ws_ext_sales_price) total_sales 
         FROM   web_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_manufact_id IN (SELECT i_manufact_id 
                                  FROM   item 
                                  WHERE  i_category IN ( 'Books' )) 
                AND ws_item_sk = i_item_sk 
                AND ws_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 3 
                AND ws_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -5 
         GROUP  BY i_manufact_id) 
SELECT i_manufact_id, 
               Sum(total_sales) total_sales 
FROM   (SELECT * 
        FROM   ss 
        UNION ALL 
        SELECT * 
        FROM   cs 
        UNION ALL 
        SELECT * 
        FROM   ws) tmp1 
GROUP  BY i_manufact_id 
ORDER  BY total_sales
LIMIT 100;


-- start query 34 in stream 0 using template query34.tpl 
SELECT c_last_name, 
       c_first_name, 
       c_salutation, 
       c_preferred_cust_flag, 
       ss_ticket_number, 
       cnt 
FROM   (SELECT ss_ticket_number, 
               ss_customer_sk, 
               Count(*) cnt 
        FROM   store_sales, 
               date_dim, 
               store, 
               household_demographics 
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk 
               AND store_sales.ss_store_sk = store.s_store_sk 
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk 
               AND ( date_dim.d_dom BETWEEN 1 AND 3 
                      OR date_dim.d_dom BETWEEN 25 AND 28 ) 
               AND ( household_demographics.hd_buy_potential = '>10000' 
                      OR household_demographics.hd_buy_potential = 'unknown' ) 
               AND household_demographics.hd_vehicle_count > 0 
               AND ( CASE 
                       WHEN household_demographics.hd_vehicle_count > 0 THEN 
                       household_demographics.hd_dep_count / 
                       household_demographics.hd_vehicle_count 
                       ELSE NULL 
                     END ) > 1.2 
               AND date_dim.d_year IN ( 1999, 1999 + 1, 1999 + 2 ) 
               AND store.s_county IN ( 'Williamson County', 'Williamson County', 
                                       'Williamson County', 
                                                             'Williamson County' 
                                       , 
                                       'Williamson County', 'Williamson County', 
                                           'Williamson County', 
                                                             'Williamson County' 
                                     ) 
        GROUP  BY ss_ticket_number, 
                  ss_customer_sk) dn, 
       customer 
WHERE  ss_customer_sk = c_customer_sk 
       AND cnt BETWEEN 15 AND 20 
ORDER  BY c_last_name, 
          c_first_name, 
          c_salutation, 
          c_preferred_cust_flag DESC;


-- start query 35 in stream 0 using template query35.tpl 
SELECT ca_state, 
               cd_gender, 
               cd_marital_status, 
               cd_dep_count, 
               Count(*) cnt1, 
               Stddev_samp(cd_dep_count), 
               Avg(cd_dep_count), 
               Max(cd_dep_count), 
               cd_dep_employed_count, 
               Count(*) cnt2, 
               Stddev_samp(cd_dep_employed_count), 
               Avg(cd_dep_employed_count), 
               Max(cd_dep_employed_count), 
               cd_dep_college_count, 
               Count(*) cnt3, 
               Stddev_samp(cd_dep_college_count), 
               Avg(cd_dep_college_count), 
               Max(cd_dep_college_count) 
FROM   customer c, 
       customer_address ca, 
       customer_demographics 
WHERE  c.c_current_addr_sk = ca.ca_address_sk 
       AND cd_demo_sk = c.c_current_cdemo_sk 
       AND EXISTS (SELECT * 
                   FROM   store_sales, 
                          date_dim 
                   WHERE  c.c_customer_sk = ss_customer_sk 
                          AND ss_sold_date_sk = d_date_sk 
                          AND d_year = 2001 
                          AND d_qoy < 4) 
       AND ( EXISTS (SELECT * 
                     FROM   web_sales, 
                            date_dim 
                     WHERE  c.c_customer_sk = ws_bill_customer_sk 
                            AND ws_sold_date_sk = d_date_sk 
                            AND d_year = 2001 
                            AND d_qoy < 4) 
              OR EXISTS (SELECT * 
                         FROM   catalog_sales, 
                                date_dim 
                         WHERE  c.c_customer_sk = cs_ship_customer_sk 
                                AND cs_sold_date_sk = d_date_sk 
                                AND d_year = 2001 
                                AND d_qoy < 4) ) 
GROUP  BY ca_state, 
          cd_gender, 
          cd_marital_status, 
          cd_dep_count, 
          cd_dep_employed_count, 
          cd_dep_college_count 
ORDER  BY ca_state, 
          cd_gender, 
          cd_marital_status, 
          cd_dep_count, 
          cd_dep_employed_count, 
          cd_dep_college_count
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 37 and QUERY_ID < 43);

-- 36
SELECT
    SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin,
    i_category,
    i_class,
    GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
    RANK() OVER (
        PARTITION BY GROUPING(i_category) + GROUPING(i_class), 
                     CASE WHEN GROUPING(i_class) = 0 THEN i_category END
        ORDER BY SUM(ss_net_profit) / SUM(ss_ext_sales_price) ASC
    ) AS rank_within_parent
FROM
    store_sales
    JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
    JOIN item ON i_item_sk = ss_item_sk
    JOIN store ON s_store_sk = ss_store_sk
WHERE
    d1.d_year = 2000
    AND s_state IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN') -- Assuming 'TN' is intended multiple times
GROUP BY
    i_category, i_class WITH ROLLUP
ORDER BY
    lochierarchy DESC,
    CASE
        WHEN lochierarchy = 0 THEN i_category
    END,
    rank_within_parent
LIMIT 100;


-- start query 37 in stream 0 using template query37.tpl 
SELECT 
         i_item_id , 
         i_item_desc , 
         i_current_price 
FROM     item, 
         inventory, 
         date_dim, 
         catalog_sales 
WHERE    i_current_price BETWEEN 20 AND      20 + 30 
AND      inv_item_sk = i_item_sk 
AND      d_date_sk=inv_date_sk 
AND      d_date BETWEEN Cast('1999-03-06' AS DATE) AND      ( 
                  Cast('1999-03-06' AS DATE) + INTERVAL '60' day) 
AND      i_manufact_id IN (843,815,850,840) 
AND      inv_quantity_on_hand BETWEEN 100 AND      500 
AND      cs_item_sk = i_item_sk 
GROUP BY i_item_id, 
         i_item_desc, 
         i_current_price 
ORDER BY i_item_id 
LIMIT 100;


-- start query 38 in stream 0 using template query38.tpl 
SELECT Count(*) 
FROM   (SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   store_sales, 
               date_dim, 
               customer 
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk 
               AND store_sales.ss_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1188 AND 1188 + 11 
        INTERSECT 
        SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   catalog_sales, 
               date_dim, 
               customer 
        WHERE  catalog_sales.cs_sold_date_sk = date_dim.d_date_sk 
               AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1188 AND 1188 + 11 
        INTERSECT 
        SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   web_sales, 
               date_dim, 
               customer 
        WHERE  web_sales.ws_sold_date_sk = date_dim.d_date_sk 
               AND web_sales.ws_bill_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1188 AND 1188 + 11) hot_cust
LIMIT 100;


-- start query 39 in stream 0 using template query39.tpl 
WITH inv 
     AS (SELECT w_warehouse_name, 
                w_warehouse_sk, 
                i_item_sk, 
                d_moy, 
                stdev, 
                mean, 
                CASE mean 
                  WHEN 0 THEN NULL 
                  ELSE stdev / mean 
                END cov 
         FROM  (SELECT w_warehouse_name, 
                       w_warehouse_sk, 
                       i_item_sk, 
                       d_moy, 
                       Stddev_samp(inv_quantity_on_hand) stdev, 
                       Avg(inv_quantity_on_hand)         mean 
                FROM   inventory, 
                       item, 
                       warehouse, 
                       date_dim 
                WHERE  inv_item_sk = i_item_sk 
                       AND inv_warehouse_sk = w_warehouse_sk 
                       AND inv_date_sk = d_date_sk 
                       AND d_year = 2002 
                GROUP  BY w_warehouse_name, 
                          w_warehouse_sk, 
                          i_item_sk, 
                          d_moy) foo 
         WHERE  CASE mean 
                  WHEN 0 THEN 0 
                  ELSE stdev / mean 
                END > 1) 
SELECT inv1.w_warehouse_sk, 
       inv1.i_item_sk, 
       inv1.d_moy, 
       inv1.mean, 
       inv1.cov, 
       inv2.w_warehouse_sk, 
       inv2.i_item_sk, 
       inv2.d_moy, 
       inv2.mean, 
       inv2.cov 
FROM   inv inv1, 
       inv inv2 
WHERE  inv1.i_item_sk = inv2.i_item_sk 
       AND inv1.w_warehouse_sk = inv2.w_warehouse_sk 
       AND inv1.d_moy = 1 
       AND inv2.d_moy = 1 + 1 
       AND inv1.cov > 1.5 
ORDER  BY inv1.w_warehouse_sk, 
          inv1.i_item_sk, 
          inv1.d_moy, 
          inv1.mean, 
          inv1.cov, 
          inv2.d_moy, 
          inv2.mean, 
          inv2.cov;

-- 40
SELECT
    w_state,
    i_item_id,
    SUM(
        CASE
            WHEN (CAST(d_date AS DATE) < CAST('2002-06-01' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
            ELSE 0
        END
    ) AS sales_before,
    SUM(
        CASE
            WHEN (CAST(d_date AS DATE) >= CAST('2002-06-01' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0)
            ELSE 0
        END
    ) AS sales_after
FROM
    catalog_sales
LEFT OUTER JOIN
    catalog_returns ON (cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk)
JOIN
    warehouse ON cs_warehouse_sk = w_warehouse_sk
JOIN
    item ON i_item_sk = cs_item_sk
JOIN
    date_dim ON cs_sold_date_sk = d_date_sk
WHERE
    i_current_price BETWEEN 0.99 AND 1.49
    AND d_date BETWEEN (CAST('2002-06-01' AS DATE) - INTERVAL 30 DAY) AND (CAST('2002-06-01' AS DATE) + INTERVAL 30 DAY)
GROUP BY
    w_state,
    i_item_id
ORDER BY
    w_state,
    i_item_id
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 43 and QUERY_ID < 49);

-- start query 41 in stream 0 using template query41.tpl 
SELECT Distinct(i_product_name) 
FROM   item i1 
WHERE  i_manufact_id BETWEEN 765 AND 765 + 40 
       AND (SELECT Count(*) AS item_cnt 
            FROM   item 
            WHERE  ( i_manufact = i1.i_manufact 
                     AND ( ( i_category = 'Women' 
                             AND ( i_color = 'dim' 
                                    OR i_color = 'green' ) 
                             AND ( i_units = 'Gross' 
                                    OR i_units = 'Dozen' ) 
                             AND ( i_size = 'economy' 
                                    OR i_size = 'petite' ) ) 
                            OR ( i_category = 'Women' 
                                 AND ( i_color = 'navajo' 
                                        OR i_color = 'aquamarine' ) 
                                 AND ( i_units = 'Case' 
                                        OR i_units = 'Unknown' ) 
                                 AND ( i_size = 'large' 
                                        OR i_size = 'N/A' ) ) 
                            OR ( i_category = 'Men' 
                                 AND ( i_color = 'indian' 
                                        OR i_color = 'dark' ) 
                                 AND ( i_units = 'Oz' 
                                        OR i_units = 'Lb' ) 
                                 AND ( i_size = 'extra large' 
                                        OR i_size = 'small' ) ) 
                            OR ( i_category = 'Men' 
                                 AND ( i_color = 'peach' 
                                        OR i_color = 'purple' ) 
                                 AND ( i_units = 'Tbl' 
                                        OR i_units = 'Bunch' ) 
                                 AND ( i_size = 'economy' 
                                        OR i_size = 'petite' ) ) ) ) 
                    OR ( i_manufact = i1.i_manufact 
                         AND ( ( i_category = 'Women' 
                                 AND ( i_color = 'orchid' 
                                        OR i_color = 'peru' ) 
                                 AND ( i_units = 'Carton' 
                                        OR i_units = 'Cup' ) 
                                 AND ( i_size = 'economy' 
                                        OR i_size = 'petite' ) ) 
                                OR ( i_category = 'Women' 
                                     AND ( i_color = 'violet' 
                                            OR i_color = 'papaya' ) 
                                     AND ( i_units = 'Ounce' 
                                            OR i_units = 'Box' ) 
                                     AND ( i_size = 'large' 
                                            OR i_size = 'N/A' ) ) 
                                OR ( i_category = 'Men' 
                                     AND ( i_color = 'drab' 
                                            OR i_color = 'grey' ) 
                                     AND ( i_units = 'Each' 
                                            OR i_units = 'N/A' ) 
                                     AND ( i_size = 'extra large' 
                                            OR i_size = 'small' ) ) 
                                OR ( i_category = 'Men' 
                                     AND ( i_color = 'chocolate' 
                                            OR i_color = 'antique' ) 
                                     AND ( i_units = 'Dram' 
                                            OR i_units = 'Gram' ) 
                                     AND ( i_size = 'economy' 
                                            OR i_size = 'petite' ) ) ) )) > 0 
ORDER  BY i_product_name
LIMIT 100;


-- start query 42 in stream 0 using template query42.tpl 
SELECT dt.d_year, 
               item.i_category_id, 
               item.i_category, 
               Sum(ss_ext_sales_price) 
FROM   date_dim dt, 
       store_sales, 
       item 
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk 
       AND store_sales.ss_item_sk = item.i_item_sk 
       AND item.i_manager_id = 1 
       AND dt.d_moy = 12 
       AND dt.d_year = 2000 
GROUP  BY dt.d_year, 
          item.i_category_id, 
          item.i_category 
ORDER  BY Sum(ss_ext_sales_price) DESC, 
          dt.d_year, 
          item.i_category_id, 
          item.i_category
LIMIT 100;


-- start query 43 in stream 0 using template query43.tpl 
SELECT s_store_name, 
               s_store_id, 
               Sum(CASE 
                     WHEN ( d_day_name = 'Sunday' ) THEN ss_sales_price 
                     ELSE NULL 
                   END) sun_sales, 
               Sum(CASE 
                     WHEN ( d_day_name = 'Monday' ) THEN ss_sales_price 
                     ELSE NULL 
                   END) mon_sales, 
               Sum(CASE 
                     WHEN ( d_day_name = 'Tuesday' ) THEN ss_sales_price 
                     ELSE NULL 
                   END) tue_sales, 
               Sum(CASE 
                     WHEN ( d_day_name = 'Wednesday' ) THEN ss_sales_price 
                     ELSE NULL 
                   END) wed_sales, 
               Sum(CASE 
                     WHEN ( d_day_name = 'Thursday' ) THEN ss_sales_price 
                     ELSE NULL 
                   END) thu_sales, 
               Sum(CASE 
                     WHEN ( d_day_name = 'Friday' ) THEN ss_sales_price 
                     ELSE NULL 
                   END) fri_sales, 
               Sum(CASE 
                     WHEN ( d_day_name = 'Saturday' ) THEN ss_sales_price 
                     ELSE NULL 
                   END) sat_sales 
FROM   date_dim, 
       store_sales, 
       store 
WHERE  d_date_sk = ss_sold_date_sk 
       AND s_store_sk = ss_store_sk 
       AND s_gmt_offset = -5 
       AND d_year = 2002 
GROUP  BY s_store_name, 
          s_store_id 
ORDER  BY s_store_name, 
          s_store_id, 
          sun_sales, 
          mon_sales, 
          tue_sales, 
          wed_sales, 
          thu_sales, 
          fri_sales, 
          sat_sales
LIMIT 100;


-- start query 44 in stream 0 using template query44.tpl 
SELECT asceding.rnk, 
               i1.i_product_name best_performing, 
               i2.i_product_name worst_performing 
FROM  (SELECT * 
       FROM   (SELECT item_sk, 
                      Rank() 
                        OVER ( 
                          ORDER BY rank_col ASC) rnk 
               FROM   (SELECT ss_item_sk         item_sk, 
                              Avg(ss_net_profit) rank_col 
                       FROM   store_sales ss1 
                       WHERE  ss_store_sk = 4 
                       GROUP  BY ss_item_sk 
                       HAVING Avg(ss_net_profit) > 0.9 * 
                              (SELECT Avg(ss_net_profit) 
                                      rank_col 
                               FROM   store_sales 
                               WHERE  ss_store_sk = 4 
                                      AND ss_cdemo_sk IS 
                                          NULL 
                               GROUP  BY ss_store_sk))V1) 
              V11 
       WHERE  rnk < 11) asceding, 
      (SELECT * 
       FROM   (SELECT item_sk, 
                      Rank() 
                        OVER ( 
                          ORDER BY rank_col DESC) rnk 
               FROM   (SELECT ss_item_sk         item_sk, 
                              Avg(ss_net_profit) rank_col 
                       FROM   store_sales ss1 
                       WHERE  ss_store_sk = 4 
                       GROUP  BY ss_item_sk 
                       HAVING Avg(ss_net_profit) > 0.9 * 
                              (SELECT Avg(ss_net_profit) 
                                      rank_col 
                               FROM   store_sales 
                               WHERE  ss_store_sk = 4 
                                      AND ss_cdemo_sk IS 
                                          NULL 
                               GROUP  BY ss_store_sk))V2) 
              V21 
       WHERE  rnk < 11) descending, 
      item i1, 
      item i2 
WHERE  asceding.rnk = descending.rnk 
       AND i1.i_item_sk = asceding.item_sk 
       AND i2.i_item_sk = descending.item_sk 
ORDER  BY asceding.rnk
LIMIT 100;


-- start query 45 in stream 0 using template query45.tpl 
SELECT ca_zip, 
               ca_state, 
               Sum(ws_sales_price) 
FROM   web_sales, 
       customer, 
       customer_address, 
       date_dim, 
       item 
WHERE  ws_bill_customer_sk = c_customer_sk 
       AND c_current_addr_sk = ca_address_sk 
       AND ws_item_sk = i_item_sk 
       AND ( Substr(ca_zip, 1, 5) IN ( '85669', '86197', '88274', '83405', 
                                       '86475', '85392', '85460', '80348', 
                                       '81792' ) 
              OR i_item_id IN (SELECT i_item_id 
                               FROM   item 
                               WHERE  i_item_sk IN ( 2, 3, 5, 7, 
                                                     11, 13, 17, 19, 
                                                     23, 29 )) ) 
       AND ws_sold_date_sk = d_date_sk 
       AND d_qoy = 1 
       AND d_year = 2000 
GROUP  BY ca_zip, 
          ca_state 
ORDER  BY ca_zip, 
          ca_state
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 49 and QUERY_ID < 55);

-- start query 46 in stream 0 using template query46.tpl 
SELECT c_last_name, 
               c_first_name, 
               ca_city, 
               bought_city, 
               ss_ticket_number, 
               amt, 
               profit 
FROM   (SELECT ss_ticket_number, 
               ss_customer_sk, 
               ca_city            bought_city, 
               Sum(ss_coupon_amt) amt, 
               Sum(ss_net_profit) profit 
        FROM   store_sales, 
               date_dim, 
               store, 
               household_demographics, 
               customer_address 
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk 
               AND store_sales.ss_store_sk = store.s_store_sk 
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk 
               AND store_sales.ss_addr_sk = customer_address.ca_address_sk 
               AND ( household_demographics.hd_dep_count = 6 
                      OR household_demographics.hd_vehicle_count = 0 ) 
               AND date_dim.d_dow IN ( 6, 0 ) 
               AND date_dim.d_year IN ( 2000, 2000 + 1, 2000 + 2 ) 
               AND store.s_city IN ( 'Midway', 'Fairview', 'Fairview', 
                                     'Fairview', 
                                     'Fairview' ) 
        GROUP  BY ss_ticket_number, 
                  ss_customer_sk, 
                  ss_addr_sk, 
                  ca_city) dn, 
       customer, 
       customer_address current_addr 
WHERE  ss_customer_sk = c_customer_sk 
       AND customer.c_current_addr_sk = current_addr.ca_address_sk 
       AND current_addr.ca_city <> bought_city 
ORDER  BY c_last_name, 
          c_first_name, 
          ca_city, 
          bought_city, 
          ss_ticket_number
LIMIT 100;


-- start query 47 in stream 0 using template query47.tpl 
WITH v1 
     AS (SELECT i_category, 
                i_brand, 
                s_store_name, 
                s_company_name, 
                d_year, 
                d_moy, 
                Sum(ss_sales_price)         sum_sales, 
                Avg(Sum(ss_sales_price)) 
                  OVER ( 
                    partition BY i_category, i_brand, s_store_name, 
                  s_company_name, 
                  d_year) 
                                            avg_monthly_sales, 
                Rank() 
                  OVER ( 
                    partition BY i_category, i_brand, s_store_name, 
                  s_company_name 
                    ORDER BY d_year, d_moy) rn 
         FROM   item, 
                store_sales, 
                date_dim, 
                store 
         WHERE  ss_item_sk = i_item_sk 
                AND ss_sold_date_sk = d_date_sk 
                AND ss_store_sk = s_store_sk 
                AND ( d_year = 1999 
                       OR ( d_year = 1999 - 1 
                            AND d_moy = 12 ) 
                       OR ( d_year = 1999 + 1 
                            AND d_moy = 1 ) ) 
         GROUP  BY i_category, 
                   i_brand, 
                   s_store_name, 
                   s_company_name, 
                   d_year, 
                   d_moy), 
     v2 
     AS (SELECT v1.i_category, 
                v1.d_year, 
                v1.d_moy, 
                v1.avg_monthly_sales, 
                v1.sum_sales, 
                v1_lag.sum_sales  psum, 
                v1_lead.sum_sales nsum 
         FROM   v1, 
                v1 v1_lag, 
                v1 v1_lead 
         WHERE  v1.i_category = v1_lag.i_category 
                AND v1.i_category = v1_lead.i_category 
                AND v1.i_brand = v1_lag.i_brand 
                AND v1.i_brand = v1_lead.i_brand 
                AND v1.s_store_name = v1_lag.s_store_name 
                AND v1.s_store_name = v1_lead.s_store_name 
                AND v1.s_company_name = v1_lag.s_company_name 
                AND v1.s_company_name = v1_lead.s_company_name 
                AND v1.rn = v1_lag.rn + 1 
                AND v1.rn = v1_lead.rn - 1) 
SELECT * 
FROM   v2 
WHERE  d_year = 1999 
       AND avg_monthly_sales > 0 
       AND CASE 
             WHEN avg_monthly_sales > 0 THEN Abs(sum_sales - avg_monthly_sales) 
                                             / 
                                             avg_monthly_sales 
             ELSE NULL 
           END > 0.1 
ORDER  BY sum_sales - avg_monthly_sales, 
          3
LIMIT 100;


-- start query 48 in stream 0 using template query48.tpl 
SELECT 
    SUM(ss_quantity) 
FROM   
    store_sales
    JOIN store ON s_store_sk = ss_store_sk 
    JOIN date_dim ON ss_sold_date_sk = d_date_sk 
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk 
    JOIN customer_address ON ss_addr_sk = ca_address_sk
WHERE  
    d_year = 1999 
    AND (
        (cd_marital_status = 'W' AND cd_education_status = 'Secondary' AND ss_sales_price BETWEEN 100.00 AND 150.00) 
        OR (cd_marital_status = 'M' AND cd_education_status = 'Advanced Degree' AND ss_sales_price BETWEEN 50.00 AND 100.00) 
        OR (cd_marital_status = 'D' AND cd_education_status = '2 yr Degree' AND ss_sales_price BETWEEN 150.00 AND 200.00)
    )
    AND (
        (ca_country = 'United States' AND ca_state IN ('TX', 'NE', 'MO') AND ss_net_profit BETWEEN 0 AND 2000) 
        OR (ca_country = 'United States' AND ca_state IN ('CO', 'TN', 'ND') AND ss_net_profit BETWEEN 150 AND 3000) 
        OR (ca_country = 'United States' AND ca_state IN ('OK', 'PA', 'CA') AND ss_net_profit BETWEEN 50 AND 25000)
    );


-- start query 49 in stream 0 using template query49.tpl 
SELECT 'web' AS channel, 
               web.item, 
               web.return_ratio, 
               web.return_rank, 
               web.currency_rank 
FROM   (SELECT item, 
               return_ratio, 
               currency_ratio, 
               Rank() 
                 OVER ( 
                   ORDER BY return_ratio)   AS return_rank, 
               Rank() 
                 OVER ( 
                   ORDER BY currency_ratio) AS currency_rank 
        FROM   (SELECT ws.ws_item_sk                                       AS 
                       item, 
                       ( Cast(Sum(COALESCE(wr.wr_return_quantity, 0)) AS DEC(15, 
                              4)) / 
                         Cast( 
                         Sum(COALESCE(ws.ws_quantity, 0)) AS DEC(15, 4)) ) AS 
                       return_ratio, 
                       ( Cast(Sum(COALESCE(wr.wr_return_amt, 0)) AS DEC(15, 4)) 
                         / Cast( 
                         Sum( 
                         COALESCE(ws.ws_net_paid, 0)) AS DEC(15, 
                         4)) )                                             AS 
                       currency_ratio 
                FROM   web_sales ws 
                       LEFT OUTER JOIN web_returns wr 
                                    ON ( ws.ws_order_number = wr.wr_order_number 
                                         AND ws.ws_item_sk = wr.wr_item_sk ), 
                       date_dim 
                WHERE  wr.wr_return_amt > 10000 
                       AND ws.ws_net_profit > 1 
                       AND ws.ws_net_paid > 0 
                       AND ws.ws_quantity > 0 
                       AND ws_sold_date_sk = d_date_sk 
                       AND d_year = 1999 
                       AND d_moy = 12 
                GROUP  BY ws.ws_item_sk) in_web) web 
WHERE  ( web.return_rank <= 10 
          OR web.currency_rank <= 10 ) 
UNION 
SELECT 'catalog' AS channel, 
       catalog.item, 
       catalog.return_ratio, 
       catalog.return_rank, 
       catalog.currency_rank 
FROM   (SELECT item, 
               return_ratio, 
               currency_ratio, 
               Rank() 
                 OVER ( 
                   ORDER BY return_ratio)   AS return_rank, 
               Rank() 
                 OVER ( 
                   ORDER BY currency_ratio) AS currency_rank 
        FROM   (SELECT cs.cs_item_sk                                       AS 
                       item, 
                       ( Cast(Sum(COALESCE(cr.cr_return_quantity, 0)) AS DEC(15, 
                              4)) / 
                         Cast( 
                         Sum(COALESCE(cs.cs_quantity, 0)) AS DEC(15, 4)) ) AS 
                       return_ratio, 
                       ( Cast(Sum(COALESCE(cr.cr_return_amount, 0)) AS DEC(15, 4 
                              )) / 
                         Cast(Sum( 
                         COALESCE(cs.cs_net_paid, 0)) AS DEC( 
                         15, 4)) )                                         AS 
                       currency_ratio 
                FROM   catalog_sales cs 
                       LEFT OUTER JOIN catalog_returns cr 
                                    ON ( cs.cs_order_number = cr.cr_order_number 
                                         AND cs.cs_item_sk = cr.cr_item_sk ), 
                       date_dim 
                WHERE  cr.cr_return_amount > 10000 
                       AND cs.cs_net_profit > 1 
                       AND cs.cs_net_paid > 0 
                       AND cs.cs_quantity > 0 
                       AND cs_sold_date_sk = d_date_sk 
                       AND d_year = 1999 
                       AND d_moy = 12 
                GROUP  BY cs.cs_item_sk) in_cat) catalog 
WHERE  ( catalog.return_rank <= 10 
          OR catalog.currency_rank <= 10 ) 
UNION 
SELECT 'store' AS channel, 
       store.item, 
       store.return_ratio, 
       store.return_rank, 
       store.currency_rank 
FROM   (SELECT item, 
               return_ratio, 
               currency_ratio, 
               Rank() 
                 OVER ( 
                   ORDER BY return_ratio)   AS return_rank, 
               Rank() 
                 OVER ( 
                   ORDER BY currency_ratio) AS currency_rank 
        FROM   (SELECT sts.ss_item_sk                                       AS 
                       item, 
                       ( Cast(Sum(COALESCE(sr.sr_return_quantity, 0)) AS DEC(15, 
                              4)) / 
                         Cast( 
                         Sum(COALESCE(sts.ss_quantity, 0)) AS DEC(15, 4)) ) AS 
                       return_ratio, 
                       ( Cast(Sum(COALESCE(sr.sr_return_amt, 0)) AS DEC(15, 4)) 
                         / Cast( 
                         Sum( 
                         COALESCE(sts.ss_net_paid, 0)) AS DEC(15, 4)) )     AS 
                       currency_ratio 
                FROM   store_sales sts 
                       LEFT OUTER JOIN store_returns sr 
                                    ON ( sts.ss_ticket_number = 
                                         sr.sr_ticket_number 
                                         AND sts.ss_item_sk = sr.sr_item_sk ), 
                       date_dim 
                WHERE  sr.sr_return_amt > 10000 
                       AND sts.ss_net_profit > 1 
                       AND sts.ss_net_paid > 0 
                       AND sts.ss_quantity > 0 
                       AND ss_sold_date_sk = d_date_sk 
                       AND d_year = 1999 
                       AND d_moy = 12 
                GROUP  BY sts.ss_item_sk) in_store) store 
WHERE  ( store.return_rank <= 10 
          OR store.currency_rank <= 10 ) 
ORDER  BY 1, 
          4, 
          5
LIMIT 100;


-- start query 50 in stream 0 using template query50.tpl 
SELECT s_store_name, 
               s_company_id, 
               s_street_number, 
               s_street_name, 
               s_street_type, 
               s_suite_number, 
               s_city, 
               s_county, 
               s_state, 
               s_zip, 
               Sum(CASE 
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk <= 30 ) THEN 1 
                     ELSE 0 
                   END) AS `30 days`, 
               Sum(CASE 
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 30 ) 
                          AND ( sr_returned_date_sk - ss_sold_date_sk <= 60 ) 
                   THEN 1 
                     ELSE 0 
                   END) AS `31-60 days`, 
               Sum(CASE 
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 60 ) 
                          AND ( sr_returned_date_sk - ss_sold_date_sk <= 90 ) 
                   THEN 1 
                     ELSE 0 
                   END) AS `61-90 days`, 
               Sum(CASE 
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 90 ) 
                          AND ( sr_returned_date_sk - ss_sold_date_sk <= 120 ) 
                   THEN 1 
                     ELSE 0 
                   END) AS `91-120 days`, 
               Sum(CASE 
                     WHEN ( sr_returned_date_sk - ss_sold_date_sk > 120 ) THEN 1 
                     ELSE 0 
                   END) AS `>120 days` 
FROM   store_sales, 
       store_returns, 
       store, 
       date_dim d1, 
       date_dim d2 
WHERE  d2.d_year = 2002 
       AND d2.d_moy = 9 
       AND ss_ticket_number = sr_ticket_number 
       AND ss_item_sk = sr_item_sk 
       AND ss_sold_date_sk = d1.d_date_sk 
       AND sr_returned_date_sk = d2.d_date_sk 
       AND ss_customer_sk = sr_customer_sk 
       AND ss_store_sk = s_store_sk 
GROUP  BY s_store_name, 
          s_company_id, 
          s_street_number, 
          s_street_name, 
          s_street_type, 
          s_suite_number, 
          s_city, 
          s_county, 
          s_state, 
          s_zip 
ORDER  BY s_store_name, 
          s_company_id, 
          s_street_number, 
          s_street_name, 
          s_street_type, 
          s_suite_number, 
          s_city, 
          s_county, 
          s_state, 
          s_zip
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 55 and QUERY_ID < 61);

-- start query 51 in stream 0 using template query51.tpl 
WITH web_v1 AS 
( 
    SELECT
        ws_item_sk AS item_sk,
        d_date,
        SUM(SUM(ws_sales_price)) OVER (PARTITION BY ws_item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
    FROM
        web_sales
        JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE
        d_month_seq BETWEEN 1192 AND 1192 + 11
        AND ws_item_sk IS NOT NULL
    GROUP BY
        ws_item_sk,
        d_date
), 
store_v1 AS 
( 
    SELECT
        ss_item_sk AS item_sk,
        d_date,
        SUM(SUM(ss_sales_price)) OVER (PARTITION BY ss_item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
    FROM
        store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE
        d_month_seq BETWEEN 1192 AND 1192 + 11
        AND ss_item_sk IS NOT NULL
    GROUP BY
        ss_item_sk,
        d_date
) 
SELECT
    *
FROM
    (
        SELECT
            item_sk,
            d_date,
            web_sales,
            store_sales,
            MAX(web_sales) OVER (PARTITION BY item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS web_cumulative,
            MAX(store_sales) OVER (PARTITION BY item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS store_cumulative
        FROM
            (
                SELECT
                    COALESCE(web.item_sk, store.item_sk) AS item_sk,
                    COALESCE(web.d_date, store.d_date) AS d_date,
                    web.cume_sales AS web_sales,
                    store.cume_sales AS store_sales
                FROM
                    web_v1 web
                LEFT JOIN
                    store_v1 store ON web.item_sk = store.item_sk AND web.d_date = store.d_date
                
                UNION ALL
                
                SELECT
                    COALESCE(web.item_sk, store.item_sk) AS item_sk,
                    COALESCE(web.d_date, store.d_date) AS d_date,
                    web.cume_sales AS web_sales,
                    store.cume_sales AS store_sales
                FROM
                    web_v1 web
                RIGHT JOIN
                    store_v1 store ON web.item_sk = store.item_sk AND web.d_date = store.d_date
            ) x
    ) y
WHERE
    web_cumulative > store_cumulative 
ORDER BY
    item_sk, 
    d_date 
LIMIT 100;


-- start query 52 in stream 0 using template query52.tpl 
SELECT dt.d_year, 
               item.i_brand_id         brand_id, 
               item.i_brand            brand, 
               Sum(ss_ext_sales_price) ext_price 
FROM   date_dim dt, 
       store_sales, 
       item 
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk 
       AND store_sales.ss_item_sk = item.i_item_sk 
       AND item.i_manager_id = 1 
       AND dt.d_moy = 11 
       AND dt.d_year = 1999 
GROUP  BY dt.d_year, 
          item.i_brand, 
          item.i_brand_id 
ORDER  BY dt.d_year, 
          ext_price DESC, 
          brand_id
LIMIT 100;


-- start query 53 in stream 0 using template query53.tpl 
SELECT * 
FROM   (SELECT i_manufact_id, 
               Sum(ss_sales_price)             sum_sales, 
               Avg(Sum(ss_sales_price)) 
                 OVER ( 
                   partition BY i_manufact_id) avg_quarterly_sales 
        FROM   item, 
               store_sales, 
               date_dim, 
               store 
        WHERE  ss_item_sk = i_item_sk 
               AND ss_sold_date_sk = d_date_sk 
               AND ss_store_sk = s_store_sk 
               AND d_month_seq IN ( 1199, 1199 + 1, 1199 + 2, 1199 + 3, 
                                    1199 + 4, 1199 + 5, 1199 + 6, 1199 + 7, 
                                    1199 + 8, 1199 + 9, 1199 + 10, 1199 + 11 ) 
               AND ( ( i_category IN ( 'Books', 'Children', 'Electronics' ) 
                       AND i_class IN ( 'personal', 'portable', 'reference', 
                                        'self-help' ) 
                       AND i_brand IN ( 'scholaramalgamalg #14', 
                                        'scholaramalgamalg #7' 
                                        , 
                                        'exportiunivamalg #9', 
                                                       'scholaramalgamalg #9' ) 
                     ) 
                      OR ( i_category IN ( 'Women', 'Music', 'Men' ) 
                           AND i_class IN ( 'accessories', 'classical', 
                                            'fragrances', 
                                            'pants' ) 
                           AND i_brand IN ( 'amalgimporto #1', 
                                            'edu packscholar #1', 
                                            'exportiimporto #1', 
                                                'importoamalg #1' ) ) ) 
        GROUP  BY i_manufact_id, 
                  d_qoy) tmp1 
WHERE  CASE 
         WHEN avg_quarterly_sales > 0 THEN Abs (sum_sales - avg_quarterly_sales) 
                                           / 
                                           avg_quarterly_sales 
         ELSE NULL 
       END > 0.1 
ORDER  BY avg_quarterly_sales, 
          sum_sales, 
          i_manufact_id
LIMIT 100;


-- start query 54 in stream 0 using template query54.tpl 
WITH my_customers AS (
    SELECT DISTINCT
        c_customer_sk,
        c_current_addr_sk
    FROM (
        SELECT
            cs_sold_date_sk AS sold_date_sk,
            cs_bill_customer_sk AS customer_sk,
            cs_item_sk AS item_sk
        FROM
            catalog_sales
        UNION ALL
        SELECT
            ws_sold_date_sk AS sold_date_sk,
            ws_bill_customer_sk AS customer_sk,
            ws_item_sk AS item_sk
        FROM
            web_sales
    ) cs_or_ws_sales
    JOIN item ON item_sk = i_item_sk
    JOIN date_dim ON sold_date_sk = d_date_sk
    JOIN customer ON c_customer_sk = cs_or_ws_sales.customer_sk
    WHERE
        i_category = 'Sports'
        AND i_class = 'fitness'
        AND d_moy = 5
        AND d_year = 2000
),
my_revenue AS (
    SELECT
        c_customer_sk,
        SUM(ss_ext_sales_price) AS revenue
    FROM
        my_customers
    JOIN store_sales ON c_customer_sk = ss_customer_sk
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    JOIN store ON ca_county = s_county AND ca_state = s_state
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE
        d_month_seq BETWEEN (SELECT DISTINCT d_month_seq + 1 FROM date_dim WHERE d_year = 2000 AND d_moy = 5)
                          AND (SELECT DISTINCT d_month_seq + 3 FROM date_dim WHERE d_year = 2000 AND d_moy = 5)
    GROUP BY
        c_customer_sk
),
segments AS (
    SELECT
        CAST(revenue / 50 AS SIGNED) AS segment
    FROM
        my_revenue
)
SELECT
    segment,
    COUNT(*) AS num_customers,
    segment * 50 AS segment_base
FROM
    segments
GROUP BY
    segment
ORDER BY
    segment,
    num_customers
LIMIT 100;


-- start query 55 in stream 0 using template query55.tpl 
SELECT i_brand_id              brand_id, 
               i_brand                 brand, 
               Sum(ss_ext_sales_price) ext_price 
FROM   date_dim, 
       store_sales, 
       item 
WHERE  d_date_sk = ss_sold_date_sk 
       AND ss_item_sk = i_item_sk 
       AND i_manager_id = 33 
       AND d_moy = 12 
       AND d_year = 1998 
GROUP  BY i_brand, 
          i_brand_id 
ORDER  BY ext_price DESC, 
          i_brand_id
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 61 and QUERY_ID < 67);

-- start query 56 in stream 0 using template query56.tpl 
WITH ss 
     AS (SELECT i_item_id, 
                Sum(ss_ext_sales_price) total_sales 
         FROM   store_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' ) 
                             ) 
                AND ss_item_sk = i_item_sk 
                AND ss_sold_date_sk = d_date_sk 
                AND d_year = 1998 
                AND d_moy = 3 
                AND ss_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id), 
     cs 
     AS (SELECT i_item_id, 
                Sum(cs_ext_sales_price) total_sales 
         FROM   catalog_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' ) 
                             ) 
                AND cs_item_sk = i_item_sk 
                AND cs_sold_date_sk = d_date_sk 
                AND d_year = 1998 
                AND d_moy = 3 
                AND cs_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id), 
     ws 
     AS (SELECT i_item_id, 
                Sum(ws_ext_sales_price) total_sales 
         FROM   web_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_color IN ( 'firebrick', 'rosy', 'white' ) 
                             ) 
                AND ws_item_sk = i_item_sk 
                AND ws_sold_date_sk = d_date_sk 
                AND d_year = 1998 
                AND d_moy = 3 
                AND ws_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id) 
SELECT i_item_id, 
               Sum(total_sales) total_sales 
FROM   (SELECT * 
        FROM   ss 
        UNION ALL 
        SELECT * 
        FROM   cs 
        UNION ALL 
        SELECT * 
        FROM   ws) tmp1 
GROUP  BY i_item_id 
ORDER  BY total_sales
LIMIT 100;


-- start query 57 in stream 0 using template query57.tpl 
WITH v1 
     AS (SELECT i_category, 
                i_brand, 
                cc_name, 
                d_year, 
                d_moy, 
                Sum(cs_sales_price)                                    sum_sales 
                , 
                Avg(Sum(cs_sales_price)) 
                  OVER ( 
                    partition BY i_category, i_brand, cc_name, d_year) 
                avg_monthly_sales 
                   , 
                Rank() 
                  OVER ( 
                    partition BY i_category, i_brand, cc_name 
                    ORDER BY d_year, d_moy)                            rn 
         FROM   item, 
                catalog_sales, 
                date_dim, 
                call_center 
         WHERE  cs_item_sk = i_item_sk 
                AND cs_sold_date_sk = d_date_sk 
                AND cc_call_center_sk = cs_call_center_sk 
                AND ( d_year = 2000 
                       OR ( d_year = 2000 - 1 
                            AND d_moy = 12 ) 
                       OR ( d_year = 2000 + 1 
                            AND d_moy = 1 ) ) 
         GROUP  BY i_category, 
                   i_brand, 
                   cc_name, 
                   d_year, 
                   d_moy), 
     v2 
     AS (SELECT v1.i_brand, 
                v1.d_year, 
                v1.avg_monthly_sales, 
                v1.sum_sales, 
                v1_lag.sum_sales  psum, 
                v1_lead.sum_sales nsum 
         FROM   v1, 
                v1 v1_lag, 
                v1 v1_lead 
         WHERE  v1.i_category = v1_lag.i_category 
                AND v1.i_category = v1_lead.i_category 
                AND v1.i_brand = v1_lag.i_brand 
                AND v1.i_brand = v1_lead.i_brand 
                AND v1. cc_name = v1_lag. cc_name 
                AND v1. cc_name = v1_lead. cc_name 
                AND v1.rn = v1_lag.rn + 1 
                AND v1.rn = v1_lead.rn - 1) 
SELECT * 
FROM   v2 
WHERE  d_year = 2000 
       AND avg_monthly_sales > 0 
       AND CASE 
             WHEN avg_monthly_sales > 0 THEN Abs(sum_sales - avg_monthly_sales) 
                                             / 
                                             avg_monthly_sales 
             ELSE NULL 
           END > 0.1 
ORDER  BY sum_sales - avg_monthly_sales, 
          3
LIMIT 100;


-- start query 58 in stream 0 using template query58.tpl 
WITH ss_items 
     AS (SELECT i_item_id               item_id, 
                Sum(ss_ext_sales_price) ss_item_rev 
         FROM   store_sales, 
                item, 
                date_dim 
         WHERE  ss_item_sk = i_item_sk 
                AND d_date IN (SELECT d_date 
                               FROM   date_dim 
                               WHERE  d_week_seq = (SELECT d_week_seq 
                                                    FROM   date_dim 
                                                    WHERE  d_date = '2002-02-25' 
                                                   )) 
                AND ss_sold_date_sk = d_date_sk 
         GROUP  BY i_item_id), 
     cs_items 
     AS (SELECT i_item_id               item_id, 
                Sum(cs_ext_sales_price) cs_item_rev 
         FROM   catalog_sales, 
                item, 
                date_dim 
         WHERE  cs_item_sk = i_item_sk 
                AND d_date IN (SELECT d_date 
                               FROM   date_dim 
                               WHERE  d_week_seq = (SELECT d_week_seq 
                                                    FROM   date_dim 
                                                    WHERE  d_date = '2002-02-25' 
                                                   )) 
                AND cs_sold_date_sk = d_date_sk 
         GROUP  BY i_item_id), 
     ws_items 
     AS (SELECT i_item_id               item_id, 
                Sum(ws_ext_sales_price) ws_item_rev 
         FROM   web_sales, 
                item, 
                date_dim 
         WHERE  ws_item_sk = i_item_sk 
                AND d_date IN (SELECT d_date 
                               FROM   date_dim 
                               WHERE  d_week_seq = (SELECT d_week_seq 
                                                    FROM   date_dim 
                                                    WHERE  d_date = '2002-02-25' 
                                                   )) 
                AND ws_sold_date_sk = d_date_sk 
         GROUP  BY i_item_id) 
SELECT ss_items.item_id, 
               ss_item_rev, 
               ss_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 * 
               100 ss_dev, 
               cs_item_rev, 
               cs_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 * 
               100 cs_dev, 
               ws_item_rev, 
               ws_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 * 
               100 ws_dev, 
               ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 
               average 
FROM   ss_items, 
       cs_items, 
       ws_items 
WHERE  ss_items.item_id = cs_items.item_id 
       AND ss_items.item_id = ws_items.item_id 
       AND ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev 
       AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev 
       AND cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev 
       AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev 
       AND ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev 
       AND ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev 
ORDER  BY item_id, 
          ss_item_rev
LIMIT 100;


-- start query 59 in stream 0 using template query59.tpl 
WITH wss 
     AS (SELECT d_week_seq, 
                ss_store_sk, 
                Sum(CASE 
                      WHEN ( d_day_name = 'Sunday' ) THEN ss_sales_price 
                      ELSE NULL 
                    END) sun_sales, 
                Sum(CASE 
                      WHEN ( d_day_name = 'Monday' ) THEN ss_sales_price 
                      ELSE NULL 
                    END) mon_sales, 
                Sum(CASE 
                      WHEN ( d_day_name = 'Tuesday' ) THEN ss_sales_price 
                      ELSE NULL 
                    END) tue_sales, 
                Sum(CASE 
                      WHEN ( d_day_name = 'Wednesday' ) THEN ss_sales_price 
                      ELSE NULL 
                    END) wed_sales, 
                Sum(CASE 
                      WHEN ( d_day_name = 'Thursday' ) THEN ss_sales_price 
                      ELSE NULL 
                    END) thu_sales, 
                Sum(CASE 
                      WHEN ( d_day_name = 'Friday' ) THEN ss_sales_price 
                      ELSE NULL 
                    END) fri_sales, 
                Sum(CASE 
                      WHEN ( d_day_name = 'Saturday' ) THEN ss_sales_price 
                      ELSE NULL 
                    END) sat_sales 
         FROM   store_sales, 
                date_dim 
         WHERE  d_date_sk = ss_sold_date_sk 
         GROUP  BY d_week_seq, 
                   ss_store_sk) 
SELECT s_store_name1, 
               s_store_id1, 
               d_week_seq1, 
               sun_sales1 / sun_sales2, 
               mon_sales1 / mon_sales2, 
               tue_sales1 / tue_sales2, 
               wed_sales1 / wed_sales2, 
               thu_sales1 / thu_sales2, 
               fri_sales1 / fri_sales2, 
               sat_sales1 / sat_sales2 
FROM   (SELECT s_store_name   s_store_name1, 
               wss.d_week_seq d_week_seq1, 
               s_store_id     s_store_id1, 
               sun_sales      sun_sales1, 
               mon_sales      mon_sales1, 
               tue_sales      tue_sales1, 
               wed_sales      wed_sales1, 
               thu_sales      thu_sales1, 
               fri_sales      fri_sales1, 
               sat_sales      sat_sales1 
        FROM   wss, 
               store, 
               date_dim d 
        WHERE  d.d_week_seq = wss.d_week_seq 
               AND ss_store_sk = s_store_sk 
               AND d_month_seq BETWEEN 1196 AND 1196 + 11) y, 
       (SELECT s_store_name   s_store_name2, 
               wss.d_week_seq d_week_seq2, 
               s_store_id     s_store_id2, 
               sun_sales      sun_sales2, 
               mon_sales      mon_sales2, 
               tue_sales      tue_sales2, 
               wed_sales      wed_sales2, 
               thu_sales      thu_sales2, 
               fri_sales      fri_sales2, 
               sat_sales      sat_sales2 
        FROM   wss, 
               store, 
               date_dim d 
        WHERE  d.d_week_seq = wss.d_week_seq 
               AND ss_store_sk = s_store_sk 
               AND d_month_seq BETWEEN 1196 + 12 AND 1196 + 23) x 
WHERE  s_store_id1 = s_store_id2 
       AND d_week_seq1 = d_week_seq2 - 52 
ORDER  BY s_store_name1, 
          s_store_id1, 
          d_week_seq1
LIMIT 100;


-- start query 60 in stream 0 using template query60.tpl 
WITH ss 
     AS (SELECT i_item_id, 
                Sum(ss_ext_sales_price) total_sales 
         FROM   store_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_category IN ( 'Jewelry' )) 
                AND ss_item_sk = i_item_sk 
                AND ss_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 8 
                AND ss_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id), 
     cs 
     AS (SELECT i_item_id, 
                Sum(cs_ext_sales_price) total_sales 
         FROM   catalog_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_category IN ( 'Jewelry' )) 
                AND cs_item_sk = i_item_sk 
                AND cs_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 8 
                AND cs_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id), 
     ws 
     AS (SELECT i_item_id, 
                Sum(ws_ext_sales_price) total_sales 
         FROM   web_sales, 
                date_dim, 
                customer_address, 
                item 
         WHERE  i_item_id IN (SELECT i_item_id 
                              FROM   item 
                              WHERE  i_category IN ( 'Jewelry' )) 
                AND ws_item_sk = i_item_sk 
                AND ws_sold_date_sk = d_date_sk 
                AND d_year = 1999 
                AND d_moy = 8 
                AND ws_bill_addr_sk = ca_address_sk 
                AND ca_gmt_offset = -6 
         GROUP  BY i_item_id) 
SELECT i_item_id, 
               Sum(total_sales) total_sales 
FROM   (SELECT * 
        FROM   ss 
        UNION ALL 
        SELECT * 
        FROM   cs 
        UNION ALL 
        SELECT * 
        FROM   ws) tmp1 
GROUP  BY i_item_id 
ORDER  BY i_item_id, 
          total_sales
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 67 and QUERY_ID < 73);

-- start query 61 in stream 0 using template query61.tpl 
SELECT promotions, 
               total, 
               Cast(promotions AS DECIMAL(15, 4)) / 
               Cast(total AS DECIMAL(15, 4)) * 100 
FROM   (SELECT Sum(ss_ext_sales_price) promotions 
        FROM   store_sales, 
               store, 
               promotion, 
               date_dim, 
               customer, 
               customer_address, 
               item 
        WHERE  ss_sold_date_sk = d_date_sk 
               AND ss_store_sk = s_store_sk 
               AND ss_promo_sk = p_promo_sk 
               AND ss_customer_sk = c_customer_sk 
               AND ca_address_sk = c_current_addr_sk 
               AND ss_item_sk = i_item_sk 
               AND ca_gmt_offset = -7 
               AND i_category = 'Books' 
               AND ( p_channel_dmail = 'Y' 
                      OR p_channel_email = 'Y' 
                      OR p_channel_tv = 'Y' ) 
               AND s_gmt_offset = -7 
               AND d_year = 2001 
               AND d_moy = 12) promotional_sales, 
       (SELECT Sum(ss_ext_sales_price) total 
        FROM   store_sales, 
               store, 
               date_dim, 
               customer, 
               customer_address, 
               item 
        WHERE  ss_sold_date_sk = d_date_sk 
               AND ss_store_sk = s_store_sk 
               AND ss_customer_sk = c_customer_sk 
               AND ca_address_sk = c_current_addr_sk 
               AND ss_item_sk = i_item_sk 
               AND ca_gmt_offset = -7 
               AND i_category = 'Books' 
               AND s_gmt_offset = -7 
               AND d_year = 2001 
               AND d_moy = 12) all_sales 
ORDER  BY promotions, 
          total
LIMIT 100;


-- start query 62 in stream 0 using template query62.tpl 
SELECT Substr(w_warehouse_name, 1, 20), 
               sm_type, 
               web_name, 
               Sum(CASE 
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk <= 30 ) THEN 1 
                     ELSE 0 
                   END) AS `30 days`, 
               Sum(CASE 
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 30 ) 
                          AND ( ws_ship_date_sk - ws_sold_date_sk <= 60 ) THEN 1 
                     ELSE 0 
                   END) AS `31-60 days`, 
               Sum(CASE 
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 60 ) 
                          AND ( ws_ship_date_sk - ws_sold_date_sk <= 90 ) THEN 1 
                     ELSE 0 
                   END) AS `61-90 days`, 
               Sum(CASE 
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 90 ) 
                          AND ( ws_ship_date_sk - ws_sold_date_sk <= 120 ) THEN 
                     1 
                     ELSE 0 
                   END) AS `91-120 days`, 
               Sum(CASE 
                     WHEN ( ws_ship_date_sk - ws_sold_date_sk > 120 ) THEN 1 
                     ELSE 0 
                   END) AS `>120 days` 
FROM   web_sales, 
       warehouse, 
       ship_mode, 
       web_site, 
       date_dim 
WHERE  d_month_seq BETWEEN 1222 AND 1222 + 11 
       AND ws_ship_date_sk = d_date_sk 
       AND ws_warehouse_sk = w_warehouse_sk 
       AND ws_ship_mode_sk = sm_ship_mode_sk 
       AND ws_web_site_sk = web_site_sk 
GROUP  BY Substr(w_warehouse_name, 1, 20), 
          sm_type, 
          web_name 
ORDER  BY Substr(w_warehouse_name, 1, 20), 
          sm_type, 
          web_name
LIMIT 100;


-- start query 63 in stream 0 using template query63.tpl 
SELECT * 
FROM   (SELECT i_manager_id, 
               Sum(ss_sales_price)            sum_sales, 
               Avg(Sum(ss_sales_price)) 
                 OVER ( 
                   partition BY i_manager_id) avg_monthly_sales 
        FROM   item, 
               store_sales, 
               date_dim, 
               store 
        WHERE  ss_item_sk = i_item_sk 
               AND ss_sold_date_sk = d_date_sk 
               AND ss_store_sk = s_store_sk 
               AND d_month_seq IN ( 1200, 1200 + 1, 1200 + 2, 1200 + 3, 
                                    1200 + 4, 1200 + 5, 1200 + 6, 1200 + 7, 
                                    1200 + 8, 1200 + 9, 1200 + 10, 1200 + 11 ) 
               AND ( ( i_category IN ( 'Books', 'Children', 'Electronics' ) 
                       AND i_class IN ( 'personal', 'portable', 'reference', 
                                        'self-help' ) 
                       AND i_brand IN ( 'scholaramalgamalg #14', 
                                        'scholaramalgamalg #7' 
                                        , 
                                        'exportiunivamalg #9', 
                                                       'scholaramalgamalg #9' ) 
                     ) 
                      OR ( i_category IN ( 'Women', 'Music', 'Men' ) 
                           AND i_class IN ( 'accessories', 'classical', 
                                            'fragrances', 
                                            'pants' ) 
                           AND i_brand IN ( 'amalgimporto #1', 
                                            'edu packscholar #1', 
                                            'exportiimporto #1', 
                                                'importoamalg #1' ) ) ) 
        GROUP  BY i_manager_id, 
                  d_moy) tmp1 
WHERE  CASE 
         WHEN avg_monthly_sales > 0 THEN Abs (sum_sales - avg_monthly_sales) / 
                                         avg_monthly_sales 
         ELSE NULL 
       END > 0.1 
ORDER  BY i_manager_id, 
          avg_monthly_sales, 
          sum_sales
LIMIT 100;


-- start query 64 in stream 0 using template query64.tpl 
WITH cs_ui 
     AS (SELECT cs_item_sk, 
                Sum(cs_ext_list_price) AS sale, 
                Sum(cr_refunded_cash + cr_reversed_charge 
                    + cr_store_credit) AS refund 
         FROM   catalog_sales, 
                catalog_returns 
         WHERE  cs_item_sk = cr_item_sk 
                AND cs_order_number = cr_order_number 
         GROUP  BY cs_item_sk 
         HAVING Sum(cs_ext_list_price) > 2 * Sum( 
                cr_refunded_cash + cr_reversed_charge 
                + cr_store_credit)), 
     cross_sales 
     AS (SELECT i_product_name         product_name, 
                i_item_sk              item_sk, 
                s_store_name           store_name, 
                s_zip                  store_zip, 
                ad1.ca_street_number   b_street_number, 
                ad1.ca_street_name     b_streen_name, 
                ad1.ca_city            b_city, 
                ad1.ca_zip             b_zip, 
                ad2.ca_street_number   c_street_number, 
                ad2.ca_street_name     c_street_name, 
                ad2.ca_city            c_city, 
                ad2.ca_zip             c_zip, 
                d1.d_year              AS syear, 
                d2.d_year              AS fsyear, 
                d3.d_year              s2year, 
                Count(*)               cnt, 
                Sum(ss_wholesale_cost) s1, 
                Sum(ss_list_price)     s2, 
                Sum(ss_coupon_amt)     s3 
         FROM   store_sales, 
                store_returns, 
                cs_ui, 
                date_dim d1, 
                date_dim d2, 
                date_dim d3, 
                store, 
                customer, 
                customer_demographics cd1, 
                customer_demographics cd2, 
                promotion, 
                household_demographics hd1, 
                household_demographics hd2, 
                customer_address ad1, 
                customer_address ad2, 
                income_band ib1, 
                income_band ib2, 
                item 
         WHERE  ss_store_sk = s_store_sk 
                AND ss_sold_date_sk = d1.d_date_sk 
                AND ss_customer_sk = c_customer_sk 
                AND ss_cdemo_sk = cd1.cd_demo_sk 
                AND ss_hdemo_sk = hd1.hd_demo_sk 
                AND ss_addr_sk = ad1.ca_address_sk 
                AND ss_item_sk = i_item_sk 
                AND ss_item_sk = sr_item_sk 
                AND ss_ticket_number = sr_ticket_number 
                AND ss_item_sk = cs_ui.cs_item_sk 
                AND c_current_cdemo_sk = cd2.cd_demo_sk 
                AND c_current_hdemo_sk = hd2.hd_demo_sk 
                AND c_current_addr_sk = ad2.ca_address_sk 
                AND c_first_sales_date_sk = d2.d_date_sk 
                AND c_first_shipto_date_sk = d3.d_date_sk 
                AND ss_promo_sk = p_promo_sk 
                AND hd1.hd_income_band_sk = ib1.ib_income_band_sk 
                AND hd2.hd_income_band_sk = ib2.ib_income_band_sk 
                AND cd1.cd_marital_status <> cd2.cd_marital_status 
                AND i_color IN ( 'cyan', 'peach', 'blush', 'frosted', 
                                 'powder', 'orange' ) 
                AND i_current_price BETWEEN 58 AND 58 + 10 
                AND i_current_price BETWEEN 58 + 1 AND 58 + 15 
         GROUP  BY i_product_name, 
                   i_item_sk, 
                   s_store_name, 
                   s_zip, 
                   ad1.ca_street_number, 
                   ad1.ca_street_name, 
                   ad1.ca_city, 
                   ad1.ca_zip, 
                   ad2.ca_street_number, 
                   ad2.ca_street_name, 
                   ad2.ca_city, 
                   ad2.ca_zip, 
                   d1.d_year, 
                   d2.d_year, 
                   d3.d_year) 
SELECT cs1.product_name, 
       cs1.store_name, 
       cs1.store_zip, 
       cs1.b_street_number, 
       cs1.b_streen_name, 
       cs1.b_city, 
       cs1.b_zip, 
       cs1.c_street_number, 
       cs1.c_street_name, 
       cs1.c_city, 
       cs1.c_zip, 
       cs1.syear, 
       cs1.cnt, 
       cs1.s1, 
       cs1.s2, 
       cs1.s3, 
       cs2.s1, 
       cs2.s2, 
       cs2.s3, 
       cs2.syear, 
       cs2.cnt 
FROM   cross_sales cs1, 
       cross_sales cs2 
WHERE  cs1.item_sk = cs2.item_sk 
       AND cs1.syear = 2001 
       AND cs2.syear = 2001 + 1 
       AND cs2.cnt <= cs1.cnt 
       AND cs1.store_name = cs2.store_name 
       AND cs1.store_zip = cs2.store_zip 
ORDER  BY cs1.product_name, 
          cs1.store_name, 
          cs2.cnt;


-- start query 65 in stream 0 using template query65.tpl 
SELECT s_store_name, 
               i_item_desc, 
               sc.revenue, 
               i_current_price, 
               i_wholesale_cost, 
               i_brand 
FROM   store, 
       item, 
       (SELECT ss_store_sk, 
               Avg(revenue) AS ave 
        FROM   (SELECT ss_store_sk, 
                       ss_item_sk, 
                       Sum(ss_sales_price) AS revenue 
                FROM   store_sales, 
                       date_dim 
                WHERE  ss_sold_date_sk = d_date_sk 
                       AND d_month_seq BETWEEN 1199 AND 1199 + 11 
                GROUP  BY ss_store_sk, 
                          ss_item_sk) sa 
        GROUP  BY ss_store_sk) sb, 
       (SELECT ss_store_sk, 
               ss_item_sk, 
               Sum(ss_sales_price) AS revenue 
        FROM   store_sales, 
               date_dim 
        WHERE  ss_sold_date_sk = d_date_sk 
               AND d_month_seq BETWEEN 1199 AND 1199 + 11 
        GROUP  BY ss_store_sk, 
                  ss_item_sk) sc 
WHERE  sb.ss_store_sk = sc.ss_store_sk 
       AND sc.revenue <= 0.1 * sb.ave 
       AND s_store_sk = sc.ss_store_sk 
       AND i_item_sk = sc.ss_item_sk 
ORDER  BY s_store_name, 
          i_item_desc
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 73 and QUERY_ID < 79);

-- start query 66 in stream 0 using template query66.tpl 
SELECT w_warehouse_name, 
               w_warehouse_sq_ft, 
               w_city, 
               w_county, 
               w_state, 
               w_country, 
               ship_carriers, 
               year1,
               Sum(jan_sales)                     AS jan_sales, 
               Sum(feb_sales)                     AS feb_sales, 
               Sum(mar_sales)                     AS mar_sales, 
               Sum(apr_sales)                     AS apr_sales, 
               Sum(may_sales)                     AS may_sales, 
               Sum(jun_sales)                     AS jun_sales, 
               Sum(jul_sales)                     AS jul_sales, 
               Sum(aug_sales)                     AS aug_sales, 
               Sum(sep_sales)                     AS sep_sales, 
               Sum(oct_sales)                     AS oct_sales, 
               Sum(nov_sales)                     AS nov_sales, 
               Sum(dec_sales)                     AS dec_sales, 
               Sum(jan_sales / w_warehouse_sq_ft) AS jan_sales_per_sq_foot, 
               Sum(feb_sales / w_warehouse_sq_ft) AS feb_sales_per_sq_foot, 
               Sum(mar_sales / w_warehouse_sq_ft) AS mar_sales_per_sq_foot, 
               Sum(apr_sales / w_warehouse_sq_ft) AS apr_sales_per_sq_foot, 
               Sum(may_sales / w_warehouse_sq_ft) AS may_sales_per_sq_foot, 
               Sum(jun_sales / w_warehouse_sq_ft) AS jun_sales_per_sq_foot, 
               Sum(jul_sales / w_warehouse_sq_ft) AS jul_sales_per_sq_foot, 
               Sum(aug_sales / w_warehouse_sq_ft) AS aug_sales_per_sq_foot, 
               Sum(sep_sales / w_warehouse_sq_ft) AS sep_sales_per_sq_foot, 
               Sum(oct_sales / w_warehouse_sq_ft) AS oct_sales_per_sq_foot, 
               Sum(nov_sales / w_warehouse_sq_ft) AS nov_sales_per_sq_foot, 
               Sum(dec_sales / w_warehouse_sq_ft) AS dec_sales_per_sq_foot, 
               Sum(jan_net)                       AS jan_net, 
               Sum(feb_net)                       AS feb_net, 
               Sum(mar_net)                       AS mar_net, 
               Sum(apr_net)                       AS apr_net, 
               Sum(may_net)                       AS may_net, 
               Sum(jun_net)                       AS jun_net, 
               Sum(jul_net)                       AS jul_net, 
               Sum(aug_net)                       AS aug_net, 
               Sum(sep_net)                       AS sep_net, 
               Sum(oct_net)                       AS oct_net, 
               Sum(nov_net)                       AS nov_net, 
               Sum(dec_net)                       AS dec_net 
FROM   (SELECT w_warehouse_name, 
               w_warehouse_sq_ft, 
               w_city, 
               w_county, 
               w_state, 
               w_country, 
               'ZOUROS' 
               || ',' 
               || 'ZHOU' AS ship_carriers, 
               d_year    AS year1, 
               Sum(CASE 
                     WHEN d_moy = 1 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS jan_sales, 
               Sum(CASE 
                     WHEN d_moy = 2 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS feb_sales, 
               Sum(CASE 
                     WHEN d_moy = 3 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS mar_sales, 
               Sum(CASE 
                     WHEN d_moy = 4 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS apr_sales, 
               Sum(CASE 
                     WHEN d_moy = 5 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS may_sales, 
               Sum(CASE 
                     WHEN d_moy = 6 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS jun_sales, 
               Sum(CASE 
                     WHEN d_moy = 7 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS jul_sales, 
               Sum(CASE 
                     WHEN d_moy = 8 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS aug_sales, 
               Sum(CASE 
                     WHEN d_moy = 9 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS sep_sales, 
               Sum(CASE 
                     WHEN d_moy = 10 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS oct_sales, 
               Sum(CASE 
                     WHEN d_moy = 11 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS nov_sales, 
               Sum(CASE 
                     WHEN d_moy = 12 THEN ws_ext_sales_price * ws_quantity 
                     ELSE 0 
                   END)  AS dec_sales, 
               Sum(CASE 
                     WHEN d_moy = 1 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS jan_net, 
               Sum(CASE 
                     WHEN d_moy = 2 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS feb_net, 
               Sum(CASE 
                     WHEN d_moy = 3 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS mar_net, 
               Sum(CASE 
                     WHEN d_moy = 4 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS apr_net, 
               Sum(CASE 
                     WHEN d_moy = 5 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS may_net, 
               Sum(CASE 
                     WHEN d_moy = 6 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS jun_net, 
               Sum(CASE 
                     WHEN d_moy = 7 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS jul_net, 
               Sum(CASE 
                     WHEN d_moy = 8 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS aug_net, 
               Sum(CASE 
                     WHEN d_moy = 9 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS sep_net, 
               Sum(CASE 
                     WHEN d_moy = 10 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS oct_net, 
               Sum(CASE 
                     WHEN d_moy = 11 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS nov_net, 
               Sum(CASE 
                     WHEN d_moy = 12 THEN ws_net_paid_inc_ship * ws_quantity 
                     ELSE 0 
                   END)  AS dec_net 
        FROM   web_sales, 
               warehouse, 
               date_dim, 
               time_dim, 
               ship_mode 
        WHERE  ws_warehouse_sk = w_warehouse_sk 
               AND ws_sold_date_sk = d_date_sk 
               AND ws_sold_time_sk = t_time_sk 
               AND ws_ship_mode_sk = sm_ship_mode_sk 
               AND d_year = 1998 
               AND t_time BETWEEN 7249 AND 7249 + 28800 
               AND sm_carrier IN ( 'ZOUROS', 'ZHOU' ) 
        GROUP  BY w_warehouse_name, 
                  w_warehouse_sq_ft, 
                  w_city, 
                  w_county, 
                  w_state, 
                  w_country, 
                  d_year 
        UNION ALL 
        SELECT w_warehouse_name, 
               w_warehouse_sq_ft, 
               w_city, 
               w_county, 
               w_state, 
               w_country, 
               'ZOUROS' 
               || ',' 
               || 'ZHOU' AS ship_carriers, 
               d_year    AS year1, 
               Sum(CASE 
                     WHEN d_moy = 1 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS jan_sales, 
               Sum(CASE 
                     WHEN d_moy = 2 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS feb_sales, 
               Sum(CASE 
                     WHEN d_moy = 3 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS mar_sales, 
               Sum(CASE 
                     WHEN d_moy = 4 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS apr_sales, 
               Sum(CASE 
                     WHEN d_moy = 5 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS may_sales, 
               Sum(CASE 
                     WHEN d_moy = 6 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS jun_sales, 
               Sum(CASE 
                     WHEN d_moy = 7 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS jul_sales, 
               Sum(CASE 
                     WHEN d_moy = 8 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS aug_sales, 
               Sum(CASE 
                     WHEN d_moy = 9 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS sep_sales, 
               Sum(CASE 
                     WHEN d_moy = 10 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS oct_sales, 
               Sum(CASE 
                     WHEN d_moy = 11 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS nov_sales, 
               Sum(CASE 
                     WHEN d_moy = 12 THEN cs_ext_sales_price * cs_quantity 
                     ELSE 0 
                   END)  AS dec_sales, 
               Sum(CASE 
                     WHEN d_moy = 1 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS jan_net, 
               Sum(CASE 
                     WHEN d_moy = 2 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS feb_net, 
               Sum(CASE 
                     WHEN d_moy = 3 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS mar_net, 
               Sum(CASE 
                     WHEN d_moy = 4 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS apr_net, 
               Sum(CASE 
                     WHEN d_moy = 5 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS may_net, 
               Sum(CASE 
                     WHEN d_moy = 6 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS jun_net, 
               Sum(CASE 
                     WHEN d_moy = 7 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS jul_net, 
               Sum(CASE 
                     WHEN d_moy = 8 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS aug_net, 
               Sum(CASE 
                     WHEN d_moy = 9 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS sep_net, 
               Sum(CASE 
                     WHEN d_moy = 10 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS oct_net, 
               Sum(CASE 
                     WHEN d_moy = 11 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS nov_net, 
               Sum(CASE 
                     WHEN d_moy = 12 THEN cs_net_paid * cs_quantity 
                     ELSE 0 
                   END)  AS dec_net 
        FROM   catalog_sales, 
               warehouse, 
               date_dim, 
               time_dim, 
               ship_mode 
        WHERE  cs_warehouse_sk = w_warehouse_sk 
               AND cs_sold_date_sk = d_date_sk 
               AND cs_sold_time_sk = t_time_sk 
               AND cs_ship_mode_sk = sm_ship_mode_sk 
               AND d_year = 1998 
               AND t_time BETWEEN 7249 AND 7249 + 28800 
               AND sm_carrier IN ( 'ZOUROS', 'ZHOU' ) 
        GROUP  BY w_warehouse_name, 
                  w_warehouse_sq_ft, 
                  w_city, 
                  w_county, 
                  w_state, 
                  w_country, 
                  d_year) x 
GROUP  BY w_warehouse_name, 
          w_warehouse_sq_ft, 
          w_city, 
          w_county, 
          w_state, 
          w_country, 
          ship_carriers, 
          year1 
ORDER  BY w_warehouse_name
LIMIT 100;

-- 67
SELECT *
FROM (
    SELECT
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy,
        d_moy,
        s_store_id,
        sumsales,
        RANK() OVER (PARTITION BY i_category ORDER BY sumsales DESC) AS rk
    FROM (
        SELECT
            i_category,
            i_class,
            i_brand,
            i_product_name,
            d_year,
            d_qoy,
            d_moy,
            s_store_id,
            SUM(COALESCE(ss_sales_price * ss_quantity, 0)) AS sumsales
        FROM
            store_sales
            JOIN date_dim ON ss_sold_date_sk = d_date_sk
            JOIN store ON ss_store_sk = s_store_sk
            JOIN item ON ss_item_sk = i_item_sk
        WHERE
            d_month_seq BETWEEN 1181 AND 1181 + 11
        GROUP BY
            i_category,
            i_class,
            i_brand,
            i_product_name,
            d_year,
            d_qoy,
            d_moy,
            s_store_id
    ) dw1
) dw2
WHERE
    rk <= 100
ORDER BY
    i_category,
    i_class,
    i_brand,
    i_product_name,
    d_year,
    d_qoy,
    d_moy,
    s_store_id,
    sumsales,
    rk;


-- start query 68 in stream 0 using template query68.tpl 
SELECT c_last_name, 
               c_first_name, 
               ca_city, 
               bought_city, 
               ss_ticket_number, 
               extended_price, 
               extended_tax, 
               list_price 
FROM   (SELECT ss_ticket_number, 
               ss_customer_sk, 
               ca_city                 bought_city, 
               Sum(ss_ext_sales_price) extended_price, 
               Sum(ss_ext_list_price)  list_price, 
               Sum(ss_ext_tax)         extended_tax 
        FROM   store_sales, 
               date_dim, 
               store, 
               household_demographics, 
               customer_address 
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk 
               AND store_sales.ss_store_sk = store.s_store_sk 
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk 
               AND store_sales.ss_addr_sk = customer_address.ca_address_sk 
               AND date_dim.d_dom BETWEEN 1 AND 2 
               AND ( household_demographics.hd_dep_count = 8 
                      OR household_demographics.hd_vehicle_count = 3 ) 
               AND date_dim.d_year IN ( 1998, 1998 + 1, 1998 + 2 ) 
               AND store.s_city IN ( 'Fairview', 'Midway' ) 
        GROUP  BY ss_ticket_number, 
                  ss_customer_sk, 
                  ss_addr_sk, 
                  ca_city) dn, 
       customer, 
       customer_address current_addr 
WHERE  ss_customer_sk = c_customer_sk 
       AND customer.c_current_addr_sk = current_addr.ca_address_sk 
       AND current_addr.ca_city <> bought_city 
ORDER  BY c_last_name, 
          ss_ticket_number
LIMIT 100;

-- 69

SELECT cd_gender, 
               cd_marital_status, 
               cd_education_status, 
               Count(*) cnt1, 
               cd_purchase_estimate, 
               Count(*) cnt2, 
               cd_credit_rating, 
               Count(*) cnt3 
FROM   customer c, 
       customer_address ca, 
       customer_demographics 
WHERE  c.c_current_addr_sk = ca.ca_address_sk 
       AND ca_state IN ( 'KS', 'AZ', 'NE' ) 
       AND cd_demo_sk = c.c_current_cdemo_sk 
       AND EXISTS (SELECT * 
                   FROM   store_sales, 
                          date_dim 
                   WHERE  c.c_customer_sk = ss_customer_sk 
                          AND ss_sold_date_sk = d_date_sk 
                          AND d_year = 2004 
                          AND d_moy BETWEEN 3 AND 3 + 2) 
       AND ( NOT EXISTS (SELECT * 
                         FROM   web_sales, 
                                date_dim 
                         WHERE  c.c_customer_sk = ws_bill_customer_sk 
                                AND ws_sold_date_sk = d_date_sk 
                                AND d_year = 2004 
                                AND d_moy BETWEEN 3 AND 3 + 2) 
             AND NOT EXISTS (SELECT * 
                             FROM   catalog_sales, 
                                    date_dim 
                             WHERE  c.c_customer_sk = cs_ship_customer_sk 
                                    AND cs_sold_date_sk = d_date_sk 
                                    AND d_year = 2004 
                                    AND d_moy BETWEEN 3 AND 3 + 2) ) 
GROUP  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating 
ORDER  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating
LIMIT 100;


-- start query 70 in stream 0 using template query70.tpl 
SELECT
    total_sum,
    s_state,
    s_county,
    lochierarchy,
    rank_within_parent
FROM (
    SELECT
        SUM(ss_net_profit) AS total_sum,
        s_state,
        s_county,
        (GROUPING(s_state) + GROUPING(s_county)) AS lochierarchy,
        RANK() OVER (
            PARTITION BY GROUPING(s_state) + GROUPING(s_county), CASE WHEN GROUPING(s_county) = 0 THEN s_state END
            ORDER BY SUM(ss_net_profit) DESC
        ) AS rank_within_parent
    FROM
        store_sales
        JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
        JOIN store ON s_store_sk = ss_store_sk
    WHERE
        d1.d_month_seq BETWEEN 1200 AND 1200 + 11
        AND s_state IN (
            SELECT s_state
            FROM (
                SELECT
                    s_state,
                    RANK() OVER (PARTITION BY s_state ORDER BY SUM(ss_net_profit) DESC) AS ranking
                FROM
                    store_sales
                    JOIN store ON s_store_sk = ss_store_sk
                    JOIN date_dim ON d_date_sk = ss_sold_date_sk
                WHERE
                    d_month_seq BETWEEN 1200 AND 1200 + 11
                GROUP BY
                    s_state
            ) tmp1
            WHERE
                ranking <= 5
        )
    GROUP BY
        s_state, s_county WITH ROLLUP
) AS results
ORDER BY
    lochierarchy DESC,
    CASE WHEN lochierarchy = 0 THEN s_state END,
    rank_within_parent
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 79 and QUERY_ID < 85);

-- start query 71 in stream 0 using template query71.tpl 
SELECT i_brand_id     brand_id, 
       i_brand        brand, 
       t_hour, 
       t_minute, 
       Sum(ext_price) ext_price 
FROM   item, 
       (SELECT ws_ext_sales_price AS ext_price, 
               ws_sold_date_sk    AS sold_date_sk, 
               ws_item_sk         AS sold_item_sk, 
               ws_sold_time_sk    AS time_sk 
        FROM   web_sales, 
               date_dim 
        WHERE  d_date_sk = ws_sold_date_sk 
               AND d_moy = 11 
               AND d_year = 2001 
        UNION ALL 
        SELECT cs_ext_sales_price AS ext_price, 
               cs_sold_date_sk    AS sold_date_sk, 
               cs_item_sk         AS sold_item_sk, 
               cs_sold_time_sk    AS time_sk 
        FROM   catalog_sales, 
               date_dim 
        WHERE  d_date_sk = cs_sold_date_sk 
               AND d_moy = 11 
               AND d_year = 2001 
        UNION ALL 
        SELECT ss_ext_sales_price AS ext_price, 
               ss_sold_date_sk    AS sold_date_sk, 
               ss_item_sk         AS sold_item_sk, 
               ss_sold_time_sk    AS time_sk 
        FROM   store_sales, 
               date_dim 
        WHERE  d_date_sk = ss_sold_date_sk 
               AND d_moy = 11 
               AND d_year = 2001) AS tmp, 
       time_dim 
WHERE  sold_item_sk = i_item_sk 
       AND i_manager_id = 1 
       AND time_sk = t_time_sk 
       AND ( t_meal_time = 'breakfast' 
              OR t_meal_time = 'dinner' ) 
GROUP  BY i_brand, 
          i_brand_id, 
          t_hour, 
          t_minute 
ORDER  BY ext_price DESC, 
          i_brand_id;


-- start query 72 in stream 0 using template query72.tpl 
SELECT i_item_desc, 
               w_warehouse_name, 
               d1.d_week_seq, 
               Sum(CASE 
                     WHEN p_promo_sk IS NULL THEN 1 
                     ELSE 0 
                   END) no_promo, 
               Sum(CASE 
                     WHEN p_promo_sk IS NOT NULL THEN 1 
                     ELSE 0 
                   END) promo, 
               Count(*) total_cnt 
FROM   catalog_sales 
       JOIN inventory 
         ON ( cs_item_sk = inv_item_sk ) 
       JOIN warehouse 
         ON ( w_warehouse_sk = inv_warehouse_sk ) 
       JOIN item 
         ON ( i_item_sk = cs_item_sk ) 
       JOIN customer_demographics 
         ON ( cs_bill_cdemo_sk = cd_demo_sk ) 
       JOIN household_demographics 
         ON ( cs_bill_hdemo_sk = hd_demo_sk ) 
       JOIN date_dim d1 
         ON ( cs_sold_date_sk = d1.d_date_sk ) 
       JOIN date_dim d2 
         ON ( inv_date_sk = d2.d_date_sk ) 
       JOIN date_dim d3 
         ON ( cs_ship_date_sk = d3.d_date_sk ) 
       LEFT OUTER JOIN promotion 
                    ON ( cs_promo_sk = p_promo_sk ) 
       LEFT OUTER JOIN catalog_returns 
                    ON ( cr_item_sk = cs_item_sk 
                         AND cr_order_number = cs_order_number ) 
WHERE  d1.d_week_seq = d2.d_week_seq 
       AND inv_quantity_on_hand < cs_quantity 
       AND d3.d_date > d1.d_date + INTERVAL '5' day 
       AND hd_buy_potential = '501-1000' 
       AND d1.d_year = 2002 
       AND cd_marital_status = 'M' 
GROUP  BY i_item_desc, 
          w_warehouse_name, 
          d1.d_week_seq 
ORDER  BY total_cnt DESC, 
          i_item_desc, 
          w_warehouse_name, 
          d_week_seq
LIMIT 100;


-- start query 73 in stream 0 using template query73.tpl 
SELECT c_last_name, 
       c_first_name, 
       c_salutation, 
       c_preferred_cust_flag, 
       ss_ticket_number, 
       cnt 
FROM   (SELECT ss_ticket_number, 
               ss_customer_sk, 
               Count(*) cnt 
        FROM   store_sales, 
               date_dim, 
               store, 
               household_demographics 
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk 
               AND store_sales.ss_store_sk = store.s_store_sk 
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk 
               AND date_dim.d_dom BETWEEN 1 AND 2 
               AND ( household_demographics.hd_buy_potential = '>10000' 
                      OR household_demographics.hd_buy_potential = '0-500' ) 
               AND household_demographics.hd_vehicle_count > 0 
               AND CASE 
                     WHEN household_demographics.hd_vehicle_count > 0 THEN 
                     household_demographics.hd_dep_count / 
                     household_demographics.hd_vehicle_count 
                     ELSE NULL 
                   END > 1 
               AND date_dim.d_year IN ( 2000, 2000 + 1, 2000 + 2 ) 
               AND store.s_county IN ( 'Williamson County', 'Williamson County', 
                                       'Williamson County', 
                                                             'Williamson County' 
                                     ) 
        GROUP  BY ss_ticket_number, 
                  ss_customer_sk) dj, 
       customer 
WHERE  ss_customer_sk = c_customer_sk 
       AND cnt BETWEEN 1 AND 5 
ORDER  BY cnt DESC, 
          c_last_name ASC;


-- start query 74 in stream 0 using template query74.tpl 
WITH year_total 
     AS (SELECT c_customer_id    customer_id, 
                c_first_name     customer_first_name, 
                c_last_name      customer_last_name, 
                d_year           AS year1, 
                Sum(ss_net_paid) year_total, 
                's'              sale_type 
         FROM   customer, 
                store_sales, 
                date_dim 
         WHERE  c_customer_sk = ss_customer_sk 
                AND ss_sold_date_sk = d_date_sk 
                AND d_year IN ( 1999, 1999 + 1 ) 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   d_year 
         UNION ALL 
         SELECT c_customer_id    customer_id, 
                c_first_name     customer_first_name, 
                c_last_name      customer_last_name, 
                d_year           AS year1, 
                Sum(ws_net_paid) year_total, 
                'w'              sale_type 
         FROM   customer, 
                web_sales, 
                date_dim 
         WHERE  c_customer_sk = ws_bill_customer_sk 
                AND ws_sold_date_sk = d_date_sk 
                AND d_year IN ( 1999, 1999 + 1 ) 
         GROUP  BY c_customer_id, 
                   c_first_name, 
                   c_last_name, 
                   d_year) 
SELECT t_s_secyear.customer_id, 
               t_s_secyear.customer_first_name, 
               t_s_secyear.customer_last_name 
FROM   year_total t_s_firstyear, 
       year_total t_s_secyear, 
       year_total t_w_firstyear, 
       year_total t_w_secyear 
WHERE  t_s_secyear.customer_id = t_s_firstyear.customer_id 
       AND t_s_firstyear.customer_id = t_w_secyear.customer_id 
       AND t_s_firstyear.customer_id = t_w_firstyear.customer_id 
       AND t_s_firstyear.sale_type = 's' 
       AND t_w_firstyear.sale_type = 'w' 
       AND t_s_secyear.sale_type = 's' 
       AND t_w_secyear.sale_type = 'w' 
       AND t_s_firstyear.year1 = 1999 
       AND t_s_secyear.year1 = 1999 + 1 
       AND t_w_firstyear.year1 = 1999 
       AND t_w_secyear.year1 = 1999 + 1 
       AND t_s_firstyear.year_total > 0 
       AND t_w_firstyear.year_total > 0 
       AND CASE 
             WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / 
                                                    t_w_firstyear.year_total 
             ELSE NULL 
           END > CASE 
                   WHEN t_s_firstyear.year_total > 0 THEN 
                   t_s_secyear.year_total / 
                   t_s_firstyear.year_total 
                   ELSE NULL 
                 END 
ORDER  BY 1, 
          2, 
          3
LIMIT 100;


-- start query 75 in stream 0 using template query75.tpl 
WITH all_sales 
     AS (SELECT d_year, 
                i_brand_id, 
                i_class_id, 
                i_category_id, 
                i_manufact_id, 
                Sum(sales_cnt) AS sales_cnt, 
                Sum(sales_amt) AS sales_amt 
         FROM   (SELECT d_year, 
                        i_brand_id, 
                        i_class_id, 
                        i_category_id, 
                        i_manufact_id, 
                        cs_quantity - COALESCE(cr_return_quantity, 0)        AS 
                        sales_cnt, 
                        cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS 
                        sales_amt 
                 FROM   catalog_sales 
                        JOIN item 
                          ON i_item_sk = cs_item_sk 
                        JOIN date_dim 
                          ON d_date_sk = cs_sold_date_sk 
                        LEFT JOIN catalog_returns 
                               ON ( cs_order_number = cr_order_number 
                                    AND cs_item_sk = cr_item_sk ) 
                 WHERE  i_category = 'Men' 
                 UNION 
                 SELECT d_year, 
                        i_brand_id, 
                        i_class_id, 
                        i_category_id, 
                        i_manufact_id, 
                        ss_quantity - COALESCE(sr_return_quantity, 0)     AS 
                        sales_cnt, 
                        ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS 
                        sales_amt 
                 FROM   store_sales 
                        JOIN item 
                          ON i_item_sk = ss_item_sk 
                        JOIN date_dim 
                          ON d_date_sk = ss_sold_date_sk 
                        LEFT JOIN store_returns 
                               ON ( ss_ticket_number = sr_ticket_number 
                                    AND ss_item_sk = sr_item_sk ) 
                 WHERE  i_category = 'Men' 
                 UNION 
                 SELECT d_year, 
                        i_brand_id, 
                        i_class_id, 
                        i_category_id, 
                        i_manufact_id, 
                        ws_quantity - COALESCE(wr_return_quantity, 0)     AS 
                        sales_cnt, 
                        ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS 
                        sales_amt 
                 FROM   web_sales 
                        JOIN item 
                          ON i_item_sk = ws_item_sk 
                        JOIN date_dim 
                          ON d_date_sk = ws_sold_date_sk 
                        LEFT JOIN web_returns 
                               ON ( ws_order_number = wr_order_number 
                                    AND ws_item_sk = wr_item_sk ) 
                 WHERE  i_category = 'Men') sales_detail 
         GROUP  BY d_year, 
                   i_brand_id, 
                   i_class_id, 
                   i_category_id, 
                   i_manufact_id) 
SELECT prev_yr.d_year                        AS prev_year, 
               curr_yr.d_year                        AS year1, 
               curr_yr.i_brand_id, 
               curr_yr.i_class_id, 
               curr_yr.i_category_id, 
               curr_yr.i_manufact_id, 
               prev_yr.sales_cnt                     AS prev_yr_cnt, 
               curr_yr.sales_cnt                     AS curr_yr_cnt, 
               curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff, 
               curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff 
FROM   all_sales curr_yr, 
       all_sales prev_yr 
WHERE  curr_yr.i_brand_id = prev_yr.i_brand_id 
       AND curr_yr.i_class_id = prev_yr.i_class_id 
       AND curr_yr.i_category_id = prev_yr.i_category_id 
       AND curr_yr.i_manufact_id = prev_yr.i_manufact_id 
       AND curr_yr.d_year = 2002 
       AND prev_yr.d_year = 2002 - 1 
       AND Cast(curr_yr.sales_cnt AS DECIMAL(17, 2)) / Cast(prev_yr.sales_cnt AS 
                                                                DECIMAL(17, 2)) 
           < 0.9 
ORDER  BY sales_cnt_diff
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 85 and QUERY_ID < 91);

-- start query 76 in stream 0 using template query76.tpl 
SELECT channel, 
               col_name, 
               d_year, 
               d_qoy, 
               i_category, 
               Count(*)             sales_cnt, 
               Sum(ext_sales_price) sales_amt 
FROM   (SELECT 'store'            AS channel, 
               'ss_hdemo_sk'      col_name, 
               d_year, 
               d_qoy, 
               i_category, 
               ss_ext_sales_price ext_sales_price 
        FROM   store_sales, 
               item, 
               date_dim 
        WHERE  ss_hdemo_sk IS NULL 
               AND ss_sold_date_sk = d_date_sk 
               AND ss_item_sk = i_item_sk 
        UNION ALL 
        SELECT 'web'              AS channel, 
               'ws_ship_hdemo_sk' col_name, 
               d_year, 
               d_qoy, 
               i_category, 
               ws_ext_sales_price ext_sales_price 
        FROM   web_sales, 
               item, 
               date_dim 
        WHERE  ws_ship_hdemo_sk IS NULL 
               AND ws_sold_date_sk = d_date_sk 
               AND ws_item_sk = i_item_sk 
        UNION ALL 
        SELECT 'catalog'          AS channel, 
               'cs_warehouse_sk'  col_name, 
               d_year, 
               d_qoy, 
               i_category, 
               cs_ext_sales_price ext_sales_price 
        FROM   catalog_sales, 
               item, 
               date_dim 
        WHERE  cs_warehouse_sk IS NULL 
               AND cs_sold_date_sk = d_date_sk 
               AND cs_item_sk = i_item_sk) foo 
GROUP  BY channel, 
          col_name, 
          d_year, 
          d_qoy, 
          i_category 
ORDER  BY channel, 
          col_name, 
          d_year, 
          d_qoy, 
          i_category
LIMIT 100;


-- start query 77 in stream 0 using template query77.tpl 
WITH ss AS (
    SELECT
        ss_store_sk,
        SUM(ss_ext_sales_price) AS sales,
        SUM(ss_net_profit) AS profit
    FROM
        store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE
        d_date BETWEEN CAST('2001-08-16' AS DATE) AND (CAST('2001-08-16' AS DATE) + INTERVAL 30 DAY)
    GROUP BY
        ss_store_sk
),
sr AS (
    SELECT
        sr_store_sk,
        SUM(sr_return_amt) AS returns1,
        SUM(sr_net_loss) AS profit_loss
    FROM
        store_returns
        JOIN date_dim ON sr_returned_date_sk = d_date_sk
    WHERE
        d_date BETWEEN CAST('2001-08-16' AS DATE) AND (CAST('2001-08-16' AS DATE) + INTERVAL 30 DAY)
    GROUP BY
        sr_store_sk
),
cs AS (
    SELECT
        cs_call_center_sk,
        SUM(cs_ext_sales_price) AS sales,
        SUM(cs_net_profit) AS profit
    FROM
        catalog_sales
        JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE
        d_date BETWEEN CAST('2001-08-16' AS DATE) AND (CAST('2001-08-16' AS DATE) + INTERVAL 30 DAY)
    GROUP BY
        cs_call_center_sk
),
cr AS (
    SELECT
        cr_call_center_sk,
        SUM(cr_return_amount) AS returns1,
        SUM(cr_net_loss) AS profit_loss
    FROM
        catalog_returns
        JOIN date_dim ON cr_returned_date_sk = d_date_sk
    WHERE
        d_date BETWEEN CAST('2001-08-16' AS DATE) AND (CAST('2001-08-16' AS DATE) + INTERVAL 30 DAY)
    GROUP BY
        cr_call_center_sk
),
ws AS (
    SELECT
        ws_item_sk,
        SUM(ws_ext_sales_price) AS sales,
        SUM(ws_net_profit) AS profit
    FROM
        web_sales
        JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE
        d_date BETWEEN CAST('2001-08-16' AS DATE) AND (CAST('2001-08-16' AS DATE) + INTERVAL 30 DAY)
    GROUP BY
        ws_item_sk
),
wr AS (
    SELECT
        wr_item_sk,
        SUM(wr_return_amt) AS returns1,
        SUM(wr_net_loss) AS profit_loss
    FROM
        web_returns
        JOIN date_dim ON wr_returned_date_sk = d_date_sk
    WHERE
        d_date BETWEEN CAST('2001-08-16' AS DATE) AND (CAST('2001-08-16' AS DATE) + INTERVAL 30 DAY)
    GROUP BY
        wr_item_sk
)
SELECT
    'store channel' AS channel,
    ss.ss_store_sk AS id,
    SUM(sales) AS sales,
    SUM(COALESCE(sr.returns1, 0)) AS returns1,
    SUM(profit - COALESCE(sr.profit_loss, 0)) AS profit
FROM
    ss
    LEFT JOIN sr ON ss.ss_store_sk = sr.sr_store_sk
GROUP BY
    channel, id
UNION ALL
SELECT
    'catalog channel' AS channel,
    cs.cs_call_center_sk AS id,
    SUM(cs.sales) AS sales,
    SUM(cr.returns1) AS returns1,
    SUM(cs.profit - cr.profit_loss) AS profit
FROM
    cs
    JOIN cr ON cs.cs_call_center_sk = cr.cr_call_center_sk
GROUP BY
    channel, id
UNION ALL
SELECT
    'web channel' AS channel,
    ws.ws_item_sk AS id,
    SUM(ws.sales) AS sales,
    SUM(COALESCE(wr.returns1, 0)) AS returns1,
    SUM(ws.profit - COALESCE(wr.profit_loss, 0)) AS profit
FROM
    ws
    LEFT JOIN wr ON ws.ws_item_sk = wr.wr_item_sk
GROUP BY
    channel, id
ORDER BY
    channel,
    id
LIMIT 100;


-- start query 78 in stream 0 using template query78.tpl 
WITH ws 
     AS (SELECT d_year                 AS ws_sold_year, 
                ws_item_sk, 
                ws_bill_customer_sk    ws_customer_sk, 
                Sum(ws_quantity)       ws_qty, 
                Sum(ws_wholesale_cost) ws_wc, 
                Sum(ws_sales_price)    ws_sp 
         FROM   web_sales 
                LEFT JOIN web_returns 
                       ON wr_order_number = ws_order_number 
                          AND ws_item_sk = wr_item_sk 
                JOIN date_dim 
                  ON ws_sold_date_sk = d_date_sk 
         WHERE  wr_order_number IS NULL 
         GROUP  BY d_year, 
                   ws_item_sk, 
                   ws_bill_customer_sk), 
     cs 
     AS (SELECT d_year                 AS cs_sold_year, 
                cs_item_sk, 
                cs_bill_customer_sk    cs_customer_sk, 
                Sum(cs_quantity)       cs_qty, 
                Sum(cs_wholesale_cost) cs_wc, 
                Sum(cs_sales_price)    cs_sp 
         FROM   catalog_sales 
                LEFT JOIN catalog_returns 
                       ON cr_order_number = cs_order_number 
                          AND cs_item_sk = cr_item_sk 
                JOIN date_dim 
                  ON cs_sold_date_sk = d_date_sk 
         WHERE  cr_order_number IS NULL 
         GROUP  BY d_year, 
                   cs_item_sk, 
                   cs_bill_customer_sk), 
     ss 
     AS (SELECT d_year                 AS ss_sold_year, 
                ss_item_sk, 
                ss_customer_sk, 
                Sum(ss_quantity)       ss_qty, 
                Sum(ss_wholesale_cost) ss_wc, 
                Sum(ss_sales_price)    ss_sp 
         FROM   store_sales 
                LEFT JOIN store_returns 
                       ON sr_ticket_number = ss_ticket_number 
                          AND ss_item_sk = sr_item_sk 
                JOIN date_dim 
                  ON ss_sold_date_sk = d_date_sk 
         WHERE  sr_ticket_number IS NULL 
         GROUP  BY d_year, 
                   ss_item_sk, 
                   ss_customer_sk) 
SELECT ss_item_sk, 
               Round(ss_qty / ( COALESCE(ws_qty + cs_qty, 1) ), 2) ratio, 
               ss_qty                                              store_qty, 
               ss_wc 
               store_wholesale_cost, 
               ss_sp 
               store_sales_price, 
               COALESCE(ws_qty, 0) + COALESCE(cs_qty, 0) 
               other_chan_qty, 
               COALESCE(ws_wc, 0) + COALESCE(cs_wc, 0) 
               other_chan_wholesale_cost, 
               COALESCE(ws_sp, 0) + COALESCE(cs_sp, 0) 
               other_chan_sales_price 
FROM   ss 
       LEFT JOIN ws 
              ON ( ws_sold_year = ss_sold_year 
                   AND ws_item_sk = ss_item_sk 
                   AND ws_customer_sk = ss_customer_sk ) 
       LEFT JOIN cs 
              ON ( cs_sold_year = ss_sold_year 
                   AND cs_item_sk = cs_item_sk 
                   AND cs_customer_sk = ss_customer_sk ) 
WHERE  COALESCE(ws_qty, 0) > 0 
       AND COALESCE(cs_qty, 0) > 0 
       AND ss_sold_year = 1999 
ORDER  BY ss_item_sk, 
          ss_qty DESC, 
          ss_wc DESC, 
          ss_sp DESC, 
          other_chan_qty, 
          other_chan_wholesale_cost, 
          other_chan_sales_price, 
          Round(ss_qty / ( COALESCE(ws_qty + cs_qty, 1) ), 2)
LIMIT 100;


-- start query 79 in stream 0 using template query79.tpl 
SELECT c_last_name, 
               c_first_name, 
               Substr(s_city, 1, 30), 
               ss_ticket_number, 
               amt, 
               profit 
FROM   (SELECT ss_ticket_number, 
               ss_customer_sk, 
               store.s_city, 
               Sum(ss_coupon_amt) amt, 
               Sum(ss_net_profit) profit 
        FROM   store_sales, 
               date_dim, 
               store, 
               household_demographics 
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk 
               AND store_sales.ss_store_sk = store.s_store_sk 
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk 
               AND ( household_demographics.hd_dep_count = 8 
                      OR household_demographics.hd_vehicle_count > 4 ) 
               AND date_dim.d_dow = 1 
               AND date_dim.d_year IN ( 2000, 2000 + 1, 2000 + 2 ) 
               AND store.s_number_employees BETWEEN 200 AND 295 
        GROUP  BY ss_ticket_number, 
                  ss_customer_sk, 
                  ss_addr_sk, 
                  store.s_city) ms, 
       customer 
WHERE  ss_customer_sk = c_customer_sk 
ORDER  BY c_last_name, 
          c_first_name, 
          Substr(s_city, 1, 30), 
          profit
LIMIT 100;


-- start query 80 in stream 0 using template query80.tpl 
WITH ssr AS (
    SELECT
        s_store_id AS store_id,
        SUM(ss_ext_sales_price) AS sales,
        SUM(COALESCE(sr_return_amt, 0)) AS returns1,
        SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit
    FROM
        store_sales
        LEFT OUTER JOIN store_returns ON (ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number),
        date_dim,
        store,
        item,
        promotion
    WHERE
        ss_sold_date_sk = d_date_sk
        AND d_date BETWEEN CAST('2000-08-26' AS DATE) AND (CAST('2000-08-26' AS DATE) + INTERVAL '30' DAY)
        AND ss_store_sk = s_store_sk
        AND ss_item_sk = i_item_sk
        AND i_current_price > 50
        AND ss_promo_sk = p_promo_sk
        AND p_channel_tv = 'N'
    GROUP BY
        s_store_id
), csr AS (
    SELECT
        cp_catalog_page_id AS catalog_page_id,
        SUM(cs_ext_sales_price) AS sales,
        SUM(COALESCE(cr_return_amount, 0)) AS returns1,
        SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit
    FROM
        catalog_sales
        LEFT OUTER JOIN catalog_returns ON (cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number),
        date_dim,
        catalog_page,
        item,
        promotion
    WHERE
        cs_sold_date_sk = d_date_sk
        AND d_date BETWEEN CAST('2000-08-26' AS DATE) AND (CAST('2000-08-26' AS DATE) + INTERVAL '30' DAY)
        AND cs_catalog_page_sk = cp_catalog_page_sk
        AND cs_item_sk = i_item_sk
        AND i_current_price > 50
        AND cs_promo_sk = p_promo_sk
        AND p_channel_tv = 'N'
    GROUP BY
        cp_catalog_page_id
), wsr AS (
    SELECT
        web_site_id,
        SUM(ws_ext_sales_price) AS sales,
        SUM(COALESCE(wr_return_amt, 0)) AS returns1,
        SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit
    FROM
        web_sales
        LEFT OUTER JOIN web_returns ON (ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number),
        date_dim,
        web_site,
        item,
        promotion
    WHERE
        ws_sold_date_sk = d_date_sk
        AND d_date BETWEEN CAST('2000-08-26' AS DATE) AND (CAST('2000-08-26' AS DATE) + INTERVAL '30' DAY)
        AND ws_web_site_sk = web_site_sk
        AND ws_item_sk = i_item_sk
        AND i_current_price > 50
        AND ws_promo_sk = p_promo_sk
        AND p_channel_tv = 'N'
    GROUP BY
        web_site_id
)
SELECT
    channel,
    id,
    SUM(sales) AS sales,
    SUM(returns1) AS returns1,
    SUM(profit) AS profit
FROM
    (
        SELECT 'store channel' AS channel,
               CONCAT('store', store_id) AS id,
               sales,
               returns1,
               profit
        FROM ssr
        UNION ALL
        SELECT 'catalog channel' AS channel,
               CONCAT('catalog_page', catalog_page_id) AS id,
               sales,
               returns1,
               profit
        FROM csr
        UNION ALL
        SELECT 'web channel' AS channel,
               CONCAT('web_site', web_site_id) AS id,
               sales,
               returns1,
               profit
        FROM wsr
    ) x
GROUP BY
    channel, id
ORDER BY
    channel,
    id
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 91 and QUERY_ID < 97);

-- start query 81 in stream 0 using template query81.tpl 
WITH customer_total_return 
     AS (SELECT cr_returning_customer_sk   AS ctr_customer_sk, 
                ca_state                   AS ctr_state, 
                Sum(cr_return_amt_inc_tax) AS ctr_total_return 
         FROM   catalog_returns, 
                date_dim, 
                customer_address 
         WHERE  cr_returned_date_sk = d_date_sk 
                AND d_year = 1999 
                AND cr_returning_addr_sk = ca_address_sk 
         GROUP  BY cr_returning_customer_sk, 
                   ca_state) 
SELECT c_customer_id, 
               c_salutation, 
               c_first_name, 
               c_last_name, 
               ca_street_number, 
               ca_street_name, 
               ca_street_type, 
               ca_suite_number, 
               ca_city, 
               ca_county, 
               ca_state, 
               ca_zip, 
               ca_country, 
               ca_gmt_offset, 
               ca_location_type, 
               ctr_total_return 
FROM   customer_total_return ctr1, 
       customer_address, 
       customer 
WHERE  ctr1.ctr_total_return > (SELECT Avg(ctr_total_return) * 1.2 
                                FROM   customer_total_return ctr2 
                                WHERE  ctr1.ctr_state = ctr2.ctr_state) 
       AND ca_address_sk = c_current_addr_sk 
       AND ca_state = 'TX' 
       AND ctr1.ctr_customer_sk = c_customer_sk 
ORDER  BY c_customer_id, 
          c_salutation, 
          c_first_name, 
          c_last_name, 
          ca_street_number, 
          ca_street_name, 
          ca_street_type, 
          ca_suite_number, 
          ca_city, 
          ca_county, 
          ca_state, 
          ca_zip, 
          ca_country, 
          ca_gmt_offset, 
          ca_location_type, 
          ctr_total_return
LIMIT 100;


-- start query 82 in stream 0 using template query82.tpl 
SELECT
         i_item_id , 
         i_item_desc , 
         i_current_price 
FROM     item, 
         inventory, 
         date_dim, 
         store_sales 
WHERE    i_current_price BETWEEN 63 AND      63+30 
AND      inv_item_sk = i_item_sk 
AND      d_date_sk=inv_date_sk 
AND      d_date BETWEEN Cast('1998-04-27' AS DATE) AND      ( 
                  Cast('1998-04-27' AS DATE) + INTERVAL '60' day) 
AND      i_manufact_id IN (57,293,427,320) 
AND      inv_quantity_on_hand BETWEEN 100 AND      500 
AND      ss_item_sk = i_item_sk 
GROUP BY i_item_id, 
         i_item_desc, 
         i_current_price 
ORDER BY i_item_id 
LIMIT 100;


-- start query 83 in stream 0 using template query83.tpl 
WITH sr_items 
     AS (SELECT i_item_id               item_id, 
                Sum(sr_return_quantity) sr_item_qty 
         FROM   store_returns, 
                item, 
                date_dim 
         WHERE  sr_item_sk = i_item_sk 
                AND d_date IN (SELECT d_date 
                               FROM   date_dim 
                               WHERE  d_week_seq IN (SELECT d_week_seq 
                                                     FROM   date_dim 
                                                     WHERE 
                                      d_date IN ( '1999-06-30', 
                                                  '1999-08-28', 
                                                  '1999-11-18' 
                                                ))) 
                AND sr_returned_date_sk = d_date_sk 
         GROUP  BY i_item_id), 
     cr_items 
     AS (SELECT i_item_id               item_id, 
                Sum(cr_return_quantity) cr_item_qty 
         FROM   catalog_returns, 
                item, 
                date_dim 
         WHERE  cr_item_sk = i_item_sk 
                AND d_date IN (SELECT d_date 
                               FROM   date_dim 
                               WHERE  d_week_seq IN (SELECT d_week_seq 
                                                     FROM   date_dim 
                                                     WHERE 
                                      d_date IN ( '1999-06-30', 
                                                  '1999-08-28', 
                                                  '1999-11-18' 
                                                ))) 
                AND cr_returned_date_sk = d_date_sk 
         GROUP  BY i_item_id), 
     wr_items 
     AS (SELECT i_item_id               item_id, 
                Sum(wr_return_quantity) wr_item_qty 
         FROM   web_returns, 
                item, 
                date_dim 
         WHERE  wr_item_sk = i_item_sk 
                AND d_date IN (SELECT d_date 
                               FROM   date_dim 
                               WHERE  d_week_seq IN (SELECT d_week_seq 
                                                     FROM   date_dim 
                                                     WHERE 
                                      d_date IN ( '1999-06-30', 
                                                  '1999-08-28', 
                                                  '1999-11-18' 
                                                ))) 
                AND wr_returned_date_sk = d_date_sk 
         GROUP  BY i_item_id) 
SELECT sr_items.item_id, 
               sr_item_qty, 
               sr_item_qty / ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0 * 
               100 sr_dev, 
               cr_item_qty, 
               cr_item_qty / ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0 * 
               100 cr_dev, 
               wr_item_qty, 
               wr_item_qty / ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0 * 
               100 wr_dev, 
               ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0 
               average 
FROM   sr_items, 
       cr_items, 
       wr_items 
WHERE  sr_items.item_id = cr_items.item_id 
       AND sr_items.item_id = wr_items.item_id 
ORDER  BY sr_items.item_id, 
          sr_item_qty
LIMIT 100;


-- start query 84 in stream 0 using template query84.tpl 
SELECT c_customer_id   AS customer_id, 
               c_last_name 
               || ', ' 
               || c_first_name AS customername 
FROM   customer, 
       customer_address, 
       customer_demographics, 
       household_demographics, 
       income_band, 
       store_returns 
WHERE  ca_city = 'Green Acres' 
       AND c_current_addr_sk = ca_address_sk 
       AND ib_lower_bound >= 54986 
       AND ib_upper_bound <= 54986 + 50000 
       AND ib_income_band_sk = hd_income_band_sk 
       AND cd_demo_sk = c_current_cdemo_sk 
       AND hd_demo_sk = c_current_hdemo_sk 
       AND sr_cdemo_sk = cd_demo_sk 
ORDER  BY c_customer_id
LIMIT 100;


-- start query 85 in stream 0 using template query85.tpl 
SELECT Substr(r_reason_desc, 1, 20), 
               Avg(ws_quantity), 
               Avg(wr_refunded_cash), 
               Avg(wr_fee) 
FROM   web_sales, 
       web_returns, 
       web_page, 
       customer_demographics cd1, 
       customer_demographics cd2, 
       customer_address, 
       date_dim, 
       reason 
WHERE  ws_web_page_sk = wp_web_page_sk 
       AND ws_item_sk = wr_item_sk 
       AND ws_order_number = wr_order_number 
       AND ws_sold_date_sk = d_date_sk 
       AND d_year = 2001 
       AND cd1.cd_demo_sk = wr_refunded_cdemo_sk 
       AND cd2.cd_demo_sk = wr_returning_cdemo_sk 
       AND ca_address_sk = wr_refunded_addr_sk 
       AND r_reason_sk = wr_reason_sk 
       AND ( ( cd1.cd_marital_status = 'W' 
               AND cd1.cd_marital_status = cd2.cd_marital_status 
               AND cd1.cd_education_status = 'Primary' 
               AND cd1.cd_education_status = cd2.cd_education_status 
               AND ws_sales_price BETWEEN 100.00 AND 150.00 ) 
              OR ( cd1.cd_marital_status = 'D' 
                   AND cd1.cd_marital_status = cd2.cd_marital_status 
                   AND cd1.cd_education_status = 'Secondary' 
                   AND cd1.cd_education_status = cd2.cd_education_status 
                   AND ws_sales_price BETWEEN 50.00 AND 100.00 ) 
              OR ( cd1.cd_marital_status = 'M' 
                   AND cd1.cd_marital_status = cd2.cd_marital_status 
                   AND cd1.cd_education_status = 'Advanced Degree' 
                   AND cd1.cd_education_status = cd2.cd_education_status 
                   AND ws_sales_price BETWEEN 150.00 AND 200.00 ) ) 
       AND ( ( ca_country = 'United States' 
               AND ca_state IN ( 'KY', 'ME', 'IL' ) 
               AND ws_net_profit BETWEEN 100 AND 200 ) 
              OR ( ca_country = 'United States' 
                   AND ca_state IN ( 'OK', 'NE', 'MN' ) 
                   AND ws_net_profit BETWEEN 150 AND 300 ) 
              OR ( ca_country = 'United States' 
                   AND ca_state IN ( 'FL', 'WI', 'KS' ) 
                   AND ws_net_profit BETWEEN 50 AND 250 ) ) 
GROUP  BY r_reason_desc 
ORDER  BY Substr(r_reason_desc, 1, 20), 
          Avg(ws_quantity), 
          Avg(wr_refunded_cash), 
          Avg(wr_fee)
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 97 and QUERY_ID < 103);

-- start query 86 in stream 0 using template query86.tpl 
WITH ranked_data AS (
    SELECT
        SUM(ws_net_paid) AS total_sum,
        i_category,
        i_class,
        RANK() OVER (
            PARTITION BY i_category, i_class
            ORDER BY SUM(ws_net_paid) DESC
        ) AS rank_within_parent
    FROM
        web_sales
        JOIN date_dim d1 ON d1.d_date_sk = ws_sold_date_sk
        JOIN item ON i_item_sk = ws_item_sk
    WHERE
        d1.d_month_seq BETWEEN 1183 AND 1183 + 11
    GROUP BY
        i_category, i_class
)
SELECT
    total_sum,
    i_category,
    i_class,
    rank_within_parent
FROM
    ranked_data
ORDER BY
    total_sum DESC, i_category, rank_within_parent
LIMIT 100;


-- start query 87 in stream 0 using template query87.tpl
select count(*) 
from ((select distinct c_last_name, c_first_name, d_date
       from store_sales, date_dim, customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from catalog_sales, date_dim, customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from web_sales, date_dim, customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1188 and 1188+11)
) cool_cust
;


-- start query 88 in stream 0 using template query88.tpl
select  *
from
 (select count(*) h8_30_to_9
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk   
     and ss_hdemo_sk = household_demographics.hd_demo_sk 
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 8
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2)) 
     and store.s_store_name = 'ese') s1,
 (select count(*) h9_to_9_30 
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and time_dim.t_hour = 9 
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s2,
 (select count(*) h9_30_to_10 
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 9
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s3,
 (select count(*) h10_to_10_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10 
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s4,
 (select count(*) h10_30_to_11
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10 
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s5,
 (select count(*) h11_to_11_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and time_dim.t_hour = 11
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s6,
 (select count(*) h11_30_to_12
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 11
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s7,
 (select count(*) h12_to_12_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 12
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 2 and household_demographics.hd_vehicle_count<=2+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s8
;


-- start query 89 in stream 0 using template query89.tpl 
SELECT  * 
FROM  (SELECT i_category, 
              i_class, 
              i_brand, 
              s_store_name, 
              s_company_name, 
              d_moy, 
              Sum(ss_sales_price) sum_sales, 
              Avg(Sum(ss_sales_price)) 
                OVER ( 
                  partition BY i_category, i_brand, s_store_name, s_company_name 
                ) 
                                  avg_monthly_sales 
       FROM   item, 
              store_sales, 
              date_dim, 
              store 
       WHERE  ss_item_sk = i_item_sk 
              AND ss_sold_date_sk = d_date_sk 
              AND ss_store_sk = s_store_sk 
              AND d_year IN ( 2002 ) 
              AND ( ( i_category IN ( 'Home', 'Men', 'Sports' ) 
                      AND i_class IN ( 'paint', 'accessories', 'fitness' ) ) 
                     OR ( i_category IN ( 'Shoes', 'Jewelry', 'Women' ) 
                          AND i_class IN ( 'mens', 'pendants', 'swimwear' ) ) ) 
       GROUP  BY i_category, 
                 i_class, 
                 i_brand, 
                 s_store_name, 
                 s_company_name, 
                 d_moy) tmp1 
WHERE  CASE 
         WHEN ( avg_monthly_sales <> 0 ) THEN ( 
         Abs(sum_sales - avg_monthly_sales) / avg_monthly_sales ) 
         ELSE NULL 
       END > 0.1 
ORDER  BY sum_sales - avg_monthly_sales, 
          s_store_name
LIMIT 100;


-- start query 90 in stream 0 using template query90.tpl 
SELECT Cast(amc AS DECIMAL(15, 4)) / Cast(pmc AS DECIMAL(15, 4)) 
               am_pm_ratio 
FROM   (SELECT Count(*) amc 
        FROM   web_sales, 
               household_demographics, 
               time_dim, 
               web_page 
        WHERE  ws_sold_time_sk = time_dim.t_time_sk 
               AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk 
               AND ws_web_page_sk = web_page.wp_web_page_sk 
               AND time_dim.t_hour BETWEEN 12 AND 12 + 1 
               AND household_demographics.hd_dep_count = 8 
               AND web_page.wp_char_count BETWEEN 5000 AND 5200) at1, 
       (SELECT Count(*) pmc 
        FROM   web_sales, 
               household_demographics, 
               time_dim, 
               web_page 
        WHERE  ws_sold_time_sk = time_dim.t_time_sk 
               AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk 
               AND ws_web_page_sk = web_page.wp_web_page_sk 
               AND time_dim.t_hour BETWEEN 20 AND 20 + 1 
               AND household_demographics.hd_dep_count = 8 
               AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt 
ORDER  BY am_pm_ratio
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 103 and QUERY_ID < 109);

-- start query 91 in stream 0 using template query91.tpl 
SELECT cc_call_center_id Call_Center, 
       cc_name           Call_Center_Name, 
       cc_manager        Manager, 
       Sum(cr_net_loss)  Returns_Loss 
FROM   call_center, 
       catalog_returns, 
       date_dim, 
       customer, 
       customer_address, 
       customer_demographics, 
       household_demographics 
WHERE  cr_call_center_sk = cc_call_center_sk 
       AND cr_returned_date_sk = d_date_sk 
       AND cr_returning_customer_sk = c_customer_sk 
       AND cd_demo_sk = c_current_cdemo_sk 
       AND hd_demo_sk = c_current_hdemo_sk 
       AND ca_address_sk = c_current_addr_sk 
       AND d_year = 1999 
       AND d_moy = 12 
       AND ( ( cd_marital_status = 'M' 
               AND cd_education_status = 'Unknown' ) 
              OR ( cd_marital_status = 'W' 
                   AND cd_education_status = 'Advanced Degree' ) ) 
       AND hd_buy_potential LIKE 'Unknown%' 
       AND ca_gmt_offset = -7 
GROUP  BY cc_call_center_id, 
          cc_name, 
          cc_manager, 
          cd_marital_status, 
          cd_education_status 
ORDER  BY Sum(cr_net_loss) DESC;


-- start query 92 in stream 0 using template query92.tpl 
SELECT 
         Sum(ws_ext_discount_amt) AS `Excess Discount Amount`
FROM     web_sales , 
         item , 
         date_dim 
WHERE    i_manufact_id = 718 
AND      i_item_sk = ws_item_sk 
AND      d_date BETWEEN '2002-03-29' AND      ( 
                  Cast('2002-03-29' AS DATE) +  INTERVAL '90' day) 
AND      d_date_sk = ws_sold_date_sk 
AND      ws_ext_discount_amt > 
         ( 
                SELECT 1.3 * avg(ws_ext_discount_amt) 
                FROM   web_sales , 
                       date_dim 
                WHERE  ws_item_sk = i_item_sk 
                AND    d_date BETWEEN '2002-03-29' AND    ( 
                              cast('2002-03-29' AS date) + INTERVAL '90' day) 
                AND    d_date_sk = ws_sold_date_sk ) 
ORDER BY sum(ws_ext_discount_amt) 
LIMIT 100;


-- start query 93 in stream 0 using template query93.tpl 
SELECT ss_customer_sk, 
               Sum(act_sales) sumsales 
FROM   (SELECT ss_item_sk, 
               ss_ticket_number, 
               ss_customer_sk, 
               CASE 
                 WHEN sr_return_quantity IS NOT NULL THEN 
                 ( ss_quantity - sr_return_quantity ) * ss_sales_price 
                 ELSE ( ss_quantity * ss_sales_price ) 
               END act_sales 
        FROM   store_sales 
               LEFT OUTER JOIN store_returns 
                            ON ( sr_item_sk = ss_item_sk 
                                 AND sr_ticket_number = ss_ticket_number ), 
               reason 
        WHERE  sr_reason_sk = r_reason_sk 
               AND r_reason_desc = 'reason 38') t 
GROUP  BY ss_customer_sk 
ORDER  BY sumsales, 
          ss_customer_sk
LIMIT 100;


-- start query 94 in stream 0 using template query94.tpl 
SELECT 
         Count(DISTINCT ws_order_number) AS `order count` , 
         Sum(ws_ext_ship_cost)           AS `total shipping cost` , 
         Sum(ws_net_profit)              AS `total net profit` 
FROM     web_sales ws1 , 
         date_dim , 
         customer_address , 
         web_site 
WHERE    d_date BETWEEN '2000-3-01' AND      ( 
                  Cast('2000-3-01' AS DATE) + INTERVAL '60' day) 
AND      ws1.ws_ship_date_sk = d_date_sk 
AND      ws1.ws_ship_addr_sk = ca_address_sk 
AND      ca_state = 'MT' 
AND      ws1.ws_web_site_sk = web_site_sk 
AND      web_company_name = 'pri' 
AND      EXISTS 
         ( 
                SELECT * 
                FROM   web_sales ws2 
                WHERE  ws1.ws_order_number = ws2.ws_order_number 
                AND    ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk) 
AND      NOT EXISTS 
         ( 
                SELECT * 
                FROM   web_returns wr1 
                WHERE  ws1.ws_order_number = wr1.wr_order_number) 
ORDER BY count(DISTINCT ws_order_number) 
LIMIT 100;


-- start query 95 in stream 0 using template query95.tpl 
WITH ws_wh AS 
( 
       SELECT ws1.ws_order_number, 
              ws1.ws_warehouse_sk wh1, 
              ws2.ws_warehouse_sk wh2 
       FROM   web_sales ws1, 
              web_sales ws2 
       WHERE  ws1.ws_order_number = ws2.ws_order_number 
       AND    ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk) 
SELECT 
         Count(DISTINCT ws_order_number) AS `order count` , 
         Sum(ws_ext_ship_cost)           AS `total shipping cost` , 
         Sum(ws_net_profit)              AS `total net profit` 
FROM     web_sales ws1 , 
         date_dim , 
         customer_address , 
         web_site 
WHERE    d_date BETWEEN '2000-4-01' AND      ( 
                  Cast('2000-4-01' AS DATE) + INTERVAL '60' day) 
AND      ws1.ws_ship_date_sk = d_date_sk 
AND      ws1.ws_ship_addr_sk = ca_address_sk 
AND      ca_state = 'IN' 
AND      ws1.ws_web_site_sk = web_site_sk 
AND      web_company_name = 'pri' 
AND      ws1.ws_order_number IN 
         ( 
                SELECT ws_order_number 
                FROM   ws_wh) 
AND      ws1.ws_order_number IN 
         ( 
                SELECT wr_order_number 
                FROM   web_returns, 
                       ws_wh 
                WHERE  wr_order_number = ws_wh.ws_order_number) 
ORDER BY count(DISTINCT ws_order_number) 
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 109 and QUERY_ID < 115);

-- start query 96 in stream 0 using template query96.tpl 
SELECT Count(*) 
FROM   store_sales, 
       household_demographics, 
       time_dim, 
       store 
WHERE  ss_sold_time_sk = time_dim.t_time_sk 
       AND ss_hdemo_sk = household_demographics.hd_demo_sk 
       AND ss_store_sk = s_store_sk 
       AND time_dim.t_hour = 15 
       AND time_dim.t_minute >= 30 
       AND household_demographics.hd_dep_count = 7 
       AND store.s_store_name = 'ese' 
ORDER  BY Count(*)
LIMIT 100;


-- start query 97 in stream 0 using template query97.tpl 
WITH ssci AS (
    SELECT
        ss_customer_sk AS customer_sk,
        ss_item_sk AS item_sk
    FROM
        store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE
        d_month_seq BETWEEN 1196 AND 1196 + 11
    GROUP BY
        ss_customer_sk, ss_item_sk
),
csci AS (
    SELECT
        cs_bill_customer_sk AS customer_sk,
        cs_item_sk AS item_sk
    FROM
        catalog_sales
        JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE
        d_month_seq BETWEEN 1196 AND 1196 + 11
    GROUP BY
        cs_bill_customer_sk, cs_item_sk
)
SELECT
    SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NULL THEN 1 ELSE 0 END) AS store_only,
    SUM(CASE WHEN ssci.customer_sk IS NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) AS catalog_only,
    SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) AS store_and_catalog
FROM
    ssci
LEFT JOIN
    csci ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk

UNION

SELECT
    SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN ssci.customer_sk IS NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END)
FROM
    ssci
RIGHT JOIN
    csci ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk
WHERE
    ssci.customer_sk IS NULL

LIMIT 100;


-- start query 98 in stream 0 using template query98.tpl 
SELECT i_item_id, 
       i_item_desc, 
       i_category, 
       i_class, 
       i_current_price, 
       Sum(ss_ext_sales_price)                                   AS itemrevenue, 
       Sum(ss_ext_sales_price) * 100 / Sum(Sum(ss_ext_sales_price)) 
                                         OVER ( 
                                           PARTITION BY i_class) AS revenueratio 
FROM   store_sales, 
       item, 
       date_dim 
WHERE  ss_item_sk = i_item_sk 
       AND i_category IN ( 'Men', 'Home', 'Electronics' ) 
       AND ss_sold_date_sk = d_date_sk 
       AND d_date BETWEEN CAST('2000-05-18' AS DATE) AND ( 
                          CAST('2000-05-18' AS DATE) + INTERVAL '30' DAY ) 
GROUP  BY i_item_id, 
          i_item_desc, 
          i_category, 
          i_class, 
          i_current_price 
ORDER  BY i_category, 
          i_class, 
          i_item_id, 
          i_item_desc, 
          revenueratio;


-- start query 99 in stream 0 using template query99.tpl 
SELECT Substr(w_warehouse_name, 1, 20), 
               sm_type, 
               cc_name, 
               Sum(CASE 
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk <= 30 ) THEN 1 
                     ELSE 0 
                   END) AS `30 days`, 
               Sum(CASE 
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 30 ) 
                          AND ( cs_ship_date_sk - cs_sold_date_sk <= 60 ) THEN 1 
                     ELSE 0 
                   END) AS `31-60 days`, 
               Sum(CASE 
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 60 ) 
                          AND ( cs_ship_date_sk - cs_sold_date_sk <= 90 ) THEN 1 
                     ELSE 0 
                   END) AS `61-90 days`, 
               Sum(CASE 
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 90 ) 
                          AND ( cs_ship_date_sk - cs_sold_date_sk <= 120 ) THEN 
                     1 
                     ELSE 0 
                   END) AS `91-120 days`, 
               Sum(CASE 
                     WHEN ( cs_ship_date_sk - cs_sold_date_sk > 120 ) THEN 1 
                     ELSE 0 
                   END) AS `>120 days` 
FROM   catalog_sales, 
       warehouse, 
       ship_mode, 
       call_center, 
       date_dim 
WHERE  d_month_seq BETWEEN 1200 AND 1200 + 11 
       AND cs_ship_date_sk = d_date_sk 
       AND cs_warehouse_sk = w_warehouse_sk 
       AND cs_ship_mode_sk = sm_ship_mode_sk 
       AND cs_call_center_sk = cc_call_center_sk 
GROUP  BY Substr(w_warehouse_name, 1, 20), 
          sm_type, 
          cc_name 
ORDER  BY Substr(w_warehouse_name, 1, 20), 
          sm_type, 
          cc_name
LIMIT 100;

INSERT INTO ma_profile_final
SELECT * FROM information_schema.profiling
where (QUERY_ID > 115 and QUERY_ID < 120);
