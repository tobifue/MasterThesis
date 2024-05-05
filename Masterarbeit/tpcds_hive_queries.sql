
-- 8 
-- Start query 8 in stream 0 using template query8.tpl

-- Main Query
SELECT
    s_store_name,
    SUM(ss_net_profit)
FROM
    store_sales
JOIN
    date_dim ON ss_sold_date_sk = d_date_sk
JOIN
    store ON ss_store_sk = s_store_sk
JOIN (
    SELECT
        ca_zip
    FROM (
        SELECT
            substr(ca_zip, 1, 5) AS ca_zip
        FROM
            customer_address
        WHERE
            substr(ca_zip, 1, 5) IN (
                '67436', '26121', '38443', '63157', '68856', '19485', '86425', '26741', '70991', '60899',
                '63573', '47556', '56193', '93314', '87827', '62017', '85067', '95390', '48091', '10261',
                '81845', '41790', '42853', '24675', '12840', '60065', '84430', '57451', '24021', '91735',
                '75335', '71935', '34482', '56943', '70695', '52147', '56251', '28411', '86653', '23005',
                '22478', '29031', '34398', '15365', '42460', '33337', '59433', '73943', '72477', '74081',
                '74430', '64605', '39006', '11226', '49057', '97308', '42663', '18187', '19768', '43454',
                '32147', '76637', '51975', '11181', '45630', '33129', '45995', '64386', '55522', '26697',
                '20963', '35154', '64587', '49752', '66386', '30586', '59286', '13177', '66646', '84195',
                '74316', '36853', '32927', '12469', '11904', '36269', '17724', '55346', '12595', '53988',
                '65439', '28015', '63268', '73590', '29216', '82575', '69267', '13805', '91678', '79460',
                '94152', '14961', '15419', '48277', '62588', '55493', '28360', '14152', '55225', '18007',
                '53705', '56573', '80245', '71769', '57348', '36845', '13039', '17270', '22363', '83474',
                '25294', '43269', '77666', '15488', '99146', '64441', '43338', '38736', '62754', '48556',
                '86057', '23090', '38114', '66061', '18910', '84385', '23600', '19975', '27883', '65719',
                '19933', '32085', '49731', '40473', '27190', '46192', '23949', '44738', '12436', '64794',
                '68741', '15333', '24282', '49085', '31844', '71156', '48441', '17100', '98207', '44982',
                '20277', '71496', '96299', '37583', '22206', '89174', '30589', '61924', '53079', '10976',
                '13104', '42794', '54772', '15809', '56434', '39975', '13874', '30753', '77598', '78229',
                '59478', '12345', '55547', '57422', '42600', '79444', '29074', '29752', '21676', '32096',
                '43044', '39383', '37296', '36295', '63077', '16572', '31275', '18701', '40197', '48242',
                '27219', '49865', '84175', '30446', '25165', '13807', '72142', '70499', '70464', '71429',
                '18111', '70857', '29545', '36425', '52706', '36194', '42963', '75068', '47921', '74763',
                '90990', '89456', '62073', '88397', '73963', '75885', '62657', '12530', '81146', '57434',
                '25099', '41429', '98441', '48713', '52552', '31667', '14072', '13903', '44709', '85429',
                '58017', '38295', '44875', '73541', '30091', '12707', '23762', '62258', '33247', '78722',
                '77431', '14510', '35656', '72428', '92082', '35267', '43759', '24354', '90952', '11512',
                '21242', '22579', '56114', '32339', '52282', '41791', '24484', '95020', '28408', '99710',
                '11899', '43344', '72915', '27644', '62708', '74479', '17177', '32619', '12351', '91339',
                '31169', '57081', '53522', '16712', '34419', '71779', '44187', '46206', '96099', '61910',
                '53664', '12295', '31837', '33096', '10813', '63048', '31732', '79118', '73084', '72783',
                '84952', '46965', '77956', '39815', '32311', '75329', '48156', '30826', '49661', '13736',
                '92076', '74865', '88149', '92397', '52777', '68453', '32012', '21222', '52721', '24626',
                '18210', '42177', '91791', '75251', '82075', '44372', '45542', '20609', '60115', '17362',
                '22750', '90434', '31852', '54071', '33762', '14705', '40718', '56433', '30996', '40657',
                '49056', '23585', '66455', '41021', '74736', '72151', '37007', '21729', '60177', '84558',
                '59027', '93855', '60022', '86443', '19541', '86886', '30532', '39062', '48532', '34713',
                '52077', '22564', '64638', '15273', '31677', '36138', '62367', '60261', '80213', '42818',
                '25113', '72378', '69802', '69096', '55443', '28820', '13848', '78258', '37490', '30556',
                '77380', '28447', '44550', '26791', '70609', '82182', '33306', '43224', '22322', '86959',
                '68519', '14308', '46501', '81131', '34056', '61991', '19896', '87804', '65774', '92564'
            )
        INTERSECT
        SELECT
            ca_zip
        FROM (
            SELECT
                substr(ca_zip, 1, 5) AS ca_zip,
                COUNT(*) AS cnt
            FROM
                customer_address
            JOIN
                customer ON ca_address_sk = c_current_addr_sk
            WHERE
                c_preferred_cust_flag = 'Y'
            GROUP BY
                ca_zip
            HAVING
                COUNT(*) > 10
        ) A1
    ) A2
) V1 ON substr(s_zip, 1, 2) = substr(V1.ca_zip, 1, 2)
WHERE
    d_qoy = 2
    AND d_year = 2000
GROUP BY
    s_store_name
ORDER BY
    s_store_name
LIMIT 100;

-- 9
-- Start query 9 in stream 0 using template query9.tpl

-- Main Query
SELECT
    CASE
        WHEN (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) > 3672 THEN
            (SELECT AVG(ss_ext_list_price) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20)
        ELSE
            (SELECT AVG(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20)
    END AS bucket1,
    CASE
        WHEN (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40) > 3392 THEN
            (SELECT AVG(ss_ext_list_price) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40)
        ELSE
            (SELECT AVG(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40)
    END AS bucket2,
    CASE
        WHEN (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60) > 32784 THEN
            (SELECT AVG(ss_ext_list_price) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60)
        ELSE
            (SELECT AVG(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60)
    END AS bucket3,
    CASE
        WHEN (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80) > 26032 THEN
            (SELECT AVG(ss_ext_list_price) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80)
        ELSE
            (SELECT AVG(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80)
    END AS bucket4,
    CASE
        WHEN (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100) > 23982 THEN
            (SELECT AVG(ss_ext_list_price) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100)
        ELSE
            (SELECT AVG(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100)
    END AS bucket5
FROM
    reason
WHERE
    r_reason_sk = 1;

-- 10 
-- Start query 10 in stream 0 using template query10.tpl

-- Main Query
SELECT
    cd_gender,
    cd_marital_status,
    cd_education_status,
    COUNT(*) AS cnt1,
    cd_purchase_estimate,
    COUNT(*) AS cnt2,
    cd_credit_rating,
    COUNT(*) AS cnt3,
    cd_dep_count,
    COUNT(*) AS cnt4,
    cd_dep_employed_count,
    COUNT(*) AS cnt5,
    cd_dep_college_count,
    COUNT(*) AS cnt6
FROM
    customer c
JOIN
    customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN
    customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE
    ca.ca_county IN ('Lycoming County', 'Sheridan County', 'Kandiyohi County', 'Pike County', 'Greene County')
    AND EXISTS (
        SELECT *
        FROM store_sales ss
        JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        WHERE c.c_customer_sk = ss.ss_customer_sk
        AND d.d_year = 2002
        AND d.d_moy BETWEEN 4 AND 4 + 3
    )
    AND (
        EXISTS (
            SELECT *
            FROM web_sales ws
            JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
            WHERE c.c_customer_sk = ws.ws_bill_customer_sk
            AND d.d_year = 2002
            AND d.d_moy BETWEEN 4 AND 4 + 3
        )
        OR EXISTS (
            SELECT *
            FROM catalog_sales cs
            JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
            WHERE c.c_customer_sk = cs.cs_ship_customer_sk
            AND d.d_year = 2002
            AND d.d_moy BETWEEN 4 AND 4 + 3
        )
    )
GROUP BY
    cd_gender,
    cd_marital_status,
    cd_education_status,
    cd_purchase_estimate,
    cd_credit_rating,
    cd_dep_count,
    cd_dep_employed_count,
    cd_dep_college_count
ORDER BY
    cd_gender,
    cd_marital_status,
    cd_education_status,
    cd_purchase_estimate,
    cd_credit_rating,
    cd_dep_count,
    cd_dep_employed_count,
    cd_dep_college_count
LIMIT 100;

-- 11
-- Start query 11 in stream 0 using template query11.tpl

-- Common Table Expression (CTE) to calculate yearly totals
WITH year_total AS (
    SELECT
        c_customer_id AS customer_id,
        c_first_name AS customer_first_name,
        c_last_name AS customer_last_name,
        c_preferred_cust_flag AS customer_preferred_cust_flag,
        c_birth_country AS customer_birth_country,
        c_login AS customer_login,
        c_email_address AS customer_email_address,
        d_year AS dyear,
        SUM(ss_ext_list_price - ss_ext_discount_amt) AS year_total,
        's' AS sale_type
    FROM
        customer
    JOIN
        store_sales ON c_customer_sk = ss_customer_sk
    JOIN
        date_dim ON ss_sold_date_sk = d_date_sk
    GROUP BY
        c_customer_id,
        c_first_name,
        c_last_name,
        c_preferred_cust_flag,
        c_birth_country,
        c_login,
        c_email_address,
        d_year
    UNION ALL
    SELECT
        c_customer_id AS customer_id,
        c_first_name AS customer_first_name,
        c_last_name AS customer_last_name,
        c_preferred_cust_flag AS customer_preferred_cust_flag,
        c_birth_country AS customer_birth_country,
        c_login AS customer_login,
        c_email_address AS customer_email_address,
        d_year AS dyear,
        SUM(ws_ext_list_price - ws_ext_discount_amt) AS year_total,
        'w' AS sale_type
    FROM
        customer
    JOIN
        web_sales ON c_customer_sk = ws_bill_customer_sk
    JOIN
        date_dim ON ws_sold_date_sk = d_date_sk
    GROUP BY
        c_customer_id,
        c_first_name,
        c_last_name,
        c_preferred_cust_flag,
        c_birth_country,
        c_login,
        c_email_address,
        d_year
)

-- Main Query
SELECT
    t_s_secyear.customer_id,
    t_s_secyear.customer_first_name,
    t_s_secyear.customer_last_name,
    t_s_secyear.customer_birth_country
FROM
    year_total t_s_firstyear
JOIN
    year_total t_s_secyear ON t_s_secyear.customer_id = t_s_firstyear.customer_id
JOIN
    year_total t_w_firstyear ON t_s_firstyear.customer_id = t_w_firstyear.customer_id
JOIN
    year_total t_w_secyear ON t_s_firstyear.customer_id = t_w_secyear.customer_id
WHERE
    t_s_firstyear.sale_type = 's'
    AND t_w_firstyear.sale_type = 'w'
    AND t_s_secyear.sale_type = 's'
    AND t_w_secyear.sale_type = 'w'
    AND t_s_firstyear.dyear = 2001
    AND t_s_secyear.dyear = 2001 + 1
    AND t_w_firstyear.dyear = 2001
    AND t_w_secyear.dyear = 2001 + 1
    AND t_s_firstyear.year_total > 0
    AND t_w_firstyear.year_total > 0
    AND (
        CASE
            WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
            ELSE 0.0
        END > CASE
            WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
            ELSE 0.0
        END
    )
ORDER BY
    t_s_secyear.customer_id,
    t_s_secyear.customer_first_name,
    t_s_secyear.customer_last_name,
    t_s_secyear.customer_birth_country
LIMIT 100;

--12
-- Start query 12 in stream 0 using template query12.tpl

SELECT
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    SUM(ws_ext_sales_price) AS itemrevenue,
    SUM(ws_ext_sales_price) * 100 / SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM
    web_sales
JOIN
    item ON ws_item_sk = i_item_sk
JOIN
    date_dim ON ws_sold_date_sk = d_date_sk
WHERE
    i_category IN ('Home', 'Men', 'Women')
    AND d_date BETWEEN CAST('2000-05-11' AS DATE) AND (CAST('2000-05-11' AS DATE) + INTERVAL '30' DAY)
GROUP BY
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price
ORDER BY
    i_category,
    i_class,
    i_item_id,
    i_item_desc,
    revenueratio
LIMIT 100;

--13
-- Start query 13 in stream 0 using template query13.tpl

SELECT
    AVG(ss_quantity),
    AVG(ss_ext_sales_price),
    AVG(ss_ext_wholesale_cost),
    SUM(ss_ext_wholesale_cost)
FROM
    store_sales
JOIN
    store ON s_store_sk = ss_store_sk
JOIN
    customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN
    household_demographics ON hd_demo_sk = ss_hdemo_sk
JOIN
    customer_address ON ss_addr_sk = ca_address_sk
JOIN
    date_dim ON ss_sold_date_sk = d_date_sk
WHERE
    d_year = 2001
    AND (
        (ss_hdemo_sk = hd_demo_sk
        AND cd_marital_status = 'U'
        AND cd_education_status = 'Advanced Degree'
        AND ss_sales_price BETWEEN 100.00 AND 150.00
        AND hd_dep_count = 3)
        OR
        (ss_hdemo_sk = hd_demo_sk
        AND cd_marital_status = 'M'
        AND cd_education_status = 'Primary'
        AND ss_sales_price BETWEEN 50.00 AND 100.00
        AND hd_dep_count = 1)
        OR
        (ss_hdemo_sk = hd_demo_sk
        AND cd_marital_status = 'D'
        AND cd_education_status = 'Secondary'
        AND ss_sales_price BETWEEN 150.00 AND 200.00
        AND hd_dep_count = 1)
    )
    AND (
        (ss_addr_sk = ca_address_sk
        AND ca_country = 'United States'
        AND ca_state IN ('AZ', 'NE', 'IA')
        AND ss_net_profit BETWEEN 100 AND 200)
        OR
        (ss_addr_sk = ca_address_sk
        AND ca_country = 'United States'
        AND ca_state IN ('MS', 'CA', 'NV')
        AND ss_net_profit BETWEEN 150 AND 300)
        OR
        (ss_addr_sk = ca_address_sk
        AND ca_country = 'United States'
        AND ca_state IN ('GA', 'TX', 'NJ')
        AND ss_net_profit BETWEEN 50 AND 250)
    );

--14
-- Start query 14 in stream 0 using template query14.tpl
WITH cross_items AS (
    SELECT DISTINCT i_item_sk AS ss_item_sk
    FROM item
    WHERE i_brand_id IN (
        SELECT iss.i_brand_id
        FROM store_sales
        JOIN item iss ON store_sales.ss_item_sk = iss.i_item_sk
        JOIN date_dim d1 ON store_sales.ss_sold_date_sk = d1.d_date_sk
        WHERE d1.d_year BETWEEN 1999 AND 2001
        UNION ALL
        SELECT ics.i_brand_id
        FROM catalog_sales
        JOIN item ics ON catalog_sales.cs_item_sk = ics.i_item_sk
        JOIN date_dim d2 ON catalog_sales.cs_sold_date_sk = d2.d_date_sk
        WHERE d2.d_year BETWEEN 1999 AND 2001
        UNION ALL
        SELECT iws.i_brand_id
        FROM web_sales
        JOIN item iws ON web_sales.ws_item_sk = iws.i_item_sk
        JOIN date_dim d3 ON web_sales.ws_sold_date_sk = d3.d_date_sk
        WHERE d3.d_year BETWEEN 1999 AND 2001
    )
), avg_sales AS (
    SELECT AVG(quantity * list_price) AS average_sales
    FROM (
        SELECT ss_quantity AS quantity, ss_list_price AS list_price
        FROM store_sales
        JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
        WHERE date_dim.d_year BETWEEN 1999 AND 2001
        UNION ALL
        SELECT cs_quantity AS quantity, cs_list_price AS list_price
        FROM catalog_sales
        JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
        WHERE date_dim.d_year BETWEEN 1999 AND 2001
        UNION ALL
        SELECT ws_quantity AS quantity, ws_list_price AS list_price
        FROM web_sales
        JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
        WHERE date_dim.d_year BETWEEN 1999 AND 2001
    ) x
)
SELECT
    channel,
    i_brand_id,
    i_class_id,
    i_category_id,
    SUM(sales),
    SUM(number_sales)
FROM (
    SELECT
        'store' AS channel,
        item.i_brand_id,
        item.i_class_id,
        item.i_category_id,
        SUM(ss_quantity * ss_list_price) AS sales,
        COUNT(*) AS number_sales
    FROM store_sales
    JOIN item ON store_sales.ss_item_sk = item.i_item_sk
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    WHERE item.i_item_sk IN (SELECT ss_item_sk FROM cross_items)
        AND date_dim.d_year = 2001
        AND date_dim.d_moy = 11
    GROUP BY item.i_brand_id, item.i_class_id, item.i_category_id
    HAVING SUM(ss_quantity * ss_list_price) > (SELECT average_sales FROM avg_sales)
    UNION ALL
    SELECT
        'catalog' AS channel,
        item.i_brand_id,
        item.i_class_id,
        item.i_category_id,
        SUM(cs_quantity * cs_list_price) AS sales,
        COUNT(*) AS number_sales
    FROM catalog_sales
    JOIN item ON catalog_sales.cs_item_sk = item.i_item_sk
    JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
    WHERE item.i_item_sk IN (SELECT ss_item_sk FROM cross_items)
        AND date_dim.d_year = 2001
        AND date_dim.d_moy = 11
    GROUP BY item.i_brand_id, item.i_class_id, item.i_category_id
    HAVING SUM(cs_quantity * cs_list_price) > (SELECT average_sales FROM avg_sales)
    UNION ALL
    SELECT
        'web' AS channel,
        item.i_brand_id,
        item.i_class_id,
        item.i_category_id,
        SUM(ws_quantity * ws_list_price) AS sales,
        COUNT(*) AS number_sales
    FROM web_sales
    JOIN item ON web_sales.ws_item_sk = item.i_item_sk
    JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
    WHERE item.i_item_sk IN (SELECT ss_item_sk FROM cross_items)
        AND date_dim.d_year = 2001
        AND date_dim.d_moy = 11
    GROUP BY item.i_brand_id, item.i_class_id, item.i_category_id
    HAVING SUM(ws_quantity * ws_list_price) > (SELECT average_sales FROM avg_sales)
) y
GROUP BY ROLLUP(channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel, i_brand_id, i_class_id, i_category_id
LIMIT 100;

--15
SELECT ca_zip, 
       SUM(cs_sales_price) 
FROM   catalog_sales 
JOIN   customer ON cs_bill_customer_sk = c_customer_sk 
JOIN   customer_address ON c_current_addr_sk = ca_address_sk 
JOIN   date_dim ON cs_sold_date_sk = d_date_sk 
WHERE  (SUBSTR(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792') 
        OR ca_state IN ('CA', 'WA', 'GA') 
        OR cs_sales_price > 500) 
       AND d_qoy = 1 
       AND d_year = 1998 
GROUP BY ca_zip 
ORDER BY ca_zip 
LIMIT 100;

--16
SELECT
    COUNT(DISTINCT cs_order_number) AS `order count`,
    SUM(cs_ext_ship_cost) AS `total shipping cost`,
    SUM(cs_net_profit) AS `total net profit`
FROM
    catalog_sales cs1
JOIN
    date_dim ON cs1.cs_ship_date_sk = d_date_sk
JOIN
    customer_address ON cs1.cs_ship_addr_sk = ca_address_sk
JOIN
    call_center ON cs1.cs_call_center_sk = cc_call_center_sk
WHERE
    d_date BETWEEN '2002-03-01' AND DATE_ADD('2002-03-01', 60)
    AND ca_state = 'IA'
    AND cc_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County')
    AND EXISTS (
        SELECT *
        FROM catalog_sales cs2
        WHERE cs1.cs_order_number = cs2.cs_order_number
        AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk
    )
    AND NOT EXISTS (
        SELECT *
        FROM catalog_returns cr1
        WHERE cs1.cs_order_number = cr1.cr_order_number
    )
GROUP BY
    ca_state
ORDER BY
    `order count`
LIMIT 100;

--17
SELECT 
    i_item_id, 
    i_item_desc, 
    s_state, 
    COUNT(ss_quantity) AS store_sales_quantitycount, 
    AVG(ss_quantity) AS store_sales_quantityave, 
    STDDEV_SAMP(ss_quantity) AS store_sales_quantitystdev, 
    STDDEV_SAMP(ss_quantity) / AVG(ss_quantity) AS store_sales_quantitycov, 
    COUNT(sr_return_quantity) AS store_returns_quantitycount, 
    AVG(sr_return_quantity) AS store_returns_quantityave, 
    STDDEV_SAMP(sr_return_quantity) AS store_returns_quantitystdev, 
    STDDEV_SAMP(sr_return_quantity) / AVG(sr_return_quantity) AS store_returns_quantitycov, 
    COUNT(cs_quantity) AS catalog_sales_quantitycount, 
    AVG(cs_quantity) AS catalog_sales_quantityave, 
    STDDEV_SAMP(cs_quantity) / AVG(cs_quantity) AS catalog_sales_quantitystdev, 
    STDDEV_SAMP(cs_quantity) / AVG(cs_quantity) AS catalog_sales_quantitycov
FROM   
    store_sales
JOIN   
    store_returns ON ss_customer_sk = sr_customer_sk
                  AND ss_item_sk = sr_item_sk
                  AND ss_ticket_number = sr_ticket_number
JOIN   
    catalog_sales ON sr_customer_sk = cs_bill_customer_sk
                  AND sr_item_sk = cs_item_sk
JOIN   
    date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN   
    date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
JOIN   
    date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
JOIN   
    store ON s_store_sk = ss_store_sk
JOIN   
    item ON i_item_sk = ss_item_sk
WHERE  
    d1.d_quarter_name = '1999Q1'
    AND d2.d_quarter_name IN ('1999Q1', '1999Q2', '1999Q3')
    AND d3.d_quarter_name IN ('1999Q1', '1999Q2', '1999Q3')
GROUP BY 
    i_item_id, 
    i_item_desc, 
    s_state
ORDER BY 
    i_item_id, 
    i_item_desc, 
    s_state
LIMIT 100;

--18
SELECT 
    i_item_id, 
    ca_country, 
    ca_state, 
    ca_county, 
    AVG(CAST(cs_quantity AS DECIMAL(12, 2))) AS agg1, 
    AVG(CAST(cs_list_price AS DECIMAL(12, 2))) AS agg2, 
    AVG(CAST(cs_coupon_amt AS DECIMAL(12, 2))) AS agg3, 
    AVG(CAST(cs_sales_price AS DECIMAL(12, 2))) AS agg4, 
    AVG(CAST(cs_net_profit AS DECIMAL(12, 2))) AS agg5, 
    AVG(CAST(c_birth_year AS DECIMAL(12, 2))) AS agg6, 
    AVG(CAST(cd1.cd_dep_count AS DECIMAL(12, 2))) AS agg7 
FROM   
    catalog_sales
JOIN   
    customer_demographics cd1 ON cs_bill_cdemo_sk = cd1.cd_demo_sk
JOIN   
    customer ON cs_bill_customer_sk = c_customer_sk
JOIN   
    customer_address ON c_current_addr_sk = ca_address_sk
JOIN   
    date_dim ON cs_sold_date_sk = d_date_sk
JOIN   
    item ON cs_item_sk = i_item_sk
WHERE  
    cd1.cd_gender = 'F'
    AND cd1.cd_education_status = 'Secondary'
    AND c_birth_month IN (8, 4, 2, 5, 11, 9)
    AND d_year = 2001
    AND ca_state IN ('KS', 'IA', 'AL', 'UT', 'VA', 'NC', 'TX')
GROUP BY 
    ROLLUP(i_item_id, ca_country, ca_state, ca_county)
ORDER BY 
    ca_country, 
    ca_state, 
    ca_county, 
    i_item_id
LIMIT 100;

--19
SELECT 
    i_brand_id AS brand_id, 
    i_brand AS brand, 
    i_manufact_id, 
    i_manufact, 
    SUM(ss_ext_sales_price) AS ext_price 
FROM   
    date_dim
JOIN   
    store_sales ON d_date_sk = ss_sold_date_sk
JOIN   
    item ON ss_item_sk = i_item_sk
JOIN   
    customer ON ss_customer_sk = c_customer_sk
JOIN   
    customer_address ON c_current_addr_sk = ca_address_sk
JOIN   
    store ON ss_store_sk = s_store_sk
WHERE  
    i_manager_id = 38
    AND d_moy = 12
    AND d_year = 1998
    AND SUBSTR(ca_zip, 1, 5) <> SUBSTR(s_zip, 1, 5)
GROUP BY 
    i_brand_id, 
    i_brand, 
    i_manufact_id, 
    i_manufact
ORDER BY 
    ext_price DESC, 
    i_brand_id, 
    i_brand, 
    i_manufact_id, 
    i_manufact
LIMIT 100;

--20
SELECT
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    SUM(cs_ext_sales_price) AS itemrevenue,
    SUM(cs_ext_sales_price) * 100 / SUM(SUM(cs_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM
    catalog_sales
JOIN
    item ON cs_item_sk = i_item_sk
JOIN
    date_dim ON cs_sold_date_sk = d_date_sk
WHERE
    i_category IN ('Children', 'Women', 'Electronics')
    AND d_date BETWEEN CAST('2001-02-03' AS DATE) AND (CAST('2001-02-03' AS DATE) + INTERVAL '30' DAY)
GROUP BY
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price
ORDER BY
    i_category,
    i_class,
    i_item_id,
    i_item_desc,
    revenueratio
LIMIT 100;

--21
SELECT *
FROM (
    SELECT
        w_warehouse_name,
        i_item_id,
        SUM(
            CASE
                WHEN (CAST(d_date AS DATE) < CAST('2000-05-13' AS DATE)) THEN inv_quantity_on_hand
                ELSE 0
            END) AS inv_before,
        SUM(
            CASE
                WHEN (CAST(d_date AS DATE) >= CAST('2000-05-13' AS DATE)) THEN inv_quantity_on_hand
                ELSE 0
            END) AS inv_after
    FROM
        inventory
    JOIN
        warehouse ON inv_warehouse_sk = w_warehouse_sk
    JOIN
        item ON i_item_sk = inv_item_sk
    JOIN
        date_dim ON inv_date_sk = d_date_sk
    WHERE
        i_current_price BETWEEN 0.99 AND 1.49
        AND d_date BETWEEN (CAST('2000-05-13' AS DATE) - INTERVAL '30' DAY) AND (CAST('2000-05-13' AS DATE) + INTERVAL '30' DAY)
    GROUP BY
        w_warehouse_name,
        i_item_id
) x
WHERE
    (CASE WHEN inv_before > 0 THEN inv_after / inv_before ELSE NULL END) BETWEEN 2.0/3.0 AND 3.0/2.0
ORDER BY
    w_warehouse_name,
    i_item_id
LIMIT 100;

--22
SELECT
    i_product_name,
    i_brand,
    i_class,
    i_category,
    AVG(inv_quantity_on_hand) AS qoh
FROM
    inventory
JOIN
    date_dim ON inv_date_sk = d_date_sk
JOIN
    item ON inv_item_sk = i_item_sk
JOIN
    warehouse ON inv_warehouse_sk = w_warehouse_sk
WHERE
    d_month_seq BETWEEN 1205 AND 1205 + 11
GROUP BY
    ROLLUP(i_product_name, i_brand, i_class, i_category)
ORDER BY
    qoh,
    i_product_name,
    i_brand,
    i_class,
    i_category
LIMIT 100;

--23
WITH frequent_ss_items AS (
    SELECT
        SUBSTR(i_item_desc, 1, 30) AS itemdesc,
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
        SUBSTR(i_item_desc, 1, 30),
        i_item_sk,
        d_date
    HAVING
        COUNT(*) > 4
),

max_store_sales AS (
    SELECT
        MAX(csales) AS tpcds_cmax
    FROM
        (
            SELECT
                c_customer_sk,
                SUM(ss_quantity * ss_sales_price) AS csales
            FROM
                store_sales
                JOIN customer ON ss_customer_sk = c_customer_sk
                JOIN date_dim ON ss_sold_date_sk = d_date_sk
            WHERE
                d_year IN (1998, 1998 + 1, 1998 + 2, 1998 + 3)
            GROUP BY
                c_customer_sk
        ) max_store_sales
),

best_ss_customer AS (
    SELECT
        c_customer_sk
    FROM
        (
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
        ) best_ss_customer
)

SELECT
    SUM(sales)
FROM
    (
        SELECT
            cs_quantity * cs_list_price AS sales
        FROM
            catalog_sales
            JOIN date_dim ON cs_sold_date_sk = d_date_sk
        WHERE
            d_year = 1998
            AND d_moy = 6
            AND cs_item_sk IN (SELECT item_sk FROM frequent_ss_items)
            AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
        UNION ALL
        SELECT
            ws_quantity * ws_list_price AS sales
        FROM
            web_sales
            JOIN date_dim ON ws_sold_date_sk = d_date_sk
        WHERE
            d_year = 1998
            AND d_moy = 6
            AND ws_item_sk IN (SELECT item_sk FROM frequent_ss_items)
            AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
    ) sales_limit
LIMIT 100;

--24
WITH ssales AS (
    SELECT c_last_name,
           c_first_name,
           s_store_name,
           ca_state,
           s_state,
           i_color,
           i_current_price,
           i_manager_id,
           i_units,
           i_size,
           Sum(ss_net_profit) AS netpaid
    FROM   store_sales
           JOIN store_returns ON ss_ticket_number = sr_ticket_number
                                AND ss_item_sk = sr_item_sk
           JOIN store ON ss_store_sk = s_store_sk
           JOIN item ON ss_item_sk = i_item_sk
           JOIN customer ON ss_customer_sk = c_customer_sk
           JOIN customer_address ON c_birth_country = Upper(ca_country)
                                      AND s_zip = ca_zip
    WHERE  s_market_id = 6
    GROUP  BY c_last_name,
              c_first_name,
              s_store_name,
              ca_state,
              s_state,
              i_color,
              i_current_price,
              i_manager_id,
              i_units,
              i_size
)
SELECT c_last_name,
       c_first_name,
       s_store_name,
       Sum(netpaid) AS paid
FROM   ssales
WHERE  i_color = 'papaya'
GROUP  BY c_last_name,
          c_first_name,
          s_store_name
HAVING Sum(netpaid) > (SELECT 0.05 * Avg(netpaid) FROM ssales);

--25
SELECT i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       Max(ss_net_profit) AS store_sales_profit,
       Max(sr_net_loss) AS store_returns_loss,
       Max(cs_net_profit) AS catalog_sales_profit
FROM store_sales
JOIN store_returns ON ss_customer_sk = sr_customer_sk
                   AND ss_item_sk = sr_item_sk
                   AND ss_ticket_number = sr_ticket_number
JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
                   AND sr_item_sk = cs_item_sk
JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE d1.d_moy = 4
      AND d1.d_year = 2001
      AND d2.d_moy BETWEEN 4 AND 10
      AND d2.d_year = 2001
      AND d3.d_moy BETWEEN 4 AND 10
      AND d3.d_year = 2001
GROUP BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
ORDER BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
LIMIT 100;

--26
SELECT i_item_id,
       Avg(cs_quantity) AS agg1,
       Avg(cs_list_price) AS agg2,
       Avg(cs_coupon_amt) AS agg3,
       Avg(cs_sales_price) AS agg4
FROM catalog_sales
JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
JOIN item ON cs_item_sk = i_item_sk
JOIN promotion ON cs_promo_sk = p_promo_sk
WHERE cd_gender = 'F'
      AND cd_marital_status = 'W'
      AND cd_education_status = 'Secondary'
      AND (p_channel_email = 'N' OR p_channel_event = 'N')
      AND d_year = 2000
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100;

--27
SELECT i_item_id,
       s_state,
       COUNT(*) AS count_items,
       AVG(ss_quantity) AS avg_quantity,
       AVG(ss_list_price) AS avg_list_price,
       AVG(ss_coupon_amt) AS avg_coupon_amt,
       AVG(ss_sales_price) AS avg_sales_price
FROM store_sales
JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE cd_gender = 'M'
      AND cd_marital_status = 'D'
      AND cd_education_status = 'College'
      AND d_year = 2000
      AND s_state IN ('TN')
GROUP BY i_item_id, s_state
ORDER BY i_item_id, s_state
LIMIT 100;

--28
SELECT *
FROM
  (SELECT AVG(B1.ss_list_price) AS B1_LP,
          COUNT(B1.ss_list_price) AS B1_CNT,
          COUNT(DISTINCT B1.ss_list_price) AS B1_CNTD
   FROM
     (SELECT ss_list_price
      FROM store_sales
      WHERE ss_quantity BETWEEN 0 AND 5
        AND (ss_list_price BETWEEN 18 AND 18 + 10
             OR ss_coupon_amt BETWEEN 1939 AND 1939 + 1000
             OR ss_wholesale_cost BETWEEN 34 AND 34 + 20)) AS B1) B1,
  (SELECT AVG(B2.ss_list_price) AS B2_LP,
          COUNT(B2.ss_list_price) AS B2_CNT,
          COUNT(DISTINCT B2.ss_list_price) AS B2_CNTD
   FROM
     (SELECT ss_list_price
      FROM store_sales
      WHERE ss_quantity BETWEEN 6 AND 10
        AND (ss_list_price BETWEEN 1 AND 1 + 10
             OR ss_coupon_amt BETWEEN 35 AND 35 + 1000
             OR ss_wholesale_cost BETWEEN 50 AND 50 + 20)) AS B2) B2,
  (SELECT AVG(B3.ss_list_price) AS B3_LP,
          COUNT(B3.ss_list_price) AS B3_CNT,
          COUNT(DISTINCT B3.ss_list_price) AS B3_CNTD
   FROM
     (SELECT ss_list_price
      FROM store_sales
      WHERE ss_quantity BETWEEN 11 AND 15
        AND (ss_list_price BETWEEN 91 AND 91 + 10
             OR ss_coupon_amt BETWEEN 1412 AND 1412 + 1000
             OR ss_wholesale_cost BETWEEN 17 AND 17 + 20)) AS B3) B3,
  (SELECT AVG(B4.ss_list_price) AS B4_LP,
          COUNT(B4.ss_list_price) AS B4_CNT,
          COUNT(DISTINCT B4.ss_list_price) AS B4_CNTD
   FROM
     (SELECT ss_list_price
      FROM store_sales
      WHERE ss_quantity BETWEEN 16 AND 20
        AND (ss_list_price BETWEEN 9 AND 9 + 10
             OR ss_coupon_amt BETWEEN 5270 AND 5270 + 1000
             OR ss_wholesale_cost BETWEEN 29 AND 29 + 20)) AS B4) B4,
  (SELECT AVG(B5.ss_list_price) AS B5_LP,
          COUNT(B5.ss_list_price) AS B5_CNT,
          COUNT(DISTINCT B5.ss_list_price) AS B5_CNTD
   FROM
     (SELECT ss_list_price
      FROM store_sales
      WHERE ss_quantity BETWEEN 21 AND 25
        AND (ss_list_price BETWEEN 45 AND 45 + 10
             OR ss_coupon_amt BETWEEN 826 AND 826 + 1000
             OR ss_wholesale_cost BETWEEN 5 AND 5 + 20)) AS B5) B5,
  (SELECT AVG(B6.ss_list_price) AS B6_LP,
          COUNT(B6.ss_list_price) AS B6_CNT,
          COUNT(DISTINCT B6.ss_list_price) AS B6_CNTD
   FROM
     (SELECT ss_list_price
      FROM store_sales
      WHERE ss_quantity BETWEEN 26 AND 30
        AND (ss_list_price BETWEEN 174 AND 174 + 10
             OR ss_coupon_amt BETWEEN 5548 AND 5548 + 1000
             OR ss_wholesale_cost BETWEEN 42 AND 42 + 20)) AS B6) B6
LIMIT 100;

--29
SELECT i_item_id, 
       i_item_desc, 
       s_store_id, 
       s_store_name, 
       AVG(ss_quantity) AS store_sales_quantity, 
       AVG(sr_return_quantity) AS store_returns_quantity, 
       AVG(cs_quantity) AS catalog_sales_quantity 
FROM store_sales 
JOIN store_returns ON store_sales.ss_customer_sk = store_returns.sr_customer_sk 
                    AND store_sales.ss_item_sk = store_returns.sr_item_sk 
                    AND store_sales.ss_ticket_number = store_returns.sr_ticket_number 
JOIN catalog_sales ON store_returns.sr_customer_sk = catalog_sales.cs_bill_customer_sk 
                    AND store_returns.sr_item_sk = catalog_sales.cs_item_sk 
                    AND store_sales.ss_sold_date_sk = catalog_sales.cs_sold_date_sk 
JOIN date_dim d1 ON store_sales.ss_sold_date_sk = d1.d_date_sk 
JOIN date_dim d2 ON store_returns.sr_returned_date_sk = d2.d_date_sk 
JOIN date_dim d3 ON catalog_sales.cs_sold_date_sk = d3.d_date_sk 
JOIN store ON store_sales.ss_store_sk = store.s_store_sk 
JOIN item ON store_sales.ss_item_sk = item.i_item_sk 
WHERE d1.d_moy = 4 
      AND d1.d_year = 1998 
      AND d2.d_moy BETWEEN 4 AND 4 + 3 
      AND d2.d_year = 1998 
      AND d3.d_year IN (1998, 1998 + 1, 1998 + 2) 
GROUP BY i_item_id, 
         i_item_desc, 
         s_store_id, 
         s_store_name 
ORDER BY i_item_id, 
         i_item_desc, 
         s_store_id, 
         s_store_name 
LIMIT 100;

--30
WITH customer_total_return AS (
    SELECT wr_returning_customer_sk AS ctr_customer_sk,
           ca_state AS ctr_state,
           SUM(wr_return_amt) AS ctr_total_return
    FROM web_returns
    JOIN date_dim ON wr_returned_date_sk = d_date_sk AND d_year = 2000
    JOIN customer_address ON wr_returning_addr_sk = ca_address_sk
    GROUP BY wr_returning_customer_sk, ca_state
)
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
FROM customer_total_return ctr1
JOIN customer_address ON ctr1.ctr_customer_sk = customer_address.ca_address_sk AND ca_state = 'IN'
JOIN customer ON ctr1.ctr_customer_sk = customer.c_customer_sk
WHERE ctr1.ctr_total_return > (SELECT AVG(ctr_total_return) * 1.2
                                FROM customer_total_return ctr2
                                WHERE ctr1.ctr_state = ctr2.ctr_state)
ORDER BY c_customer_id, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, ctr_total_return
LIMIT 100;

--31
WITH ss AS (
    SELECT ca_county,
           d_qoy,
           d_year,
           SUM(ss_ext_sales_price) AS store_sales
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    GROUP BY ca_county, d_qoy, d_year
),
ws AS (
    SELECT ca_county,
           d_qoy,
           d_year,
           SUM(ws_ext_sales_price) AS web_sales
    FROM web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
    GROUP BY ca_county, d_qoy, d_year
)
SELECT ss1.ca_county,
       ss1.d_year,
       ws2.web_sales / ws1.web_sales AS web_q1_q2_increase,
       ss2.store_sales / ss1.store_sales AS store_q1_q2_increase,
       ws3.web_sales / ws2.web_sales AS web_q2_q3_increase,
       ss3.store_sales / ss2.store_sales AS store_q2_q3_increase
FROM ss ss1
JOIN ss ss2 ON ss1.ca_county = ss2.ca_county AND ss1.d_year = ss2.d_year
JOIN ss ss3 ON ss1.ca_county = ss3.ca_county AND ss1.d_year = ss3.d_year
JOIN ws ws1 ON ss1.ca_county = ws1.ca_county AND ss1.d_year = ws1.d_year
JOIN ws ws2 ON ss1.ca_county = ws2.ca_county AND ss1.d_year = ws2.d_year
JOIN ws ws3 ON ss1.ca_county = ws3.ca_county AND ss1.d_year = ws3.d_year
WHERE ss1.d_qoy = 1
  AND ss1.d_year = 2001
  AND ss2.d_qoy = 2
  AND ss3.d_qoy = 3
  AND ws1.d_qoy = 1
  AND ws2.d_qoy = 2
  AND ws3.d_qoy = 3
  AND ws1.web_sales > 0
  AND ss1.store_sales > 0
  AND ws2.web_sales > 0
  AND ss2.store_sales > 0
  AND ws3.web_sales > 0
  AND ss3.store_sales > 0
  AND ws2.web_sales / ws1.web_sales > ss2.store_sales / ss1.store_sales
  AND ws3.web_sales / ws2.web_sales > ss3.store_sales / ss2.store_sales
ORDER BY ss1.d_year;

--32
SELECT 
       SUM(cs_ext_discount_amt) AS `excess discount amount`
FROM   catalog_sales
JOIN   item ON i_item_sk = cs_item_sk
JOIN   date_dim ON d_date_sk = cs_sold_date_sk
WHERE  i_manufact_id = 610 
AND    d_date BETWEEN '2001-03-04' AND DATE_ADD('2001-03-04', 90)
AND    cs_ext_discount_amt > 
       ( 
              SELECT 1.3 * AVG(cs_ext_discount_amt) 
              FROM   catalog_sales 
              JOIN   date_dim ON d_date_sk = cs_sold_date_sk
              WHERE  i_item_sk = cs_item_sk 
              AND    d_date BETWEEN '2001-03-04' AND DATE_ADD('2001-03-04', 90)
       ) 
LIMIT 100;

--33
WITH ss AS (
    SELECT i_manufact_id, 
           SUM(ss_ext_sales_price) AS total_sales 
    FROM   store_sales
    JOIN   date_dim ON ss_sold_date_sk = d_date_sk
    JOIN   customer_address ON ss_addr_sk = ca_address_sk
    JOIN   item ON ss_item_sk = i_item_sk
    WHERE  i_manufact_id IN (SELECT i_manufact_id 
                              FROM   item 
                              WHERE  i_category IN ('Books')) 
           AND d_year = 1999 
           AND d_moy = 3 
           AND ca_gmt_offset = -5 
    GROUP BY i_manufact_id
),
cs AS (
    SELECT i_manufact_id, 
           SUM(cs_ext_sales_price) AS total_sales 
    FROM   catalog_sales
    JOIN   date_dim ON cs_sold_date_sk = d_date_sk
    JOIN   customer_address ON cs_bill_addr_sk = ca_address_sk
    JOIN   item ON cs_item_sk = i_item_sk
    WHERE  i_manufact_id IN (SELECT i_manufact_id 
                              FROM   item 
                              WHERE  i_category IN ('Books')) 
           AND d_year = 1999 
           AND d_moy = 3 
           AND ca_gmt_offset = -5 
    GROUP BY i_manufact_id
),
ws AS (
    SELECT i_manufact_id, 
           SUM(ws_ext_sales_price) AS total_sales 
    FROM   web_sales
    JOIN   date_dim ON ws_sold_date_sk = d_date_sk
    JOIN   customer_address ON ws_bill_addr_sk = ca_address_sk
    JOIN   item ON ws_item_sk = i_item_sk
    WHERE  i_manufact_id IN (SELECT i_manufact_id 
                              FROM   item 
                              WHERE  i_category IN ('Books')) 
           AND d_year = 1999 
           AND d_moy = 3 
           AND ca_gmt_offset = -5 
    GROUP BY i_manufact_id
)
SELECT i_manufact_id, 
       SUM(total_sales) AS total_sales 
FROM   (
    SELECT * FROM ss 
    UNION ALL 
    SELECT * FROM cs 
    UNION ALL 
    SELECT * FROM ws
) tmp1 
GROUP BY i_manufact_id 
ORDER BY total_sales
LIMIT 100;

--34
SELECT c_last_name, 
       c_first_name, 
       c_salutation, 
       c_preferred_cust_flag, 
       ss_ticket_number, 
       cnt 
FROM   (
    SELECT ss_ticket_number, 
           ss_customer_sk, 
           COUNT(*) cnt 
    FROM   store_sales
    JOIN   date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk 
    JOIN   store ON store_sales.ss_store_sk = store.s_store_sk 
    JOIN   household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk 
    WHERE  (date_dim.d_dom BETWEEN 1 AND 3 OR date_dim.d_dom BETWEEN 25 AND 28) 
           AND (household_demographics.hd_buy_potential = '>10000' OR household_demographics.hd_buy_potential = 'unknown') 
           AND household_demographics.hd_vehicle_count > 0 
           AND (CASE WHEN household_demographics.hd_vehicle_count > 0 THEN household_demographics.hd_dep_count / household_demographics.hd_vehicle_count ELSE NULL END) > 1.2 
           AND date_dim.d_year IN (1999, 1999 + 1, 1999 + 2) 
           AND store.s_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County') 
    GROUP BY ss_ticket_number, ss_customer_sk
) dn
JOIN   customer ON ss_customer_sk = c_customer_sk 
WHERE  cnt BETWEEN 15 AND 20 
ORDER BY c_last_name, c_first_name, c_salutation, c_preferred_cust_flag DESC;

--35
SELECT ca_state, 
       cd_gender, 
       cd_marital_status, 
       cd_dep_count, 
       COUNT(*) AS cnt1, 
       STDDEV_SAMP(cd_dep_count), 
       AVG(cd_dep_count), 
       MAX(cd_dep_count), 
       cd_dep_employed_count, 
       COUNT(*) AS cnt2, 
       STDDEV_SAMP(cd_dep_employed_count), 
       AVG(cd_dep_employed_count), 
       MAX(cd_dep_employed_count), 
       cd_dep_college_count, 
       COUNT(*) AS cnt3, 
       STDDEV_SAMP(cd_dep_college_count), 
       AVG(cd_dep_college_count), 
       MAX(cd_dep_college_count) 
FROM   customer c 
JOIN   customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk 
JOIN   customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk 
WHERE  EXISTS (
           SELECT * 
           FROM   store_sales 
           JOIN   date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk 
           WHERE  c.c_customer_sk = store_sales.ss_customer_sk 
                  AND d_year = 2001 
                  AND d_qoy < 4
       ) 
       AND (
           EXISTS (
               SELECT * 
               FROM   web_sales 
               JOIN   date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk 
               WHERE  c.c_customer_sk = web_sales.ws_bill_customer_sk 
                      AND d_year = 2001 
                      AND d_qoy < 4
           ) 
           OR EXISTS (
               SELECT * 
               FROM   catalog_sales 
               JOIN   date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk 
               WHERE  c.c_customer_sk = catalog_sales.cs_ship_customer_sk 
                      AND d_year = 2001 
                      AND d_qoy < 4
           )
       )
GROUP BY ca_state, 
         cd_gender, 
         cd_marital_status, 
         cd_dep_count, 
         cd_dep_employed_count, 
         cd_dep_college_count 
ORDER BY ca_state, 
         cd_gender, 
         cd_marital_status, 
         cd_dep_count, 
         cd_dep_employed_count, 
         cd_dep_college_count
LIMIT 100;

--36
SELECT 
    gross_margin, 
    i_category, 
    i_class, 
    lochierarchy, 
    rank_within_parent 
FROM (
    SELECT 
        gross_margin,
        i_category, 
        i_class, 
        lochierarchy, 
        RANK() OVER (
            PARTITION BY lochierarchy, 
            CASE WHEN lochierarchy = 0 THEN i_category END 
            ORDER BY gross_margin ASC
        ) AS rank_within_parent 
    FROM (
        SELECT 
            SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin, 
            i_category, 
            i_class, 
            CASE WHEN i_category IS NOT NULL THEN 1 ELSE 0 END + CASE WHEN i_class IS NOT NULL THEN 1 ELSE 0 END AS lochierarchy
        FROM   
            store_sales 
            JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk 
            JOIN item ON i_item_sk = ss_item_sk 
            JOIN store ON s_store_sk = ss_store_sk 
        WHERE  
            d1.d_year = 2000 
            AND s_state IN ( 'TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN', 'TN' ) 
        GROUP BY 
            i_category, 
            i_class
    ) t1
) t2
ORDER BY 
    lochierarchy DESC, 
    CASE WHEN lochierarchy = 0 THEN i_category END, 
    rank_within_parent
LIMIT 100;

--37
SELECT 
    i_item_id,
    i_item_desc,
    i_current_price
FROM
    item
JOIN
    inventory ON inv_item_sk = i_item_sk
JOIN
    date_dim ON d_date_sk = inv_date_sk
JOIN
    catalog_sales ON cs_item_sk = i_item_sk
WHERE
    i_current_price BETWEEN 20 AND 20 + 30
    AND d_date BETWEEN '1999-03-06' AND date_add('1999-03-06', 60)
    AND i_manufact_id IN (843, 815, 850, 840)
    AND inv_quantity_on_hand BETWEEN 100 AND 500
GROUP BY
    i_item_id,
    i_item_desc,
    i_current_price
ORDER BY
    i_item_id
LIMIT 100;

--38
SELECT COUNT(*) 
FROM (
    SELECT DISTINCT 
        c_last_name, 
        c_first_name, 
        d_date 
    FROM   
        store_sales
    JOIN   
        date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk 
    JOIN   
        customer ON store_sales.ss_customer_sk = customer.c_customer_sk 
    WHERE  
        d_month_seq BETWEEN 1188 AND 1188 + 11 
    
    UNION
    
    SELECT DISTINCT 
        c_last_name, 
        c_first_name, 
        d_date 
    FROM   
        catalog_sales
    JOIN   
        date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk 
    JOIN   
        customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk 
    WHERE  
        d_month_seq BETWEEN 1188 AND 1188 + 11 
        
    UNION
    
    SELECT DISTINCT 
        c_last_name, 
        c_first_name, 
        d_date 
    FROM   
        web_sales
    JOIN   
        date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk 
    JOIN   
        customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk 
    WHERE  
        d_month_seq BETWEEN 1188 AND 1188 + 11
) hot_cust;

--39
WITH inv AS (
    SELECT
        w_warehouse_name,
        w_warehouse_sk,
        i_item_sk,
        d_moy,
        stdev,
        mean,
        CASE
            WHEN mean = 0 THEN NULL
            ELSE stdev / mean
        END AS cov
    FROM (
        SELECT
            w_warehouse_name,
            w_warehouse_sk,
            i_item_sk,
            d_moy,
            STDDEV_SAMP(inv_quantity_on_hand) AS stdev,
            AVG(inv_quantity_on_hand) AS mean
        FROM
            inventory
        JOIN
            item ON inv_item_sk = i_item_sk
        JOIN
            warehouse ON inv_warehouse_sk = w_warehouse_sk
        JOIN
            date_dim ON inv_date_sk = d_date_sk
        WHERE
            d_year = 2002
        GROUP BY
            w_warehouse_name,
            w_warehouse_sk,
            i_item_sk,
            d_moy
    ) foo
    WHERE
        CASE
            WHEN mean = 0 THEN 0
            ELSE stdev / mean
        END > 1
)
SELECT
    inv1.w_warehouse_sk,
    inv1.i_item_sk,
    inv1.d_moy,
    inv1.mean,
    inv1.cov,
    inv2.w_warehouse_sk,
    inv2.i_item_sk,
    inv2.d_moy,
    inv2.mean,
    inv2.cov
FROM
    inv inv1
JOIN
    inv inv2 ON inv1.i_item_sk = inv2.i_item_sk
             AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
             AND inv1.d_moy = 1
             AND inv2.d_moy = 1 + 1
WHERE
    inv1.cov > 1.5
ORDER BY
    inv1.w_warehouse_sk,
    inv1.i_item_sk,
    inv1.d_moy,
    inv1.mean,
    inv1.cov,
    inv2.d_moy,
    inv2.mean,
    inv2.cov;

--40
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

--41
SELECT DISTINCT(i_product_name)
FROM item i1
WHERE i_manufact_id BETWEEN 765 AND 765 + 40
AND (
    SELECT COUNT(*) AS item_cnt
    FROM item
    WHERE (
        (i_manufact = i1.i_manufact AND (
            (i_category = 'Women' AND (i_color = 'dim' OR i_color = 'green') AND (i_units = 'Gross' OR i_units = 'Dozen') AND (i_size = 'economy' OR i_size = 'petite'))
            OR (i_category = 'Women' AND (i_color = 'navajo' OR i_color = 'aquamarine') AND (i_units = 'Case' OR i_units = 'Unknown') AND (i_size = 'large' OR i_size = 'N/A'))
            OR (i_category = 'Men' AND (i_color = 'indian' OR i_color = 'dark') AND (i_units = 'Oz' OR i_units = 'Lb') AND (i_size = 'extra large' OR i_size = 'small'))
            OR (i_category = 'Men' AND (i_color = 'peach' OR i_color = 'purple') AND (i_units = 'Tbl' OR i_units = 'Bunch') AND (i_size = 'economy' OR i_size = 'petite'))
        ))
        OR (i_manufact = i1.i_manufact AND (
            (i_category = 'Women' AND (i_color = 'orchid' OR i_color = 'peru') AND (i_units = 'Carton' OR i_units = 'Cup') AND (i_size = 'economy' OR i_size = 'petite'))
            OR (i_category = 'Women' AND (i_color = 'violet' OR i_color = 'papaya') AND (i_units = 'Ounce' OR i_units = 'Box') AND (i_size = 'large' OR i_size = 'N/A'))
            OR (i_category = 'Men' AND (i_color = 'drab' OR i_color = 'grey') AND (i_units = 'Each' OR i_units = 'N/A') AND (i_size = 'extra large' OR i_size = 'small'))
            OR (i_category = 'Men' AND (i_color = 'chocolate' OR i_color = 'antique') AND (i_units = 'Dram' OR i_units = 'Gram') AND (i_size = 'economy' OR i_size = 'petite'))
        ))
    )
) > 0
ORDER BY i_product_name
LIMIT 100;

--42
SELECT dt.d_year,
       item.i_category_id,
       item.i_category,
       SUM(ss_ext_sales_price)
FROM date_dim dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE item.i_manager_id = 1
      AND dt.d_moy = 12
      AND dt.d_year = 2000
GROUP BY dt.d_year,
         item.i_category_id,
         item.i_category
ORDER BY SUM(ss_ext_sales_price) DESC,
         dt.d_year,
         item.i_category_id,
         item.i_category
LIMIT 100;

--43
SELECT s_store_name,
       s_store_id,
       SUM(CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE NULL END) AS sun_sales,
       SUM(CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE NULL END) AS mon_sales,
       SUM(CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE NULL END) AS tue_sales,
       SUM(CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE NULL END) AS wed_sales,
       SUM(CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE NULL END) AS thu_sales,
       SUM(CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE NULL END) AS fri_sales,
       SUM(CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE NULL END) AS sat_sales
FROM date_dim
JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
JOIN store ON store.s_store_sk = store_sales.ss_store_sk
WHERE date_dim.d_year = 2002
      AND store.s_gmt_offset = -5
GROUP BY s_store_name,
         s_store_id
ORDER BY s_store_name,
         s_store_id,
         sun_sales,
         mon_sales,
         tue_sales,
         wed_sales,
         thu_sales,
         fri_sales,
         sat_sales
LIMIT 100;

--44
WITH ascending AS (
    SELECT ss_item_sk AS item_sk,
           AVG(ss_net_profit) AS rank_col,
           RANK() OVER (ORDER BY AVG(ss_net_profit) ASC) AS rnk
    FROM store_sales
    WHERE ss_store_sk = 4
          AND ss_cdemo_sk IS NULL
    GROUP BY ss_item_sk
    HAVING AVG(ss_net_profit) > 0.9 * (
        SELECT AVG(ss_net_profit)
        FROM store_sales
        WHERE ss_store_sk = 4
              AND ss_cdemo_sk IS NULL
        GROUP BY ss_store_sk
    )
    LIMIT 10
),
descending AS (
    SELECT ss_item_sk AS item_sk,
           AVG(ss_net_profit) AS rank_col,
           RANK() OVER (ORDER BY AVG(ss_net_profit) DESC) AS rnk
    FROM store_sales
    WHERE ss_store_sk = 4
          AND ss_cdemo_sk IS NULL
    GROUP BY ss_item_sk
    HAVING AVG(ss_net_profit) > 0.9 * (
        SELECT AVG(ss_net_profit)
        FROM store_sales
        WHERE ss_store_sk = 4
              AND ss_cdemo_sk IS NULL
        GROUP BY ss_store_sk
    )
    LIMIT 10
)
SELECT a.rnk,
       i1.i_product_name AS best_performing,
       i2.i_product_name AS worst_performing
FROM ascending a
JOIN descending d ON a.rnk = d.rnk
JOIN item i1 ON i1.i_item_sk = a.item_sk
JOIN item i2 ON i2.i_item_sk = d.item_sk
ORDER BY a.rnk
LIMIT 100;

--45
SELECT ca_zip,
       ca_state,
       SUM(ws_sales_price)
FROM web_sales
JOIN customer ON ws_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
JOIN item ON ws_item_sk = i_item_sk
WHERE (SUBSTR(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
       OR i_item_id IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29))
      AND d_qoy = 1
      AND d_year = 2000
GROUP BY ca_zip,
         ca_state
ORDER BY ca_zip,
         ca_state
LIMIT 100;

--46
SELECT c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       amt,
       profit
FROM (
    SELECT ss_ticket_number,
           ss_customer_sk,
           ca_city AS bought_city,
           SUM(ss_coupon_amt) AS amt,
           SUM(ss_net_profit) AS profit
    FROM store_sales
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
    WHERE household_demographics.hd_dep_count = 6
          OR household_demographics.hd_vehicle_count = 0
          AND date_dim.d_dow IN (6, 0)
          AND date_dim.d_year IN (2000, 2000 + 1, 2000 + 2)
          AND store.s_city IN ('Midway', 'Fairview', 'Fairview', 'Fairview', 'Fairview')
    GROUP BY ss_ticket_number,
             ss_customer_sk,
             ss_addr_sk,
             ca_city
) dn
JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
JOIN customer_address current_addr ON customer.c_current_addr_sk = current_addr.ca_address_sk
WHERE current_addr.ca_city <> bought_city
ORDER BY c_last_name,
         c_first_name,
         ca_city,
         bought_city,
         ss_ticket_number
LIMIT 100;

--47
WITH v1 AS (
    SELECT i_category,
           i_brand,
           s_store_name,
           s_company_name,
           d_year,
           d_moy,
           SUM(ss_sales_price) AS sum_sales,
           AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) AS avg_monthly_sales,
           RANK() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year, d_moy) AS rn
    FROM item
    JOIN store_sales ON ss_item_sk = i_item_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN store ON ss_store_sk = s_store_sk
    WHERE d_year = 1999
          OR (d_year = 1999 - 1 AND d_moy = 12)
          OR (d_year = 1999 + 1 AND d_moy = 1)
    GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
),
v2 AS (
    SELECT v1.i_category,
           v1.d_year,
           v1.d_moy,
           v1.avg_monthly_sales,
           v1.sum_sales,
           v1_lag.sum_sales AS psum,
           v1_lead.sum_sales AS nsum
    FROM v1
    JOIN v1 v1_lag ON v1.i_category = v1_lag.i_category
                   AND v1.i_brand = v1_lag.i_brand
                   AND v1.s_store_name = v1_lag.s_store_name
                   AND v1.s_company_name = v1_lag.s_company_name
                   AND v1.rn = v1_lag.rn + 1
    JOIN v1 v1_lead ON v1.i_category = v1_lead.i_category
                    AND v1.i_brand = v1_lead.i_brand
                    AND v1.s_store_name = v1_lead.s_store_name
                    AND v1.s_company_name = v1_lead.s_company_name
                    AND v1.rn = v1_lead.rn - 1
)
SELECT *
FROM v2
WHERE d_year = 1999
      AND avg_monthly_sales > 0
      AND CASE
            WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
            ELSE NULL
          END > 0.1
ORDER BY sum_sales - avg_monthly_sales, d_moy
LIMIT 100;

--48
SELECT SUM(ss_quantity)
FROM store_sales
JOIN store ON s_store_sk = ss_store_sk
JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE d_year = 1999
      AND (
          (cd_marital_status = 'W' AND cd_education_status = 'Secondary' AND ss_sales_price BETWEEN 100.00 AND 150.00) OR
          (cd_marital_status = 'M' AND cd_education_status = 'Advanced Degree' AND ss_sales_price BETWEEN 50.00 AND 100.00) OR
          (cd_marital_status = 'D' AND cd_education_status = '2 yr Degree' AND ss_sales_price BETWEEN 150.00 AND 200.00)
      )
      AND (
          (ca_country = 'United States' AND ca_state IN ('TX', 'NE', 'MO') AND ss_net_profit BETWEEN 0 AND 2000) OR
          (ca_country = 'United States' AND ca_state IN ('CO', 'TN', 'ND') AND ss_net_profit BETWEEN 150 AND 3000) OR
          (ca_country = 'United States' AND ca_state IN ('OK', 'PA', 'CA') AND ss_net_profit BETWEEN 50 AND 25000)
      );

--49
SELECT 'web' AS channel, 
       web.item, 
       web.return_ratio, 
       web.return_rank, 
       web.currency_rank 
FROM   (SELECT item, 
               return_ratio, 
               currency_ratio, 
               RANK() OVER (ORDER BY return_ratio) AS return_rank, 
               RANK() OVER (ORDER BY currency_ratio) AS currency_rank 
        FROM   (SELECT ws.ws_item_sk AS item, 
                       CAST(SUM(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15, 4)) AS return_ratio, 
                       CAST(SUM(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15, 4)) AS currency_ratio 
                FROM   web_sales ws 
                LEFT OUTER JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number AND ws.ws_item_sk = wr.wr_item_sk) 
                JOIN date_dim ON ws.ws_sold_date_sk = date_dim.d_date_sk 
                WHERE  wr.wr_return_amt > 10000 
                       AND ws.ws_net_profit > 1 
                       AND ws.ws_net_paid > 0 
                       AND ws.ws_quantity > 0 
                       AND d_year = 1999 
                       AND d_moy = 12 
                GROUP  BY ws.ws_item_sk) in_web) web 
WHERE  web.return_rank <= 10 OR web.currency_rank <= 10 

UNION 

SELECT 'catalog' AS channel, 
       catalog.item, 
       catalog.return_ratio, 
       catalog.return_rank, 
       catalog.currency_rank 
FROM   (SELECT item, 
               return_ratio, 
               currency_ratio, 
               RANK() OVER (ORDER BY return_ratio) AS return_rank, 
               RANK() OVER (ORDER BY currency_ratio) AS currency_rank 
        FROM   (SELECT cs.cs_item_sk AS item, 
                       CAST(SUM(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15, 4)) AS return_ratio, 
                       CAST(SUM(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15, 4)) AS currency_ratio 
                FROM   catalog_sales cs 
                LEFT OUTER JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number AND cs.cs_item_sk = cr.cr_item_sk) 
                JOIN date_dim ON cs.cs_sold_date_sk = date_dim.d_date_sk 
                WHERE  cr.cr_return_amount > 10000 
                       AND cs.cs_net_profit > 1 
                       AND cs.cs_net_paid > 0 
                       AND cs.cs_quantity > 0 
                       AND d_year = 1999 
                       AND d_moy = 12 
                GROUP  BY cs.cs_item_sk) in_cat) catalog 
WHERE  catalog.return_rank <= 10 OR catalog.currency_rank <= 10 

UNION 

SELECT 'store' AS channel, 
       store.item, 
       store.return_ratio, 
       store.return_rank, 
       store.currency_rank 
FROM   (SELECT item, 
               return_ratio, 
               currency_ratio, 
               RANK() OVER (ORDER BY return_ratio) AS return_rank, 
               RANK() OVER (ORDER BY currency_ratio) AS currency_rank 
        FROM   (SELECT sts.ss_item_sk AS item, 
                       CAST(SUM(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15, 4)) AS return_ratio, 
                       CAST(SUM(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15, 4)) AS currency_ratio 
                FROM   store_sales sts 
                LEFT OUTER JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number AND sts.ss_item_sk = sr.sr_item_sk) 
                JOIN date_dim ON sts.ss_sold_date_sk = date_dim.d_date_sk 
                WHERE  sr.sr_return_amt > 10000 
                       AND sts.ss_net_profit > 1 
                       AND sts.ss_net_paid > 0 
                       AND sts.ss_quantity > 0 
                       AND d_year = 1999 
                       AND d_moy = 12 
                GROUP  BY sts.ss_item_sk) in_store) store 
WHERE  store.return_rank <= 10 OR store.currency_rank <= 10 

ORDER  BY 1, 4, 5
LIMIT 100;

--50
SELECT 
    s_store_name, 
    s_company_id, 
    s_street_number, 
    s_street_name, 
    s_street_type, 
    s_suite_number, 
    s_city, 
    s_county, 
    s_state, 
    s_zip, 
    SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS `30 days`, 
    SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 30) AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS `31-60 days`, 
    SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 60) AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS `61-90 days`, 
    SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 90) AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS `91-120 days`, 
    SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1 ELSE 0 END) AS `>120 days` 
FROM   
    store_sales, 
    store_returns, 
    store, 
    date_dim d1, 
    date_dim d2 
WHERE  
    d2.d_year = 2002 
    AND d2.d_moy = 9 
    AND ss_ticket_number = sr_ticket_number 
    AND ss_item_sk = sr_item_sk 
    AND ss_sold_date_sk = d1.d_date_sk 
    AND sr_returned_date_sk = d2.d_date_sk 
    AND ss_customer_sk = sr_customer_sk 
    AND ss_store_sk = s_store_sk 
GROUP  BY 
    s_store_name, 
    s_company_id, 
    s_street_number, 
    s_street_name, 
    s_street_type, 
    s_suite_number, 
    s_city, 
    s_county, 
    s_state, 
    s_zip 
ORDER  BY 
    s_store_name, 
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

--51
WITH web_v1 AS (
    SELECT
        ws_item_sk AS item_sk,
        d_date,
        SUM(SUM(ws_sales_price)) OVER (PARTITION BY ws_item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
    FROM
        web_sales,
        date_dim
    WHERE
        ws_sold_date_sk = d_date_sk
        AND d_month_seq BETWEEN 1192 AND 1192 + 11
        AND ws_item_sk IS NOT NULL
    GROUP BY
        ws_item_sk,
        d_date
), 
store_v1 AS (
    SELECT
        ss_item_sk AS item_sk,
        d_date,
        SUM(SUM(ss_sales_price)) OVER (PARTITION BY ss_item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_sales
    FROM
        store_sales,
        date_dim
    WHERE
        ss_sold_date_sk = d_date_sk
        AND d_month_seq BETWEEN 1192 AND 1192 + 11
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
                    CASE
                        WHEN web.item_sk IS NOT NULL THEN web.item_sk
                        ELSE store.item_sk
                    END AS item_sk,
                    CASE
                        WHEN web.d_date IS NOT NULL THEN web.d_date
                        ELSE store.d_date
                    END AS d_date,
                    web.cume_sales AS web_sales,
                    store.cume_sales AS store_sales
                FROM
                    web_v1 web
                FULL OUTER JOIN
                    store_v1 store ON (web.item_sk = store.item_sk AND web.d_date = store.d_date)
            ) x
    ) y
WHERE
    web_cumulative > store_cumulative
ORDER BY
    item_sk,
    d_date
LIMIT 100;

--52
SELECT
    dt.d_year,
    item.i_brand_id AS brand_id,
    item.i_brand AS brand,
    SUM(ss_ext_sales_price) AS ext_price
FROM
    date_dim dt
JOIN
    store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN
    item ON store_sales.ss_item_sk = item.i_item_sk
WHERE
    item.i_manager_id = 1
    AND dt.d_moy = 11
    AND dt.d_year = 1999
GROUP BY
    dt.d_year,
    item.i_brand_id,
    item.i_brand
ORDER BY
    dt.d_year,
    ext_price DESC,
    brand_id
LIMIT 100;

--53
SELECT *
FROM (
    SELECT
        i_manufact_id,
        SUM(ss_sales_price) AS sum_sales,
        AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manufact_id) AS avg_quarterly_sales
    FROM
        item
    JOIN
        store_sales ON item.i_item_sk = store_sales.ss_item_sk
    JOIN
        date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    JOIN
        store ON store_sales.ss_store_sk = store.s_store_sk
    WHERE
        d_month_seq IN (1199, 1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210)
        AND (
            (i_category IN ('Books', 'Children', 'Electronics')
             AND i_class IN ('personal', 'portable', 'reference', 'self-help')
             AND i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9'))
            OR
            (i_category IN ('Women', 'Music', 'Men')
             AND i_class IN ('accessories', 'classical', 'fragrances', 'pants')
             AND i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1'))
        )
    GROUP BY
        i_manufact_id,
        d_qoy
) tmp1
WHERE
    CASE
        WHEN avg_quarterly_sales > 0 THEN ABS(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
        ELSE NULL
    END > 0.1
ORDER BY
    avg_quarterly_sales,
    sum_sales,
    i_manufact_id
LIMIT 100;

--54
WITH my_customers AS (
    SELECT DISTINCT
        c_customer_sk,
        c_current_addr_sk
    FROM (
        SELECT
            cs_sold_date_sk AS sold_date_sk,
            cs_bill_customer_sk AS customer_sk,
            cs_item_sk AS item_sk
        FROM catalog_sales
        UNION ALL
        SELECT
            ws_sold_date_sk AS sold_date_sk,
            ws_bill_customer_sk AS customer_sk,
            ws_item_sk AS item_sk
        FROM web_sales
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
        CAST((revenue / 50) AS INT) AS segment
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

--55
SELECT
    i_brand_id AS brand_id,
    i_brand AS brand,
    SUM(ss_ext_sales_price) AS ext_price
FROM
    date_dim
JOIN store_sales ON d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE
    i_manager_id = 33
    AND d_moy = 12
    AND d_year = 1998
GROUP BY
    i_brand_id,
    i_brand
ORDER BY
    ext_price DESC,
    i_brand_id
LIMIT 100;

--56
WITH ss AS (
    SELECT
        i_item_id,
        SUM(ss_ext_sales_price) AS total_sales
    FROM
        store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    JOIN item ON ss_item_sk = i_item_sk
    WHERE
        i_color IN ('firebrick', 'rosy', 'white')
        AND d_year = 1998
        AND d_moy = 3
        AND ca_gmt_offset = -6
    GROUP BY
        i_item_id
),
cs AS (
    SELECT
        i_item_id,
        SUM(cs_ext_sales_price) AS total_sales
    FROM
        catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    JOIN customer_address ON cs_bill_addr_sk = ca_address_sk
    JOIN item ON cs_item_sk = i_item_sk
    WHERE
        i_color IN ('firebrick', 'rosy', 'white')
        AND d_year = 1998
        AND d_moy = 3
        AND ca_gmt_offset = -6
    GROUP BY
        i_item_id
),
ws AS (
    SELECT
        i_item_id,
        SUM(ws_ext_sales_price) AS total_sales
    FROM
        web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
    JOIN item ON ws_item_sk = i_item_sk
    WHERE
        i_color IN ('firebrick', 'rosy', 'white')
        AND d_year = 1998
        AND d_moy = 3
        AND ca_gmt_offset = -6
    GROUP BY
        i_item_id
)
SELECT
    i_item_id,
    SUM(total_sales) AS total_sales
FROM
    (
        SELECT * FROM ss
        UNION ALL
        SELECT * FROM cs
        UNION ALL
        SELECT * FROM ws
    ) tmp1
GROUP BY
    i_item_id
ORDER BY
    total_sales
LIMIT 100;

--57
WITH v1 AS (
    SELECT
        i_category,
        i_brand,
        cc_name,
        d_year,
        d_moy,
        SUM(cs_sales_price) AS sum_sales,
        AVG(SUM(cs_sales_price)) OVER (PARTITION BY i_category, i_brand, cc_name, d_year) AS avg_monthly_sales,
        RANK() OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year, d_moy) AS rn
    FROM
        item
    JOIN catalog_sales ON cs_item_sk = i_item_sk
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    JOIN call_center ON cc_call_center_sk = cs_call_center_sk
    WHERE
        (d_year = 2000 OR (d_year = 2000 - 1 AND d_moy = 12) OR (d_year = 2000 + 1 AND d_moy = 1))
    GROUP BY
        i_category, i_brand, cc_name, d_year, d_moy
),
v2 AS (
    SELECT
        v1.i_brand,
        v1.d_year,
        v1.avg_monthly_sales,
        v1.sum_sales,
        v1_lag.sum_sales AS psum,
        v1_lead.sum_sales AS nsum
    FROM
        v1
    JOIN v1 v1_lag ON v1.i_category = v1_lag.i_category AND v1.i_brand = v1_lag.i_brand AND v1.cc_name = v1_lag.cc_name AND v1.rn = v1_lag.rn + 1
    JOIN v1 v1_lead ON v1.i_category = v1_lead.i_category AND v1.i_brand = v1_lead.i_brand AND v1.cc_name = v1_lead.cc_name AND v1.rn = v1_lead.rn - 1
)
SELECT *
FROM v2
WHERE d_year = 2000
    AND avg_monthly_sales > 0
    AND CASE
        WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
        ELSE NULL
        END > 0.1
ORDER BY sum_sales - avg_monthly_sales, 3
LIMIT 100;

--58
WITH ss_items AS (
    SELECT
        i_item_id AS item_id,
        SUM(ss_ext_sales_price) AS ss_item_rev
    FROM
        store_sales
    JOIN item ON ss_item_sk = i_item_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE
        d_date IN (SELECT d_date FROM date_dim WHERE d_week_seq = (SELECT d_week_seq FROM date_dim WHERE d_date = '2002-02-25'))
    GROUP BY
        i_item_id
),
cs_items AS (
    SELECT
        i_item_id AS item_id,
        SUM(cs_ext_sales_price) AS cs_item_rev
    FROM
        catalog_sales
    JOIN item ON cs_item_sk = i_item_sk
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE
        d_date IN (SELECT d_date FROM date_dim WHERE d_week_seq = (SELECT d_week_seq FROM date_dim WHERE d_date = '2002-02-25'))
    GROUP BY
        i_item_id
),
ws_items AS (
    SELECT
        i_item_id AS item_id,
        SUM(ws_ext_sales_price) AS ws_item_rev
    FROM
        web_sales
    JOIN item ON ws_item_sk = i_item_sk
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE
        d_date IN (SELECT d_date FROM date_dim WHERE d_week_seq = (SELECT d_week_seq FROM date_dim WHERE d_date = '2002-02-25'))
    GROUP BY
        i_item_id
)
SELECT
    ss_items.item_id,
    ss_item_rev,
    ss_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 AS ss_dev,
    cs_item_rev,
    cs_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 AS cs_dev,
    ws_item_rev,
    ws_item_rev / (ss_item_rev + cs_item_rev + ws_item_rev) / 3 * 100 AS ws_dev,
    (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average
FROM
    ss_items
JOIN cs_items ON ss_items.item_id = cs_items.item_id
JOIN ws_items ON ss_items.item_id = ws_items.item_id
WHERE
    ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
    AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
    AND cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
    AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
    AND ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
    AND ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
ORDER BY
    item_id, ss_item_rev
LIMIT 100;

--59
WITH wss AS (
    SELECT
        d_week_seq,
        ss_store_sk,
        SUM(CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE NULL END) AS sun_sales,
        SUM(CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE NULL END) AS mon_sales,
        SUM(CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE NULL END) AS tue_sales,
        SUM(CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE NULL END) AS wed_sales,
        SUM(CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE NULL END) AS thu_sales,
        SUM(CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE NULL END) AS fri_sales,
        SUM(CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE NULL END) AS sat_sales
    FROM
        store_sales
    JOIN date_dim ON d_date_sk = ss_sold_date_sk
    GROUP BY
        d_week_seq,
        ss_store_sk
)
SELECT
    s_store_name1,
    s_store_id1,
    d_week_seq1,
    sun_sales1 / sun_sales2,
    mon_sales1 / mon_sales2,
    tue_sales1 / tue_sales2,
    wed_sales1 / wed_sales2,
    thu_sales1 / thu_sales2,
    fri_sales1 / fri_sales2,
    sat_sales1 / sat_sales2
FROM
    (SELECT
        s_store_name AS s_store_name1,
        wss.d_week_seq AS d_week_seq1,
        s_store_id AS s_store_id1,
        sun_sales AS sun_sales1,
        mon_sales AS mon_sales1,
        tue_sales AS tue_sales1,
        wed_sales AS wed_sales1,
        thu_sales AS thu_sales1,
        fri_sales AS fri_sales1,
        sat_sales AS sat_sales1
    FROM
        wss
    JOIN store ON ss_store_sk = s_store_sk
    JOIN date_dim d ON d.d_week_seq = wss.d_week_seq
    WHERE
        d_month_seq BETWEEN 1196 AND 1196 + 11) y
JOIN
    (SELECT
        s_store_name AS s_store_name2,
        wss.d_week_seq AS d_week_seq2,
        s_store_id AS s_store_id2,
        sun_sales AS sun_sales2,
        mon_sales AS mon_sales2,
        tue_sales AS tue_sales2,
        wed_sales AS wed_sales2,
        thu_sales AS thu_sales2,
        fri_sales AS fri_sales2,
        sat_sales AS sat_sales2
    FROM
        wss
    JOIN store ON ss_store_sk = s_store_sk
    JOIN date_dim d ON d.d_week_seq = wss.d_week_seq
    WHERE
        d_month_seq BETWEEN 1196 + 12 AND 1196 + 23) x
ON
    s_store_id1 = s_store_id2
    AND d_week_seq1 = d_week_seq2 - 52
ORDER BY
    s_store_name1,
    s_store_id1,
    d_week_seq1
LIMIT 100;

--60
WITH ss AS (
    SELECT
        i_item_id,
        SUM(ss_ext_sales_price) AS total_sales
    FROM
        store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    JOIN item ON ss_item_sk = i_item_sk
    WHERE
        i_category IN ('Jewelry')
        AND d_year = 1999
        AND d_moy = 8
        AND ca_gmt_offset = -6
    GROUP BY
        i_item_id
),
cs AS (
    SELECT
        i_item_id,
        SUM(cs_ext_sales_price) AS total_sales
    FROM
        catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    JOIN customer_address ON cs_bill_addr_sk = ca_address_sk
    JOIN item ON cs_item_sk = i_item_sk
    WHERE
        i_category IN ('Jewelry')
        AND d_year = 1999
        AND d_moy = 8
        AND ca_gmt_offset = -6
    GROUP BY
        i_item_id
),
ws AS (
    SELECT
        i_item_id,
        SUM(ws_ext_sales_price) AS total_sales
    FROM
        web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
    JOIN item ON ws_item_sk = i_item_sk
    WHERE
        i_category IN ('Jewelry')
        AND d_year = 1999
        AND d_moy = 8
        AND ca_gmt_offset = -6
    GROUP BY
        i_item_id
)
SELECT
    i_item_id,
    SUM(total_sales) AS total_sales
FROM
    (
        SELECT * FROM ss
        UNION ALL
        SELECT * FROM cs
        UNION ALL
        SELECT * FROM ws
    ) tmp1
GROUP BY
    i_item_id
ORDER BY
    i_item_id,
    total_sales
LIMIT 100;

--61
SELECT
    promotions,
    total,
    CAST(promotions AS DECIMAL(15, 4)) / CAST(total AS DECIMAL(15, 4)) * 100
FROM
    (
        SELECT
            SUM(ss_ext_sales_price) AS promotions
        FROM
            store_sales
        JOIN store ON ss_store_sk = s_store_sk
        JOIN promotion ON ss_promo_sk = p_promo_sk
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        JOIN customer ON ss_customer_sk = c_customer_sk
        JOIN customer_address ON ca_address_sk = c_current_addr_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE
            ca_gmt_offset = -7
            AND i_category = 'Books'
            AND (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y')
            AND s_gmt_offset = -7
            AND d_year = 2001
            AND d_moy = 12
    ) promotional_sales,
    (
        SELECT
            SUM(ss_ext_sales_price) AS total
        FROM
            store_sales
        JOIN store ON ss_store_sk = s_store_sk
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        JOIN customer ON ss_customer_sk = c_customer_sk
        JOIN customer_address ON ca_address_sk = c_current_addr_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE
            ca_gmt_offset = -7
            AND i_category = 'Books'
            AND s_gmt_offset = -7
            AND d_year = 2001
            AND d_moy = 12
    ) all_sales
ORDER BY
    promotions,
    total
LIMIT 100;

--62
SELECT
    SUBSTR(w_warehouse_name, 1, 20),
    sm_type,
    web_name,
    SUM(CASE
            WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1
            ELSE 0
        END) AS `30 days`,
    SUM(CASE
            WHEN (ws_ship_date_sk - ws_sold_date_sk > 30)
                 AND (ws_ship_date_sk - ws_sold_date_sk <= 60) THEN 1
            ELSE 0
        END) AS `31-60 days`,
    SUM(CASE
            WHEN (ws_ship_date_sk - ws_sold_date_sk > 60)
                 AND (ws_ship_date_sk - ws_sold_date_sk <= 90) THEN 1
            ELSE 0
        END) AS `61-90 days`,
    SUM(CASE
            WHEN (ws_ship_date_sk - ws_sold_date_sk > 90)
                 AND (ws_ship_date_sk - ws_sold_date_sk <= 120) THEN 1
            ELSE 0
        END) AS `91-120 days`,
    SUM(CASE
            WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1
            ELSE 0
        END) AS `>120 days`
FROM
    web_sales
JOIN warehouse ON ws_warehouse_sk = w_warehouse_sk
JOIN ship_mode ON ws_ship_mode_sk = sm_ship_mode_sk
JOIN web_site ON ws_web_site_sk = web_site_sk
JOIN date_dim ON ws_ship_date_sk = d_date_sk
WHERE
    d_month_seq BETWEEN 1222 AND 1222 + 11
GROUP BY
    SUBSTR(w_warehouse_name, 1, 20),
    sm_type,
    web_name
ORDER BY
    SUBSTR(w_warehouse_name, 1, 20),
    sm_type,
    web_name
LIMIT 100;

--63
SELECT *
FROM (
    SELECT
        i_manager_id,
        SUM(ss_sales_price) AS sum_sales,
        AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manager_id) AS avg_monthly_sales
    FROM
        item
    JOIN store_sales ON ss_item_sk = i_item_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN store ON ss_store_sk = s_store_sk
    WHERE
        d_month_seq IN (1200, 1200 + 1, 1200 + 2, 1200 + 3, 1200 + 4, 1200 + 5, 1200 + 6, 1200 + 7, 1200 + 8, 1200 + 9, 1200 + 10, 1200 + 11)
        AND (
            (i_category IN ('Books', 'Children', 'Electronics')
             AND i_class IN ('personal', 'portable', 'reference', 'self-help')
             AND i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9'))
            OR
            (i_category IN ('Women', 'Music', 'Men')
             AND i_class IN ('accessories', 'classical', 'fragrances', 'pants')
             AND i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1'))
        )
    GROUP BY
        i_manager_id,
        d_moy
) tmp1
WHERE
    CASE
        WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
        ELSE NULL
    END > 0.1
ORDER BY
    i_manager_id,
    avg_monthly_sales,
    sum_sales
LIMIT 100;

--64
SELECT
    i.i_product_name AS product_name,
    i.i_item_sk AS item_sk,
    s.s_store_name AS store_name,
    s.s_zip AS store_zip,
    ad1.ca_street_number AS b_street_number,
    ad1.ca_street_name AS b_street_name,
    ad1.ca_city AS b_city,
    ad1.ca_zip AS b_zip,
    ad2.ca_street_number AS c_street_number,
    ad2.ca_street_name AS c_street_name,
    ad2.ca_city AS c_city,
    ad2.ca_zip AS c_zip,
    d1.d_year AS syear,
    d2.d_year AS fsyear,
    d3.d_year AS s2year,
    COUNT(*) AS cnt,
    SUM(ss.ss_wholesale_cost) AS s1,
    SUM(ss.ss_list_price) AS s2,
    SUM(ss.ss_coupon_amt) AS s3
FROM
    store_sales ss
    JOIN store_returns sr ON ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number 
    JOIN date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk 
    JOIN date_dim d2 ON ss.ss_current_day_sk = d2.d_date_sk 
    JOIN date_dim d3 ON ss.ss_current_day_sk = d3.d_date_sk 
    JOIN store s ON ss.ss_store_sk = s.s_store_sk 
    JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk 
    JOIN customer_demographics cd1 ON ss.ss_cdemo_sk = cd1.cd_demo_sk 
    JOIN customer_demographics cd2 ON c.c_current_cdemo_sk = cd2.cd_demo_sk 
    JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk 
    JOIN household_demographics hd1 ON ss.ss_hdemo_sk = hd1.hd_demo_sk 
    JOIN household_demographics hd2 ON c.c_current_hdemo_sk = hd2.hd_demo_sk 
    JOIN customer_address ad1 ON ss.ss_addr_sk = ad1.ca_address_sk 
    JOIN customer_address ad2 ON c.c_current_addr_sk = ad2.ca_address_sk 
    JOIN income_band ib1 ON hd1.hd_income_band_sk = ib1.ib_income_band_sk 
    JOIN income_band ib2 ON hd2.hd_income_band_sk = ib2.ib_income_band_sk 
    JOIN item i ON ss.ss_item_sk = i.i_item_sk 
WHERE
    i.i_color IN ('cyan', 'peach', 'blush', 'frosted', 'powder', 'orange') 
    AND i.i_current_price BETWEEN 58 AND 68
GROUP BY
    i.i_product_name,
    i.i_item_sk,
    s.s_store_name,
    s.s_zip,
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
    d3.d_year
HAVING
    SUM(ss.ss_ext_list_price) > 2 * SUM(sr.sr_refunded_cash + sr.sr_reversed_charge + sr.sr_store_credit)
ORDER BY
    i.i_product_name,
    s.s_store_name;

--65
SELECT
    s.s_store_name,
    i.i_item_desc,
    sc.revenue,
    i.i_current_price,
    i.i_wholesale_cost,
    i.i_brand
FROM
    store s
JOIN
    item i ON s.s_store_sk = i.i_item_sk
JOIN
    (
        SELECT
            ss_store_sk,
            AVG(revenue) AS ave
        FROM
            (
                SELECT
                    ss_store_sk,
                    SUM(ss_sales_price) AS revenue
                FROM
                    store_sales
                JOIN
                    date_dim ON ss_sold_date_sk = d_date_sk
                WHERE
                    d_month_seq BETWEEN 1199 AND 1199 + 11
                GROUP BY
                    ss_store_sk
            ) sa
        GROUP BY
            ss_store_sk
    ) sb ON s.s_store_sk = sb.ss_store_sk
JOIN
    (
        SELECT
            ss_store_sk,
            ss_item_sk,
            SUM(ss_sales_price) AS revenue
        FROM
            store_sales
        JOIN
            date_dim ON ss_sold_date_sk = d_date_sk
        WHERE
            d_month_seq BETWEEN 1199 AND 1199 + 11
        GROUP BY
            ss_store_sk, ss_item_sk
    ) sc ON s.s_store_sk = sc.ss_store_sk
WHERE
    sc.revenue <= 0.1 * sb.ave
ORDER BY
    s.s_store_name, i.i_item_desc
LIMIT 100;

--66
SELECT 
    w_warehouse_name, 
    w_warehouse_sq_ft, 
    w_city, 
    w_county, 
    w_state, 
    w_country, 
    ship_carriers, 
    year1,
    SUM(jan_sales) AS jan_sales, 
    SUM(feb_sales) AS feb_sales, 
    SUM(mar_sales) AS mar_sales, 
    SUM(apr_sales) AS apr_sales, 
    SUM(may_sales) AS may_sales, 
    SUM(jun_sales) AS jun_sales, 
    SUM(jul_sales) AS jul_sales, 
    SUM(aug_sales) AS aug_sales, 
    SUM(sep_sales) AS sep_sales, 
    SUM(oct_sales) AS oct_sales, 
    SUM(nov_sales) AS nov_sales, 
    SUM(dec_sales) AS dec_sales, 
    SUM(jan_sales / w_warehouse_sq_ft) AS jan_sales_per_sq_foot, 
    SUM(feb_sales / w_warehouse_sq_ft) AS feb_sales_per_sq_foot, 
    SUM(mar_sales / w_warehouse_sq_ft) AS mar_sales_per_sq_foot, 
    SUM(apr_sales / w_warehouse_sq_ft) AS apr_sales_per_sq_foot, 
    SUM(may_sales / w_warehouse_sq_ft) AS may_sales_per_sq_foot, 
    SUM(jun_sales / w_warehouse_sq_ft) AS jun_sales_per_sq_foot, 
    SUM(jul_sales / w_warehouse_sq_ft) AS jul_sales_per_sq_foot, 
    SUM(aug_sales / w_warehouse_sq_ft) AS aug_sales_per_sq_foot, 
    SUM(sep_sales / w_warehouse_sq_ft) AS sep_sales_per_sq_foot, 
    SUM(oct_sales / w_warehouse_sq_ft) AS oct_sales_per_sq_foot, 
    SUM(nov_sales / w_warehouse_sq_ft) AS nov_sales_per_sq_foot, 
    SUM(dec_sales / w_warehouse_sq_ft) AS dec_sales_per_sq_foot, 
    SUM(jan_net) AS jan_net, 
    SUM(feb_net) AS feb_net, 
    SUM(mar_net) AS mar_net, 
    SUM(apr_net) AS apr_net, 
    SUM(may_net) AS may_net, 
    SUM(jun_net) AS jun_net, 
    SUM(jul_net) AS jul_net, 
    SUM(aug_net) AS aug_net, 
    SUM(sep_net) AS sep_net, 
    SUM(oct_net) AS oct_net, 
    SUM(nov_net) AS nov_net, 
    SUM(dec_net) AS dec_net 
FROM   
(
    SELECT 
        w_warehouse_name, 
        w_warehouse_sq_ft, 
        w_city, 
        w_county, 
        w_state, 
        w_country, 
        CONCAT('ZOUROS', ',', 'ZHOU') AS ship_carriers, 
        d_year AS year1, 
        SUM(CASE WHEN d_moy = 1 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS jan_sales, 
        SUM(CASE WHEN d_moy = 2 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS feb_sales, 
        SUM(CASE WHEN d_moy = 3 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS mar_sales, 
        SUM(CASE WHEN d_moy = 4 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS apr_sales, 
        SUM(CASE WHEN d_moy = 5 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS may_sales, 
        SUM(CASE WHEN d_moy = 6 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS jun_sales, 
        SUM(CASE WHEN d_moy = 7 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS jul_sales, 
        SUM(CASE WHEN d_moy = 8 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS aug_sales, 
        SUM(CASE WHEN d_moy = 9 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS sep_sales, 
        SUM(CASE WHEN d_moy = 10 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS oct_sales, 
        SUM(CASE WHEN d_moy = 11 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS nov_sales, 
        SUM(CASE WHEN d_moy = 12 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS dec_sales, 
        SUM(CASE WHEN d_moy = 1 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS jan_net, 
        SUM(CASE WHEN d_moy = 2 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS feb_net, 
        SUM(CASE WHEN d_moy = 3 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS mar_net, 
        SUM(CASE WHEN d_moy = 4 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS apr_net, 
        SUM(CASE WHEN d_moy = 5 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS may_net, 
        SUM(CASE WHEN d_moy = 6 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS jun_net, 
        SUM(CASE WHEN d_moy = 7 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS jul_net, 
        SUM(CASE WHEN d_moy = 8 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS aug_net, 
        SUM(CASE WHEN d_moy = 9 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS sep_net, 
        SUM(CASE WHEN d_moy = 10 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS oct_net, 
        SUM(CASE WHEN d_moy = 11 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS nov_net, 
        SUM(CASE WHEN d_moy = 12 THEN ws_net_paid_inc_ship * ws_quantity ELSE 0 END) AS dec_net 
    FROM   
        web_sales 
    JOIN   
        warehouse ON ws_warehouse_sk = w_warehouse_sk 
    JOIN   
        date_dim ON ws_sold_date_sk = d_date_sk 
    JOIN   
        time_dim ON ws_sold_time_sk = t_time_sk 
    JOIN   
        ship_mode ON ws_ship_mode_sk = sm_ship_mode_sk 
    WHERE  
        d_year = 1998 
        AND t_time BETWEEN 7249 AND 7249 + 28800 
        AND sm_carrier IN ('ZOUROS', 'ZHOU') 
    GROUP BY 
        w_warehouse_name, 
        w_warehouse_sq_ft, 
        w_city, 
        w_county, 
        w_state, 
        w_country, 
        d_year 
    UNION ALL 
    SELECT 
        w_warehouse_name, 
        w_warehouse_sq_ft, 
        w_city, 
        w_county, 
        w_state, 
        w_country, 
        CONCAT('ZOUROS', ',', 'ZHOU') AS ship_carriers, 
        d_year AS year1, 
        SUM(CASE WHEN d_moy = 1 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS jan_sales, 
        SUM(CASE WHEN d_moy = 2 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS feb_sales, 
        SUM(CASE WHEN d_moy = 3 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS mar_sales, 
        SUM(CASE WHEN d_moy = 4 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS apr_sales, 
        SUM(CASE WHEN d_moy = 5 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS may_sales, 
        SUM(CASE WHEN d_moy = 6 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS jun_sales, 
        SUM(CASE WHEN d_moy = 7 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS jul_sales, 
        SUM(CASE WHEN d_moy = 8 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS aug_sales, 
        SUM(CASE WHEN d_moy = 9 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS sep_sales, 
        SUM(CASE WHEN d_moy = 10 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS oct_sales, 
        SUM(CASE WHEN d_moy = 11 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS nov_sales, 
        SUM(CASE WHEN d_moy = 12 THEN cs_ext_sales_price * cs_quantity ELSE 0 END) AS dec_sales, 
        SUM(CASE WHEN d_moy = 1 THEN cs_net_paid * cs_quantity ELSE 0 END) AS jan_net, 
        SUM(CASE WHEN d_moy = 2 THEN cs_net_paid * cs_quantity ELSE 0 END) AS feb_net, 
        SUM(CASE WHEN d_moy = 3 THEN cs_net_paid * cs_quantity ELSE 0 END) AS mar_net, 
        SUM(CASE WHEN d_moy = 4 THEN cs_net_paid * cs_quantity ELSE 0 END) AS apr_net, 
        SUM(CASE WHEN d_moy = 5 THEN cs_net_paid * cs_quantity ELSE 0 END) AS may_net, 
        SUM(CASE WHEN d_moy = 6 THEN cs_net_paid * cs_quantity ELSE 0 END) AS jun_net, 
        SUM(CASE WHEN d_moy = 7 THEN cs_net_paid * cs_quantity ELSE 0 END) AS jul_net, 
        SUM(CASE WHEN d_moy = 8 THEN cs_net_paid * cs_quantity ELSE 0 END) AS aug_net, 
        SUM(CASE WHEN d_moy = 9 THEN cs_net_paid * cs_quantity ELSE 0 END) AS sep_net, 
        SUM(CASE WHEN d_moy = 10 THEN cs_net_paid * cs_quantity ELSE 0 END) AS oct_net, 
        SUM(CASE WHEN d_moy = 11 THEN cs_net_paid * cs_quantity ELSE 0 END) AS nov_net, 
        SUM(CASE WHEN d_moy = 12 THEN cs_net_paid * cs_quantity ELSE 0 END) AS dec_net 
    FROM   
        catalog_sales 
    JOIN   
        warehouse ON cs_warehouse_sk = w_warehouse_sk 
    JOIN   
        date_dim ON cs_sold_date_sk = d_date_sk 
    JOIN   
        time_dim ON cs_sold_time_sk = t_time_sk 
    JOIN   
        ship_mode ON cs_ship_mode_sk = sm_ship_mode_sk 
    WHERE  
        d_year = 1998 
        AND t_time BETWEEN 7249 AND 7249 + 28800 
        AND sm_carrier IN ('ZOUROS', 'ZHOU') 
    GROUP BY 
        w_warehouse_name, 
        w_warehouse_sq_ft, 
        w_city, 
        w_county, 
        w_state, 
        w_country, 
        d_year
) x 
GROUP BY 
    w_warehouse_name, 
    w_warehouse_sq_ft, 
    w_city, 
    w_county, 
    w_state, 
    w_country, 
    ship_carriers, 
    year1 
ORDER BY 
    w_warehouse_name 
LIMIT 100;

--67
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
        JOIN 
            date_dim ON ss_sold_date_sk = d_date_sk
        JOIN 
            store ON ss_store_sk = s_store_sk
        JOIN 
            item ON ss_item_sk = i_item_sk
        WHERE 
            d_month_seq BETWEEN 1181 AND 1181 + 11
        GROUP BY 
            ROLLUP(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)
    ) dw1
) dw2
WHERE rk <= 100
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
    rk
LIMIT 100;

--68
SELECT 
    c_last_name, 
    c_first_name, 
    ca_city, 
    bought_city, 
    ss_ticket_number, 
    extended_price, 
    extended_tax, 
    list_price 
FROM (
    SELECT 
        ss_ticket_number, 
        ss_customer_sk, 
        ca_city AS bought_city, 
        SUM(ss_ext_sales_price) AS extended_price, 
        SUM(ss_ext_list_price) AS list_price, 
        SUM(ss_ext_tax) AS extended_tax 
    FROM   
        store_sales
    JOIN   
        date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk 
    JOIN   
        store ON store_sales.ss_store_sk = store.s_store_sk 
    JOIN   
        household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk 
    JOIN   
        customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk 
    WHERE  
        date_dim.d_dom BETWEEN 1 AND 2 
        AND (household_demographics.hd_dep_count = 8 OR household_demographics.hd_vehicle_count = 3) 
        AND date_dim.d_year IN (1998, 1998 + 1, 1998 + 2) 
        AND store.s_city IN ('Fairview', 'Midway') 
    GROUP BY 
        ss_ticket_number, 
        ss_customer_sk, 
        ss_addr_sk, 
        ca_city
) dn 
JOIN 
    customer ON dn.ss_customer_sk = customer.c_customer_sk 
JOIN 
    customer_address current_addr ON customer.c_current_addr_sk = current_addr.ca_address_sk 
WHERE  
    current_addr.ca_city <> bought_city 
ORDER BY 
    c_last_name, 
    ss_ticket_number
LIMIT 100;

--69
SELECT 
    cd_gender, 
    cd_marital_status, 
    cd_education_status, 
    COUNT(*) AS cnt1, 
    cd_purchase_estimate, 
    COUNT(*) AS cnt2, 
    cd_credit_rating, 
    COUNT(*) AS cnt3 
FROM   
    customer c
JOIN   
    customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN   
    customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE  
    ca.ca_state IN ('KS', 'AZ', 'NE') 
    AND EXISTS (
        SELECT * 
        FROM   
            store_sales
        JOIN   
            date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk 
        WHERE  
            c.c_customer_sk = store_sales.ss_customer_sk 
            AND date_dim.d_year = 2004 
            AND date_dim.d_moy BETWEEN 3 AND 5
    ) 
    AND NOT EXISTS (
        SELECT * 
        FROM   
            web_sales
        JOIN   
            date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk 
        WHERE  
            c.c_customer_sk = web_sales.ws_bill_customer_sk 
            AND date_dim.d_year = 2004 
            AND date_dim.d_moy BETWEEN 3 AND 5
    ) 
    AND NOT EXISTS (
        SELECT * 
        FROM   
            catalog_sales
        JOIN   
            date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk 
        WHERE  
            c.c_customer_sk = catalog_sales.cs_ship_customer_sk 
            AND date_dim.d_year = 2004 
            AND date_dim.d_moy BETWEEN 3 AND 5
    )
GROUP BY 
    cd_gender, 
    cd_marital_status, 
    cd_education_status, 
    cd_purchase_estimate, 
    cd_credit_rating 
ORDER BY 
    cd_gender, 
    cd_marital_status, 
    cd_education_status, 
    cd_purchase_estimate, 
    cd_credit_rating
LIMIT 100;

--70
SELECT 
    SUM(ss_net_profit) AS total_sum, 
    s_state, 
    s_county, 
    CASE 
        WHEN s_county IS NULL THEN 'ALL_STATES' 
        ELSE 'INDIVIDUAL_STATES' 
    END AS lochierarchy, 
    RANK() OVER (
        PARTITION BY s_state
        ORDER BY SUM(ss_net_profit) DESC
    ) AS rank_within_parent 
FROM   
    store_sales
JOIN   
    date_dim d1 ON d1.d_date_sk = store_sales.ss_sold_date_sk
JOIN   
    store ON store.s_store_sk = store_sales.ss_store_sk
JOIN (
        SELECT 
            s_state AS state_ranked, 
            SUM(ss_net_profit) AS state_profit
        FROM   
            store_sales
        JOIN   
            store ON store.s_store_sk = store_sales.ss_store_sk
        JOIN   
            date_dim ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
        WHERE  
            d_month_seq BETWEEN 1200 AND 1200 + 11 
        GROUP BY 
            s_state
     ) tmp1 ON store.s_state = tmp1.state_ranked
WHERE  
    d1.d_month_seq BETWEEN 1200 AND 1200 + 11 
    AND tmp1.state_profit <= 5
GROUP BY 
    s_state, s_county
ORDER BY 
    lochierarchy DESC, 
    CASE 
        WHEN lochierarchy = 'INDIVIDUAL_STATES' THEN s_state 
    END, 
    rank_within_parent
LIMIT 100;

--71
SELECT 
    i_brand_id AS brand_id, 
    i_brand AS brand, 
    t_hour, 
    t_minute, 
    SUM(ext_price) AS ext_price 
FROM   
    item
JOIN   
    (
        SELECT 
            ws_ext_sales_price AS ext_price, 
            ws_sold_date_sk AS sold_date_sk, 
            ws_item_sk AS sold_item_sk, 
            ws_sold_time_sk AS time_sk 
        FROM   
            web_sales
        JOIN   
            date_dim ON d_date_sk = ws_sold_date_sk
        WHERE  
            d_moy = 11 
            AND d_year = 2001 
        UNION ALL 
        SELECT 
            cs_ext_sales_price AS ext_price, 
            cs_sold_date_sk AS sold_date_sk, 
            cs_item_sk AS sold_item_sk, 
            cs_sold_time_sk AS time_sk 
        FROM   
            catalog_sales
        JOIN   
            date_dim ON d_date_sk = cs_sold_date_sk
        WHERE  
            d_moy = 11 
            AND d_year = 2001 
        UNION ALL 
        SELECT 
            ss_ext_sales_price AS ext_price, 
            ss_sold_date_sk AS sold_date_sk, 
            ss_item_sk AS sold_item_sk, 
            ss_sold_time_sk AS time_sk 
        FROM   
            store_sales
        JOIN   
            date_dim ON d_date_sk = ss_sold_date_sk
        WHERE  
            d_moy = 11 
            AND d_year = 2001
    ) tmp ON sold_item_sk = i_item_sk 
JOIN   
    time_dim ON tmp.time_sk = t_time_sk
WHERE  
    i_manager_id = 1 
    AND (t_meal_time = 'breakfast' OR t_meal_time = 'dinner') 
GROUP BY 
    i_brand, 
    i_brand_id, 
    t_hour, 
    t_minute 
ORDER BY 
    ext_price DESC, 
    i_brand_id;

--72
SELECT 
    i_item_desc, 
    w_warehouse_name, 
    d1.d_week_seq, 
    SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo, 
    SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) AS promo, 
    COUNT(*) AS total_cnt 
FROM   
    catalog_sales 
JOIN 
    inventory ON (cs_item_sk = inv_item_sk) 
JOIN 
    warehouse ON (w_warehouse_sk = inv_warehouse_sk) 
JOIN 
    item ON (i_item_sk = cs_item_sk) 
JOIN 
    customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk) 
JOIN 
    household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk) 
JOIN 
    date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk) 
JOIN 
    date_dim d2 ON (inv_date_sk = d2.d_date_sk) 
JOIN 
    date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk) 
LEFT OUTER JOIN 
    promotion ON (cs_promo_sk = p_promo_sk) 
LEFT OUTER JOIN 
    catalog_returns ON (cr_item_sk = cs_item_sk AND cr_order_number = cs_order_number) 
WHERE  
    d1.d_week_seq = d2.d_week_seq 
    AND inv_quantity_on_hand < cs_quantity 
    AND d3.d_date > date_add(d1.d_date, 5) 
    AND hd_buy_potential = '501-1000' 
    AND d1.d_year = 2002 
    AND cd_marital_status = 'M' 
GROUP BY 
    i_item_desc, 
    w_warehouse_name, 
    d1.d_week_seq 
ORDER BY 
    total_cnt DESC, 
    i_item_desc, 
    w_warehouse_name, 
    d1.d_week_seq
LIMIT 100;

--73
SELECT 
    c_last_name, 
    c_first_name, 
    c_salutation, 
    c_preferred_cust_flag, 
    ss_ticket_number, 
    cnt 
FROM   
    (SELECT 
        ss_ticket_number, 
        ss_customer_sk, 
        COUNT(*) AS cnt 
    FROM   
        store_sales 
    JOIN 
        date_dim ON (store_sales.ss_sold_date_sk = date_dim.d_date_sk) 
    JOIN 
        store ON (store_sales.ss_store_sk = store.s_store_sk) 
    JOIN 
        household_demographics ON (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk) 
    WHERE  
        date_dim.d_dom BETWEEN 1 AND 2 
        AND (household_demographics.hd_buy_potential = '>10000' OR household_demographics.hd_buy_potential = '0-500') 
        AND household_demographics.hd_vehicle_count > 0 
        AND IF(household_demographics.hd_vehicle_count > 0, household_demographics.hd_dep_count / household_demographics.hd_vehicle_count, NULL) > 1 
        AND date_dim.d_year IN (2000, 2000 + 1, 2000 + 2) 
        AND store.s_county = 'Williamson County' 
    GROUP BY 
        ss_ticket_number, 
        ss_customer_sk
    ) dj 
JOIN 
    customer ON (dj.ss_customer_sk = customer.c_customer_sk) 
WHERE  
    cnt BETWEEN 1 AND 5 
ORDER BY 
    cnt DESC, 
    c_last_name ASC;

--74
WITH year_total AS (
    SELECT 
        c_customer_id AS customer_id, 
        c_first_name AS customer_first_name, 
        c_last_name AS customer_last_name, 
        d_year AS year1, 
        SUM(ss_net_paid) AS year_total, 
        's' AS sale_type 
    FROM   
        customer 
    JOIN 
        store_sales ON (c_customer_sk = ss_customer_sk) 
    JOIN 
        date_dim ON (ss_sold_date_sk = d_date_sk) 
    WHERE  
        d_year IN (1999, 1999 + 1) 
    GROUP BY 
        c_customer_id, 
        c_first_name, 
        c_last_name, 
        d_year 
    UNION ALL 
    SELECT 
        c_customer_id AS customer_id, 
        c_first_name AS customer_first_name, 
        c_last_name AS customer_last_name, 
        d_year AS year1, 
        SUM(ws_net_paid) AS year_total, 
        'w' AS sale_type 
    FROM   
        customer 
    JOIN 
        web_sales ON (c_customer_sk = ws_bill_customer_sk) 
    JOIN 
        date_dim ON (ws_sold_date_sk = d_date_sk) 
    WHERE  
        d_year IN (1999, 1999 + 1) 
    GROUP BY 
        c_customer_id, 
        c_first_name, 
        c_last_name, 
        d_year
)
SELECT 
    t_s_secyear.customer_id, 
    t_s_secyear.customer_first_name, 
    t_s_secyear.customer_last_name 
FROM   
    year_total t_s_firstyear 
JOIN 
    year_total t_s_secyear ON (t_s_secyear.customer_id = t_s_firstyear.customer_id) 
JOIN 
    year_total t_w_firstyear ON (t_s_firstyear.customer_id = t_w_firstyear.customer_id) 
JOIN 
    year_total t_w_secyear ON (t_s_firstyear.customer_id = t_w_secyear.customer_id) 
WHERE  
    t_s_firstyear.sale_type = 's' 
    AND t_w_firstyear.sale_type = 'w' 
    AND t_s_secyear.sale_type = 's' 
    AND t_w_secyear.sale_type = 'w' 
    AND t_s_firstyear.year1 = 1999 
    AND t_s_secyear.year1 = 1999 + 1 
    AND t_w_firstyear.year1 = 1999 
    AND t_w_secyear.year1 = 1999 + 1 
    AND t_s_firstyear.year_total > 0 
    AND t_w_firstyear.year_total > 0 
    AND (t_w_secyear.year_total / t_w_firstyear.year_total) > (CASE WHEN t_s_firstyear.year_total > 0 THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE NULL END) 
ORDER BY 
    1, 
    2, 
    3
LIMIT 100;

--75
WITH all_sales AS (
    SELECT
        d_year,
        i_brand_id,
        i_class_id,
        i_category_id,
        i_manufact_id,
        SUM(sales_cnt) AS sales_cnt,
        SUM(sales_amt) AS sales_amt
    FROM (
        SELECT
            d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id,
            cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt,
            cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt
        FROM
            catalog_sales
        JOIN
            item ON i_item_sk = cs_item_sk
        JOIN
            date_dim ON d_date_sk = cs_sold_date_sk
        LEFT JOIN
            catalog_returns ON (cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk)
        WHERE
            i_category = 'Men'
        UNION ALL
        SELECT
            d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id,
            ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt,
            ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS sales_amt
        FROM
            store_sales
        JOIN
            item ON i_item_sk = ss_item_sk
        JOIN
            date_dim ON d_date_sk = ss_sold_date_sk
        LEFT JOIN
            store_returns ON (ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk)
        WHERE
            i_category = 'Men'
        UNION ALL
        SELECT
            d_year,
            i_brand_id,
            i_class_id,
            i_category_id,
            i_manufact_id,
            ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt,
            ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS sales_amt
        FROM
            web_sales
        JOIN
            item ON i_item_sk = ws_item_sk
        JOIN
            date_dim ON d_date_sk = ws_sold_date_sk
        LEFT JOIN
            web_returns ON (ws_order_number = wr_order_number AND ws_item_sk = wr_item_sk)
        WHERE
            i_category = 'Men'
    ) sales_detail
    GROUP BY
        d_year,
        i_brand_id,
        i_class_id,
        i_category_id,
        i_manufact_id
)
SELECT
    prev_yr.d_year AS prev_year,
    curr_yr.d_year AS year1,
    curr_yr.i_brand_id,
    curr_yr.i_class_id,
    curr_yr.i_category_id,
    curr_yr.i_manufact_id,
    prev_yr.sales_cnt AS prev_yr_cnt,
    curr_yr.sales_cnt AS curr_yr_cnt,
    curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
    curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM
    all_sales curr_yr
JOIN
    all_sales prev_yr ON (curr_yr.i_brand_id = prev_yr.i_brand_id
                          AND curr_yr.i_class_id = prev_yr.i_class_id
                          AND curr_yr.i_category_id = prev_yr.i_category_id
                          AND curr_yr.i_manufact_id = prev_yr.i_manufact_id)
WHERE
    curr_yr.d_year = 2002
    AND prev_yr.d_year = 2001
    AND CAST(curr_yr.sales_cnt AS DECIMAL(17, 2)) / CAST(prev_yr.sales_cnt AS DECIMAL(17, 2)) < 0.9
ORDER BY
    sales_cnt_diff
LIMIT 100;

--76
SELECT
    channel,
    col_name,
    d_year,
    d_qoy,
    i_category,
    COUNT(*) AS sales_cnt,
    SUM(ext_sales_price) AS sales_amt
FROM (
    SELECT
        'store' AS channel,
        'ss_hdemo_sk' AS col_name,
        d_year,
        d_qoy,
        i_category,
        ss_ext_sales_price AS ext_sales_price
    FROM
        store_sales
    JOIN
        item ON ss_item_sk = i_item_sk
    JOIN
        date_dim ON ss_sold_date_sk = d_date_sk
    WHERE
        ss_hdemo_sk IS NULL
    UNION ALL
    SELECT
        'web' AS channel,
        'ws_ship_hdemo_sk' AS col_name,
        d_year,
        d_qoy,
        i_category,
        ws_ext_sales_price AS ext_sales_price
    FROM
        web_sales
    JOIN
        item ON ws_item_sk = i_item_sk
    JOIN
        date_dim ON ws_sold_date_sk = d_date_sk
    WHERE
        ws_ship_hdemo_sk IS NULL
    UNION ALL
    SELECT
        'catalog' AS channel,
        'cs_warehouse_sk' AS col_name,
        d_year,
        d_qoy,
        i_category,
        cs_ext_sales_price AS ext_sales_price
    FROM
        catalog_sales
    JOIN
        item ON cs_item_sk = i_item_sk
    JOIN
        date_dim ON cs_sold_date_sk = d_date_sk
    WHERE
        cs_warehouse_sk IS NULL
) foo
GROUP BY
    channel,
    col_name,
    d_year,
    d_qoy,
    i_category
ORDER BY
    channel,
    col_name,
    d_year,
    d_qoy,
    i_category
LIMIT 100;

--77
WITH 
ss AS (
    SELECT
        ss_store_sk,
        SUM(ss_ext_sales_price) AS sales,
        SUM(ss_net_profit) AS profit
    FROM
        store_sales
    JOIN
        date_dim ON ss_sold_date_sk = d_date_sk
    WHERE
        d_date BETWEEN '2001-08-16' AND date_add('2001-08-16', 30)
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
    JOIN
        date_dim ON sr_returned_date_sk = d_date_sk
    WHERE
        d_date BETWEEN '2001-08-16' AND date_add('2001-08-16', 30)
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
    JOIN
        date_dim ON cs_sold_date_sk = d_date_sk
    WHERE
        d_date BETWEEN '2001-08-16' AND date_add('2001-08-16', 30)
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
    JOIN
        date_dim ON cr_returned_date_sk = d_date_sk
    WHERE
        d_date BETWEEN '2001-08-16' AND date_add('2001-08-16', 30)
    GROUP BY
        cr_call_center_sk
),
ws AS (
    SELECT
        ws_web_page_sk,
        SUM(ws_ext_sales_price) AS sales,
        SUM(ws_net_profit) AS profit
    FROM
        web_sales
    JOIN
        date_dim ON ws_sold_date_sk = d_date_sk
    WHERE
        d_date BETWEEN '2001-08-16' AND date_add('2001-08-16', 30)
    GROUP BY
        ws_web_page_sk
),
wr AS (
    SELECT
        wr_web_page_sk,
        SUM(wr_return_amt) AS returns1,
        SUM(wr_net_loss) AS profit_loss
    FROM
        web_returns
    JOIN
        date_dim ON wr_returned_date_sk = d_date_sk
    WHERE
        d_date BETWEEN '2001-08-16' AND date_add('2001-08-16', 30)
    GROUP BY
        wr_web_page_sk
)
SELECT
    channel,
    id,
    SUM(sales) AS sales,
    SUM(returns1) AS returns1,
    SUM(profit) AS profit
FROM (
    SELECT
        'store channel' AS channel,
        ss.ss_store_sk AS id,
        sales,
        COALESCE(returns1, 0) AS returns1,
        (profit - COALESCE(profit_loss, 0)) AS profit
    FROM
        ss
    LEFT JOIN
        sr ON ss.ss_store_sk = sr.sr_store_sk
    UNION ALL
    SELECT
        'catalog channel' AS channel,
        cs.cs_call_center_sk AS id,
        sales,
        returns1,
        (profit - profit_loss) AS profit
    FROM
        cs
    JOIN
        cr ON cs.cs_call_center_sk = cr.cr_call_center_sk
    UNION ALL
    SELECT
        'web channel' AS channel,
        ws.ws_web_page_sk AS id,
        sales,
        COALESCE(returns1, 0) AS returns1,
        (profit - COALESCE(profit_loss, 0)) AS profit
    FROM
        ws
    LEFT JOIN
        wr ON ws.ws_web_page_sk = wr.wr_web_page_sk
) x
GROUP BY
    ROLLUP(channel, id)
ORDER BY
    channel,
    id
LIMIT 100;

--78
WITH ws AS (
    SELECT
        d_year AS ws_sold_year,
        ws_item_sk,
        ws_bill_customer_sk AS ws_customer_sk,
        SUM(ws_quantity) AS ws_qty,
        SUM(ws_wholesale_cost) AS ws_wc,
        SUM(ws_sales_price) AS ws_sp
    FROM
        web_sales
        LEFT JOIN web_returns ON wr_order_number = ws_order_number AND ws_item_sk = wr_item_sk
        JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE
        wr_order_number IS NULL
    GROUP BY
        d_year,
        ws_item_sk,
        ws_bill_customer_sk
),
cs AS (
    SELECT
        d_year AS cs_sold_year,
        cs_item_sk,
        cs_bill_customer_sk AS cs_customer_sk,
        SUM(cs_quantity) AS cs_qty,
        SUM(cs_wholesale_cost) AS cs_wc,
        SUM(cs_sales_price) AS cs_sp
    FROM
        catalog_sales
        LEFT JOIN catalog_returns ON cr_order_number = cs_order_number AND cs_item_sk = cr_item_sk
        JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE
        cr_order_number IS NULL
    GROUP BY
        d_year,
        cs_item_sk,
        cs_bill_customer_sk
),
ss AS (
    SELECT
        d_year AS ss_sold_year,
        ss_item_sk,
        ss_customer_sk,
        SUM(ss_quantity) AS ss_qty,
        SUM(ss_wholesale_cost) AS ss_wc,
        SUM(ss_sales_price) AS ss_sp
    FROM
        store_sales
        LEFT JOIN store_returns ON sr_ticket_number = ss_ticket_number AND ss_item_sk = sr_item_sk
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE
        sr_ticket_number IS NULL
    GROUP BY
        d_year,
        ss_item_sk,
        ss_customer_sk
)
SELECT
    ss_item_sk,
    ROUND(ss_qty / COALESCE(ws_qty + cs_qty, 1), 2) AS ratio,
    ss_qty AS store_qty,
    ss_wc AS store_wholesale_cost,
    ss_sp AS store_sales_price,
    COALESCE(ws_qty, 0) + COALESCE(cs_qty, 0) AS other_chan_qty,
    COALESCE(ws_wc, 0) + COALESCE(cs_wc, 0) AS other_chan_wholesale_cost,
    COALESCE(ws_sp, 0) + COALESCE(cs_sp, 0) AS other_chan_sales_price
FROM
    ss
LEFT JOIN
    ws ON (ws_sold_year = ss_sold_year AND ws_item_sk = ss_item_sk AND ws_customer_sk = ss_customer_sk)
LEFT JOIN
    cs ON (cs_sold_year = ss_sold_year AND cs_item_sk = ss_item_sk AND cs_customer_sk = ss_customer_sk)
WHERE
    COALESCE(ws_qty, 0) > 0
    AND COALESCE(cs_qty, 0) > 0
    AND ss_sold_year = 1999
ORDER BY
    ss_item_sk,
    ss_qty DESC,
    ss_wc DESC,
    ss_sp DESC,
    other_chan_qty,
    other_chan_wholesale_cost,
    other_chan_sales_price,
    ROUND(ss_qty / COALESCE(ws_qty + cs_qty, 1), 2)
LIMIT 100;

--79
SELECT
    c_last_name,
    c_first_name,
    SUBSTR(s_city, 1, 30),
    ss_ticket_number,
    amt,
    profit
FROM
    (SELECT
        ss_ticket_number,
        ss_customer_sk,
        store.s_city,
        SUM(ss_coupon_amt) AS amt,
        SUM(ss_net_profit) AS profit
    FROM
        store_sales
        JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
        JOIN store ON store_sales.ss_store_sk = store.s_store_sk
        JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    WHERE
        (household_demographics.hd_dep_count = 8 OR household_demographics.hd_vehicle_count > 4)
        AND date_dim.d_dow = 1
        AND date_dim.d_year IN (2000, 2000 + 1, 2000 + 2)
        AND store.s_number_employees BETWEEN 200 AND 295
    GROUP BY
        ss_ticket_number,
        ss_customer_sk,
        store.s_city
    ) ms
JOIN
    customer ON ss_customer_sk = c_customer_sk
ORDER BY
    c_last_name,
    c_first_name,
    SUBSTR(s_city, 1, 30),
    profit
LIMIT 100;

--81
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
),
csr AS (
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
),
wsr AS (
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
        SELECT
            'store channel' AS channel,
            CONCAT('store', store_id) AS id,
            sales,
            returns1,
            profit
        FROM
            ssr
        UNION ALL
        SELECT
            'catalog channel' AS channel,
            CONCAT('catalog_page', catalog_page_id) AS id,
            sales,
            returns1,
            profit
        FROM
            csr
        UNION ALL
        SELECT
            'web channel' AS channel,
            CONCAT('web_site', web_site_id) AS id,
            sales,
            returns1,
            profit
        FROM
            wsr
    ) x
GROUP BY ROLLUP(channel, id)
ORDER BY channel, id
LIMIT 100;

--81
WITH customer_total_return AS (
    SELECT
        cr_returning_customer_sk AS ctr_customer_sk,
        ca_state AS ctr_state,
        SUM(cr_return_amt_inc_tax) AS ctr_total_return
    FROM
        catalog_returns
        JOIN date_dim ON cr_returned_date_sk = d_date_sk
        JOIN customer_address ON cr_returning_addr_sk = ca_address_sk
    WHERE
        d_year = 1999
    GROUP BY
        cr_returning_customer_sk,
        ca_state
)
SELECT
    c_customer_id,
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
FROM
    customer_total_return ctr1
    JOIN customer_address ON ctr1.ctr_state = ca_state
    JOIN customer ON ca_address_sk = c_current_addr_sk
WHERE
    ctr1.ctr_total_return > (
        SELECT AVG(ctr_total_return) * 1.2
        FROM customer_total_return ctr2
        WHERE ctr1.ctr_state = ctr2.ctr_state
    )
    AND ca_state = 'TX'
ORDER BY
    c_customer_id,
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

--82
SELECT
    i_item_id,
    i_item_desc,
    i_current_price
FROM
    item
    JOIN inventory ON inv_item_sk = i_item_sk
    JOIN date_dim ON d_date_sk = inv_date_sk
    JOIN store_sales ON ss_item_sk = i_item_sk
WHERE
    i_current_price BETWEEN 63 AND 63 + 30
    AND d_date BETWEEN CAST('1998-04-27' AS DATE) AND (CAST('1998-04-27' AS DATE) + INTERVAL '60' DAY)
    AND i_manufact_id IN (57, 293, 427, 320)
    AND inv_quantity_on_hand BETWEEN 100 AND 500
GROUP BY
    i_item_id,
    i_item_desc,
    i_current_price
ORDER BY
    i_item_id
LIMIT 100;

--83
WITH sr_items AS (
    SELECT
        i_item_id AS item_id,
        SUM(sr_return_quantity) AS sr_item_qty
    FROM
        store_returns
        JOIN item ON sr_item_sk = i_item_sk
        JOIN date_dim ON sr_returned_date_sk = d_date_sk
    WHERE
        d_date IN (
            SELECT d_date
            FROM date_dim
            WHERE d_week_seq IN (
                SELECT d_week_seq
                FROM date_dim
                WHERE d_date IN ('1999-06-30', '1999-08-28', '1999-11-18')
            )
        )
    GROUP BY
        i_item_id
),
cr_items AS (
    SELECT
        i_item_id AS item_id,
        SUM(cr_return_quantity) AS cr_item_qty
    FROM
        catalog_returns
        JOIN item ON cr_item_sk = i_item_sk
        JOIN date_dim ON cr_returned_date_sk = d_date_sk
    WHERE
        d_date IN (
            SELECT d_date
            FROM date_dim
            WHERE d_week_seq IN (
                SELECT d_week_seq
                FROM date_dim
                WHERE d_date IN ('1999-06-30', '1999-08-28', '1999-11-18')
            )
        )
    GROUP BY
        i_item_id
),
wr_items AS (
    SELECT
        i_item_id AS item_id,
        SUM(wr_return_quantity) AS wr_item_qty
    FROM
        web_returns
        JOIN item ON wr_item_sk = i_item_sk
        JOIN date_dim ON wr_returned_date_sk = d_date_sk
    WHERE
        d_date IN (
            SELECT d_date
            FROM date_dim
            WHERE d_week_seq IN (
                SELECT d_week_seq
                FROM date_dim
                WHERE d_date IN ('1999-06-30', '1999-08-28', '1999-11-18')
            )
        )
    GROUP BY
        i_item_id
)
SELECT
    sr_items.item_id,
    sr_item_qty,
    sr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS sr_dev,
    cr_item_qty,
    cr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS cr_dev,
    wr_item_qty,
    wr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS wr_dev,
    (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 AS average
FROM
    sr_items
    JOIN cr_items ON sr_items.item_id = cr_items.item_id
    JOIN wr_items ON sr_items.item_id = wr_items.item_id
ORDER BY
    sr_items.item_id, sr_item_qty
LIMIT 100;

--84
SELECT
    c_customer_id AS customer_id,
    CONCAT(c_last_name, ', ', c_first_name) AS customername
FROM
    customer
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk
    JOIN household_demographics ON hd_demo_sk = c_current_hdemo_sk
    JOIN income_band ON ib_income_band_sk = hd_income_band_sk
    JOIN store_returns ON sr_cdemo_sk = cd_demo_sk
WHERE
    ca_city = 'Green Acres'
    AND ib_lower_bound >= 54986
    AND ib_upper_bound <= 54986 + 50000
ORDER BY
    c_customer_id
LIMIT 100;

--85
SELECT
    SUBSTRING(r_reason_desc, 1, 20),
    AVG(ws_quantity),
    AVG(wr_refunded_cash),
    AVG(wr_fee)
FROM
    web_sales
    JOIN web_returns ON ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number
    JOIN web_page ON ws_web_page_sk = wp_web_page_sk
    JOIN customer_demographics cd1 ON wr_refunded_cdemo_sk = cd1.cd_demo_sk
    JOIN customer_demographics cd2 ON wr_returning_cdemo_sk = cd2.cd_demo_sk
    JOIN customer_address ON ca_address_sk = wr_refunded_addr_sk
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    JOIN reason ON wr_reason_sk = r_reason_sk
WHERE
    d_year = 2001
    AND (
        (cd1.cd_marital_status = 'W' AND cd1.cd_marital_status = cd2.cd_marital_status AND cd1.cd_education_status = 'Primary' AND cd1.cd_education_status = cd2.cd_education_status AND ws_sales_price BETWEEN 100.00 AND 150.00)
        OR (cd1.cd_marital_status = 'D' AND cd1.cd_marital_status = cd2.cd_marital_status AND cd1.cd_education_status = 'Secondary' AND cd1.cd_education_status = cd2.cd_education_status AND ws_sales_price BETWEEN 50.00 AND 100.00)
        OR (cd1.cd_marital_status = 'M' AND cd1.cd_marital_status = cd2.cd_marital_status AND cd1.cd_education_status = 'Advanced Degree' AND cd1.cd_education_status = cd2.cd_education_status AND ws_sales_price BETWEEN 150.00 AND 200.00)
    )
    AND (
        (ca_country = 'United States' AND ca_state IN ('KY', 'ME', 'IL') AND ws_net_profit BETWEEN 100 AND 200)
        OR (ca_country = 'United States' AND ca_state IN ('OK', 'NE', 'MN') AND ws_net_profit BETWEEN 150 AND 300)
        OR (ca_country = 'United States' AND ca_state IN ('FL', 'WI', 'KS') AND ws_net_profit BETWEEN 50 AND 250)
    )
GROUP BY
    r_reason_desc
ORDER BY
    SUBSTRING(r_reason_desc, 1, 20),
    AVG(ws_quantity),
    AVG(wr_refunded_cash),
    AVG(wr_fee)
LIMIT 100;

--86
SELECT
    total_sum,
    i_category,
    i_class,
    CASE
        WHEN i_category IS NULL AND i_class IS NULL THEN 'Total'
        WHEN i_category IS NOT NULL AND i_class IS NULL THEN i_category
        ELSE CONCAT(i_category, '_', i_class)
    END AS lochierarchy,
    RANK() OVER (
        PARTITION BY i_category, i_class
        ORDER BY total_sum DESC
    ) AS rank_within_parent
FROM (
    SELECT
        SUM(ws_net_paid) AS total_sum,
        i_category,
        i_class
    FROM
        web_sales
        JOIN date_dim d1 ON d1.d_date_sk = ws_sold_date_sk
        JOIN item ON i_item_sk = ws_item_sk
    WHERE
        d1.d_month_seq BETWEEN 1183 AND 1183 + 11
    GROUP BY
        i_category, i_class
) AS ranked_data
ORDER BY
    lochierarchy DESC,
    rank_within_parent
LIMIT 100;

--87
SELECT COUNT(*)
FROM (
    SELECT DISTINCT c_last_name, c_first_name, d_date
    FROM (
        SELECT c_last_name, c_first_name, d_date
        FROM store_sales
        JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
        JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
        WHERE d_month_seq BETWEEN 1188 AND 1188 + 11
        UNION ALL
        SELECT c_last_name, c_first_name, d_date
        FROM catalog_sales
        JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
        JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
        WHERE d_month_seq BETWEEN 1188 AND 1188 + 11
        UNION ALL
        SELECT c_last_name, c_first_name, d_date
        FROM web_sales
        JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
        JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
        WHERE d_month_seq BETWEEN 1188 AND 1188 + 11
    ) cool_cust
) cool_cust;

--88
SELECT
    h8_30_to_9,
    h9_to_9_30,
    h9_30_to_10,
    h10_to_10_30,
    h10_30_to_11,
    h11_to_11_30,
    h11_30_to_12,
    h12_to_12_30
FROM (
    SELECT COUNT(*) AS h8_30_to_9
    FROM store_sales
    JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    WHERE time_dim.t_hour = 8
        AND time_dim.t_minute >= 30
        AND (
            (household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2)
            OR (household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count <= 2 + 2)
            OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)
        )
        AND store.s_store_name = 'ese'
) s1
CROSS JOIN (
    SELECT COUNT(*) AS h9_to_9_30
    FROM store_sales
    JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    WHERE time_dim.t_hour = 9
        AND time_dim.t_minute < 30
        AND (
            (household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2)
            OR (household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count <= 2 + 2)
            OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)
        )
        AND store.s_store_name = 'ese'
) s2
CROSS JOIN (
    SELECT COUNT(*) AS h9_30_to_10
    FROM store_sales
    JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    WHERE time_dim.t_hour = 9
        AND time_dim.t_minute >= 30
        AND (
            (household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2)
            OR (household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count <= 2 + 2)
            OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)
        )
        AND store.s_store_name = 'ese'
) s3
CROSS JOIN (
    SELECT COUNT(*) AS h10_to_10_30
    FROM store_sales
    JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    WHERE time_dim.t_hour = 10
        AND time_dim.t_minute < 30
        AND (
            (household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2)
            OR (household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count <= 2 + 2)
            OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)
        )
        AND store.s_store_name = 'ese'
) s4
CROSS JOIN (
    SELECT COUNT(*) AS h10_30_to_11
    FROM store_sales
    JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    WHERE time_dim.t_hour = 10
        AND time_dim.t_minute >= 30
        AND (
            (household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2)
            OR (household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count <= 2 + 2)
            OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)
        )
        AND store.s_store_name = 'ese'
) s5
CROSS JOIN (
    SELECT COUNT(*) AS h11_to_11_30
    FROM store_sales
    JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    WHERE time_dim.t_hour = 11
        AND time_dim.t_minute < 30
        AND (
            (household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2)
            OR (household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count <= 2 + 2)
            OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)
        )
        AND store.s_store_name = 'ese'
) s6
CROSS JOIN (
    SELECT COUNT(*) AS h11_30_to_12
    FROM store_sales
    JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    WHERE time_dim.t_hour = 11
        AND time_dim.t_minute >= 30
        AND (
            (household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2)
            OR (household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count <= 2 + 2)
            OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)
        )
        AND store.s_store_name = 'ese'
) s7
CROSS JOIN (
    SELECT COUNT(*) AS h12_to_12_30
    FROM store_sales
    JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    WHERE time_dim.t_hour = 12
        AND time_dim.t_minute < 30
        AND (
            (household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= -1 + 2)
            OR (household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count <= 2 + 2)
            OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 3 + 2)
        )
        AND store.s_store_name = 'ese'
) s8;

--89
SELECT *
FROM (
    SELECT
        i_category,
        i_class,
        i_brand,
        s_store_name,
        s_company_name,
        d_moy,
        SUM(ss_sales_price) AS sum_sales,
        AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) AS avg_monthly_sales
    FROM
        item
        JOIN store_sales ON ss_item_sk = i_item_sk
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        JOIN store ON ss_store_sk = s_store_sk
    WHERE
        d_year IN (2002)
        AND (
            (i_category IN ('Home', 'Men', 'Sports') AND i_class IN ('paint', 'accessories', 'fitness'))
            OR (i_category IN ('Shoes', 'Jewelry', 'Women') AND i_class IN ('mens', 'pendants', 'swimwear'))
        )
    GROUP BY
        i_category,
        i_class,
        i_brand,
        s_store_name,
        s_company_name,
        d_moy
) tmp1
WHERE
    CASE
        WHEN (avg_monthly_sales <> 0) THEN (ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales)
        ELSE NULL
    END > 0.1
ORDER BY
    sum_sales - avg_monthly_sales,
    s_store_name
LIMIT 100;

--90
SELECT
    CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) AS am_pm_ratio
FROM
    (SELECT COUNT(*) AS amc
    FROM web_sales
    JOIN household_demographics ON ws_ship_hdemo_sk = household_demographics.hd_demo_sk
    JOIN time_dim ON ws_sold_time_sk = time_dim.t_time_sk
    JOIN web_page ON ws_web_page_sk = web_page.wp_web_page_sk
    WHERE time_dim.t_hour BETWEEN 12 AND 12 + 1
        AND household_demographics.hd_dep_count = 8
        AND web_page.wp_char_count BETWEEN 5000 AND 5200) at1
CROSS JOIN
    (SELECT COUNT(*) AS pmc
    FROM web_sales
    JOIN household_demographics ON ws_ship_hdemo_sk = household_demographics.hd_demo_sk
    JOIN time_dim ON ws_sold_time_sk = time_dim.t_time_sk
    JOIN web_page ON ws_web_page_sk = web_page.wp_web_page_sk
    WHERE time_dim.t_hour BETWEEN 20 AND 20 + 1
        AND household_demographics.hd_dep_count = 8
        AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt
ORDER BY
    am_pm_ratio
LIMIT 100;

--91

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

--92
SELECT 
    SUM(ws_ext_discount_amt) AS `Excess Discount Amount`
FROM     
    web_sales
JOIN 
    item ON i_item_sk = ws_item_sk
JOIN 
    date_dim ON d_date_sk = ws_sold_date_sk
WHERE    
    i_manufact_id = 718 
    AND d_date BETWEEN '2002-03-29' AND (cast('2002-03-29' AS date) + INTERVAL '90' day) 
    AND ws_ext_discount_amt > (
        SELECT 1.3 * AVG(ws_ext_discount_amt) 
        FROM web_sales
        JOIN date_dim ON d_date_sk = ws_sold_date_sk
        WHERE ws_item_sk = i_item_sk 
            AND d_date BETWEEN '2002-03-29' AND (cast('2002-03-29' AS date) + INTERVAL '90' day)
    ) 
ORDER BY 
    sum(ws_ext_discount_amt) 
LIMIT 100;

--93
SELECT ss_customer_sk, 
       SUM(act_sales) AS sumsales 
FROM   (SELECT ss_item_sk, 
               ss_ticket_number, 
               ss_customer_sk, 
               CASE 
                 WHEN sr_return_quantity IS NOT NULL THEN 
                 ( ss_quantity - sr_return_quantity ) * ss_sales_price 
                 ELSE ( ss_quantity * ss_sales_price ) 
               END AS act_sales 
        FROM   store_sales 
               LEFT OUTER JOIN store_returns 
                            ON ( sr_item_sk = ss_item_sk 
                                 AND sr_ticket_number = ss_ticket_number ) 
               JOIN reason 
                    ON sr_reason_sk = r_reason_sk 
        WHERE  r_reason_desc = 'reason 38') t 
GROUP  BY ss_customer_sk 
ORDER  BY sumsales, 
          ss_customer_sk
LIMIT 100;

--94
SELECT 
    COUNT(DISTINCT ws_order_number) AS `order count`, 
    SUM(ws_ext_ship_cost) AS `total shipping cost`, 
    SUM(ws_net_profit) AS `total net profit` 
FROM 
    web_sales ws1 
JOIN 
    date_dim ON ws1.ws_ship_date_sk = d_date_sk 
JOIN 
    customer_address ON ws1.ws_ship_addr_sk = ca_address_sk 
JOIN 
    web_site ON ws1.ws_web_site_sk = web_site_sk 
WHERE 
    d_date BETWEEN '2000-3-01' AND (CAST('2000-3-01' AS DATE) + INTERVAL 60 DAY) 
    AND ca_state = 'MT' 
    AND web_company_name = 'pri' 
    AND EXISTS (
        SELECT * 
        FROM web_sales ws2 
        WHERE ws1.ws_order_number = ws2.ws_order_number 
        AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
    ) 
    AND NOT EXISTS (
        SELECT * 
        FROM web_returns wr1 
        WHERE ws1.ws_order_number = wr1.wr_order_number
    ) 
GROUP BY 
    ws1.ws_order_number 
ORDER BY 
    count(DISTINCT ws_order_number) 
LIMIT 100;

--95
WITH ws_wh AS (
    SELECT 
        ws1.ws_order_number, 
        ws1.ws_warehouse_sk AS wh1, 
        ws2.ws_warehouse_sk AS wh2 
    FROM   
        web_sales ws1
    JOIN   
        web_sales ws2 ON ws1.ws_order_number = ws2.ws_order_number 
    WHERE  
        ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
)
SELECT 
    COUNT(DISTINCT ws_order_number) AS `order count`, 
    SUM(ws_ext_ship_cost) AS `total shipping cost`, 
    SUM(ws_net_profit) AS `total net profit` 
FROM     
    web_sales ws1 
JOIN     
    date_dim ON ws1.ws_ship_date_sk = d_date_sk 
JOIN     
    customer_address ON ws1.ws_ship_addr_sk = ca_address_sk 
JOIN     
    web_site ON ws1.ws_web_site_sk = web_site_sk 
WHERE    
    d_date BETWEEN '2000-4-01' AND (CAST('2000-4-01' AS DATE) + INTERVAL 60 DAY) 
    AND ca_state = 'IN' 
    AND web_company_name = 'pri' 
    AND ws1.ws_order_number IN (
        SELECT ws_order_number 
        FROM   ws_wh
    ) 
    AND ws1.ws_order_number IN (
        SELECT wr_order_number 
        FROM   web_returns 
        JOIN   ws_wh ON web_returns.wr_order_number = ws_wh.ws_order_number
    ) 
GROUP BY 
    ws1.ws_order_number 
ORDER BY 
    count(DISTINCT ws_order_number) 
LIMIT 100;

--96
SELECT COUNT(*) 
FROM   store_sales 
JOIN   household_demographics ON ss_hdemo_sk = hd_demo_sk 
JOIN   time_dim ON ss_sold_time_sk = t_time_sk 
JOIN   store ON ss_store_sk = s_store_sk 
WHERE  t_hour = 15 
       AND t_minute >= 30 
       AND hd_dep_count = 7 
       AND s_store_name = 'ese' 
ORDER BY COUNT(*) 
LIMIT 100;

--97
WITH ssci AS (
    SELECT ss_customer_sk AS customer_sk, 
           ss_item_sk AS item_sk 
    FROM   store_sales 
    JOIN   date_dim ON ss_sold_date_sk = d_date_sk 
    WHERE  d_month_seq BETWEEN 1196 AND 1196 + 11 
    GROUP  BY ss_customer_sk, ss_item_sk
), 
csci AS (
    SELECT cs_bill_customer_sk AS customer_sk, 
           cs_item_sk AS item_sk 
    FROM   catalog_sales 
    JOIN   date_dim ON cs_sold_date_sk = d_date_sk 
    WHERE  d_month_seq BETWEEN 1196 AND 1196 + 11 
    GROUP  BY cs_bill_customer_sk, cs_item_sk
)
SELECT SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NULL THEN 1 ELSE 0 END) AS store_only, 
       SUM(CASE WHEN ssci.customer_sk IS NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) AS catalog_only, 
       SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) AS store_and_catalog 
FROM   ssci 
FULL OUTER JOIN csci ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk
LIMIT 100;

--98
SELECT 
    i_item_id, 
    i_item_desc, 
    i_category, 
    i_class, 
    i_current_price, 
    SUM(ss_ext_sales_price) AS itemrevenue, 
    SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio 
FROM   
    store_sales 
JOIN 
    item ON ss_item_sk = i_item_sk 
JOIN 
    date_dim ON ss_sold_date_sk = d_date_sk 
WHERE  
    i_category IN ('Men', 'Home', 'Electronics') 
    AND d_date BETWEEN CAST('2000-05-18' AS DATE) AND (CAST('2000-05-18' AS DATE) + INTERVAL '30' DAY) 
GROUP BY 
    i_item_id, i_item_desc, i_category, i_class, i_current_price 
ORDER BY 
    i_category, i_class, i_item_id, i_item_desc, revenueratio;

--99
SELECT 
    SUBSTR(w_warehouse_name, 1, 20), 
    sm_type, 
    cc_name, 
    SUM(CASE 
            WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30) THEN 1 
            ELSE 0 
        END) AS `30 days`, 
    SUM(CASE 
            WHEN (cs_ship_date_sk - cs_sold_date_sk > 30) AND (cs_ship_date_sk - cs_sold_date_sk <= 60) THEN 1 
            ELSE 0 
        END) AS `31-60 days`, 
    SUM(CASE 
            WHEN (cs_ship_date_sk - cs_sold_date_sk > 60) AND (cs_ship_date_sk - cs_sold_date_sk <= 90) THEN 1 
            ELSE 0 
        END) AS `61-90 days`, 
    SUM(CASE 
            WHEN (cs_ship_date_sk - cs_sold_date_sk > 90) AND (cs_ship_date_sk - cs_sold_date_sk <= 120) THEN 1 
            ELSE 0 
        END) AS `91-120 days`, 
    SUM(CASE 
            WHEN (cs_ship_date_sk - cs_sold_date_sk > 120) THEN 1 
            ELSE 0 
        END) AS `>120 days` 
FROM   
    catalog_sales 
JOIN 
    warehouse ON cs_warehouse_sk = w_warehouse_sk 
JOIN 
    ship_mode ON cs_ship_mode_sk = sm_ship_mode_sk 
JOIN 
    call_center ON cs_call_center_sk = cc_call_center_sk 
JOIN 
    date_dim ON cs_ship_date_sk = d_date_sk 
WHERE  
    d_month_seq BETWEEN 1200 AND 1200 + 11 
GROUP BY 
    SUBSTR(w_warehouse_name, 1, 20), 
    sm_type, 
    cc_name 
ORDER BY 
    SUBSTR(w_warehouse_name, 1, 20), 
    sm_type, 
    cc_name
LIMIT 100;





