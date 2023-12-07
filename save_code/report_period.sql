, params(period_grp, period, start_date, end_date, days) AS 
(
	SELECT  DISTINCT 
	        '1. Daily'                      period_group 
	    ,   CAST(report_date AS VARCHAR)    period 
	    ,   report_date                     start_date 
	    ,   report_date                     end_date 
	    ,   1                               days 
	FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim 
	WHERE   report_date BETWEEN DATE('2022-01-01') AND CURRENT_DATE - INTERVAL '1' DAY  
	
	UNION
	SELECT  DISTINCT 
	        '2. Weekly'                     period_group 
	    ,   CAST(year_week AS VARCHAR)      period 
	    ,   first_day_of_week               start_date 
	    ,   IF(DATE_TRUNC('week', report_date) = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' DAY), CURRENT_DATE - INTERVAL '1' DAY, last_day_of_week)            end_date 
	    ,   IF(DATE_TRUNC('week', report_date) = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' DAY), day_of_week(CURRENT_DATE - INTERVAL '1' DAY), 7)              days
	FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim 
	WHERE   report_date BETWEEN DATE('2022-01-01') AND CURRENT_DATE - INTERVAL '1' DAY  
	
	UNION
	SELECT  DISTINCT 
	        '3. Monthly'                    period_group 
	    ,   CAST(year_month AS VARCHAR)     period 
	    ,   first_day_of_month              start_date 
	    ,   IF(DATE_TRUNC('month', report_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' DAY), CURRENT_DATE - INTERVAL '1' DAY, last_day_of_month)                 end_date 
	    ,   IF(DATE_TRUNC('month', report_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' DAY), day_of_month(CURRENT_DATE - INTERVAL '1' DAY), num_day_in_month)    days
	FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim 
	WHERE   report_date BETWEEN DATE('2022-01-01') AND CURRENT_DATE - INTERVAL '1' DAY  
	
	UNION
	SELECT  DISTINCT 
	        '4. Quarterly'                                      period_group 
	    ,   CONCAT(CAST(year AS VARCHAR), '-', quarter_of_year_name) period
	    ,   first_day_of_quarter                                start_date 
	    ,   IF(DATE_TRUNC('quarter', report_date) = DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1' DAY), CURRENT_DATE - INTERVAL '1' DAY, last_day_of_quarter)               end_date 
	    ,   IF(DATE_TRUNC('quarter', report_date) = DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1' DAY), date_diff('day',first_day_of_quarter,CURRENT_DATE - INTERVAL '1' DAY), date_diff('day',first_day_of_quarter,last_day_of_quarter)) + 1    days
	FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim 
	WHERE   report_date BETWEEN DATE('2022-01-01') AND CURRENT_DATE - INTERVAL '1' DAY  
	
	UNION
	SELECT  DISTINCT 
	        '5. Yearly'             period_group 
	    ,   CAST(year AS VARCHAR)   period
	    ,   first_day_of_year       start_date 
	    ,   IF(DATE_TRUNC('year', report_date) = DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' DAY), CURRENT_DATE - INTERVAL '1' DAY, last_day_of_year)    end_date 
	    ,   IF(DATE_TRUNC('year', report_date) = DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' DAY), DATE_DIFF('day',first_day_of_year,CURRENT_DATE - INTERVAL '1' DAY) +1, num_day_in_year)  days
	FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_date_dim 
	WHERE   report_date BETWEEN DATE('2022-01-01') AND CURRENT_DATE - INTERVAL '1' DAY 
)
