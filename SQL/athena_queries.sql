 -- Query 1: Overall churn rate
SELECT COUNT(*) as total_customers,
       SUM(Churn) as churned_customers,
       ROUND(AVG(Churn) * 100, 2) as churn_rate_pct
FROM ecommerce_churn_db.processed;

 -- Query 2: Overall churn rate
 SELECT CityTier,
       COUNT(*) as total_customers,
       SUM(Churn) as churned,
       ROUND(AVG(Churn) * 100, 2) as churn_rate_pct
FROM ecommerce_churn_db.processed
GROUP BY CityTier\
ORDER BY churn_rate_pct DESC;

 -- Query 3: Overall churn rate
 SELECT PreferredPaymentMode,
       COUNT(*) as total_customers,
       ROUND(AVG(Churn) * 100, 2) as churn_rate_pct
FROM ecommerce_churn_db.processed
GROUP BY PreferredPaymentMode
ORDER BY churn_rate_pct DESC;

 -- Query 4: Overall churn rate
 SELECT CustomerID,
       CashbackAmount,
       ChurnRisk,
       RANK() OVER (PARTITION BY ChurnRisk ORDER BY CashbackAmount DESC) as rank_within_risk
FROM ecommerce_churn_db.processed
LIMIT 20;

 -- Query 5: Overall churn rate
 SELECT HighValueCustomer,
       ChurnRisk,
       COUNT(*) as total,
       ROUND(AVG(Churn) * 100, 2) as churn_rate_pct,
       ROUND(AVG(CashbackAmount), 2) as avg_cashback
FROM ecommerce_churn_db.processed
GROUP BY HighValueCustomer, ChurnRisk
ORDER BY HighValueCustomer DESC, churn_rate_pct DESC;

 -- Query 6: Overall churn rate
 WITH tenure_buckets AS (
    SELECT 
        CASE 
            WHEN Tenure <= 6 THEN '0-6 months'
            WHEN Tenure <= 12 THEN '6-12 months'
            WHEN Tenure <= 24 THEN '1-2 years'
            ELSE '2+ years'
        END as tenure_group,
        Churn
    FROM ecommerce_churn_db.processed
)
SELECT tenure_group,
       COUNT(*) as total_customers,
       ROUND(AVG(Churn) * 100, 2) as churn_rate_pct
FROM tenure_buckets
GROUP BY tenure_group
ORDER BY churn_rate_pct DESC;