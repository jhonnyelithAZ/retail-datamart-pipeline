-- =========================================================================
-- Business Intelligence & Analytical SQL Queries for Retail DataMart
-- =========================================================================

-- 1. Total Revenue and Sales Volume by Country
SELECT 
    country,
    COUNT(DISTINCT invoice_no) AS total_invoices,
    SUM(quantity) AS total_items_sold,
    SUM(gross_revenue) AS total_revenue
FROM fact_sales
GROUP BY country
ORDER BY total_revenue DESC;

-- 2. Top 10 Best-Selling Products by Revenue
SELECT 
    p.product_code,
    p.canonical_description,
    p.category,
    SUM(s.quantity) AS units_sold,
    SUM(s.gross_revenue) AS total_revenue
FROM fact_sales s
JOIN dim_products p ON s.product_code = p.product_code
GROUP BY p.product_code, p.canonical_description, p.category
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Top 10 Customers by Lifetime Value (Revenue)
SELECT 
    c.customer_id,
    c.customer_type,
    COUNT(DISTINCT s.invoice_no) AS total_purchases,
    SUM(s.gross_revenue) AS lifetime_value
FROM fact_sales s
JOIN dim_customers c ON s.customer_id = c.customer_id
WHERE c.customer_id != 'UNKNOWN'
GROUP BY c.customer_id, c.customer_type
ORDER BY lifetime_value DESC
LIMIT 10;

-- 4. Monthly Sales Trend Analysis
SELECT 
    DATE_TRUNC('month', invoice_date) AS sales_month,
    COUNT(DISTINCT invoice_no) AS total_orders,
    SUM(quantity) AS items_sold,
    SUM(gross_revenue) AS monthly_revenue
FROM fact_sales
GROUP BY sales_month
ORDER BY sales_month ASC;

-- 5. Total Returns and Refund Amounts by Product
SELECT 
    p.product_code,
    p.canonical_description,
    SUM(ABS(r.quantity)) AS total_returned_units,
    SUM(r.return_amount) AS total_refund_amount
FROM fact_returns r
JOIN dim_products p ON r.product_code = p.product_code
GROUP BY p.product_code, p.canonical_description
ORDER BY total_refund_amount DESC
LIMIT 10;

-- 6. Data Quality & Rejection Audit Log Summary
SELECT 
    source_file,
    rejection_reason,
    COUNT(*) AS total_rejected_records
FROM log_rejections
GROUP BY source_file, rejection_reason
ORDER BY total_rejected_records DESC;