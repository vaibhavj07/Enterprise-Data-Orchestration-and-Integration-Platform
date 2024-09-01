SELECT COUNT(*) FROM campaign_metadata;

-- what to partition dim table by?
SELECT
COUNT(DISTINCT category) as categories,
COUNT(DISTINCT price) as prices
FROM product_catalogs;