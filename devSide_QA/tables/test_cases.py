import pandas as pd
import psycopg2
from psycopg2 import extras


def test_cases(cur_stage, cur_rds, source_id):
    batch_id_query = f'SELECT DISTINCT batch_id from listing where source_id = {source_id} ORDER BY batch_id DESC;'
    cur_stage.execute(batch_id_query)
    result = cur_stage.fetchone()
    batch_id = result[0]
    report = []

    counts_query = f'''
		SELECT Test_Case_Name AS TestCase , Table_Name AS Table, RESULT AS FINAL_RESULT FROM (
	WITH distinct_address_listing_id AS (
		SELECT id FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id}
		EXCEPT
		SELECT listing_id FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id}
	),
	distinct_address_listing_id_1 AS (
		SELECT listing_id FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id}
		EXCEPT
		SELECT id FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id}
	),
	distinct_office_listing_id AS (
		SELECT DISTINCT lreo.listing_id FROM listing_real_estate_office_rel lreo
		JOIN listing l ON l.id = lreo.listing_id
		WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
		EXCEPT
		SELECT DISTINCT id FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id}
	),
	distinct_office_listing_id_1 AS (
		SELECT DISTINCT id FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id}
		EXCEPT
		SELECT DISTINCT lreo.listing_id FROM listing_real_estate_office_rel lreo
		JOIN listing l ON l.id = lreo.listing_id
		WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
	),
	distinct_office_source_office_id AS (
		SELECT DISTINCT source_office_id FROM real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id}
		EXCEPT
		SELECT DISTINCT lpr.office_id FROM listing_real_estate_office_rel lpr
		LEFT JOIN listing l ON l.id = lpr.listing_id
		WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
	),
	distinct_office_source_office_id_1 AS (
		SELECT DISTINCT lpr.office_id FROM listing_real_estate_office_rel lpr
		LEFT JOIN listing l ON l.id = lpr.listing_id
		WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
		EXCEPT
		SELECT DISTINCT source_office_id FROM real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id}
	),
	distinct_participant_listing_id AS (
		SELECT DISTINCT lpr.listing_id FROM listing_participant_rel lpr
		JOIN listing l ON l.id = lpr.listing_id
		WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
		EXCEPT
		SELECT DISTINCT id FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id}
	),
	distinct_participant_listing_id_1 AS (
		SELECT DISTINCT id FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id}
		EXCEPT
		SELECT DISTINCT lpr.listing_id FROM listing_participant_rel lpr
		JOIN listing l ON l.id = lpr.listing_id
		WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
	)

	SELECT 'Id in listing but not in address' AS Test_Case_Name, 'Listing/Listing_address' AS Table_Name, 'id' AS Column_Name, COUNT(1) AS Count, CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM distinct_address_listing_id
	UNION ALL
	SELECT 'Listing_id in address but not in listing' AS Test_Case_Name, 'Listing/Listing_address' AS Table_Name, 'listing_id' AS Column_Name, COUNT(1) AS Count, CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM distinct_address_listing_id_1
	UNION ALL
	SELECT 'Listing_id in listing_real_estate_office_rel but not in listing' AS Test_Case_Name, 'listing_real_estate_office_rel' AS Table_Name, 'listing_id' AS Column_Name, COUNT(1) AS Count, CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM distinct_office_listing_id
	UNION ALL
	SELECT 'Id in listing but not in listing_real_estate_office_rel' AS Test_Case_Name, 'listing_real_estate_office_rel' AS Table_Name, 'listing_id' AS Column_Name, COUNT(1) AS Count, CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM distinct_office_listing_id_1
	UNION ALL
	SELECT 'source_office_id in office but not in rel table' AS Test_Case_Name, 'real_estate_office' AS Table_Name, 'source_office_id' AS Column_Name, COUNT(1) AS Count, CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM distinct_office_source_office_id
	UNION ALL
	SELECT 'office_id in rel but not in office table' AS Test_Case_Name, 'listing_real_estate_office_rel' AS Table_Name, 'office_id' AS Column_Name, COUNT(1) AS Count, CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM distinct_office_source_office_id_1
	UNION ALL
	SELECT 'Listing_id in listing_participant_rel but not in listing' AS Test_Case_Name, 'listing_participant_rel' AS Table_Name, 'listing_id' AS Column_Name, COUNT(1) AS Count, CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM distinct_participant_listing_id
	UNION ALL
	SELECT 'Id in listing but not in listing_participant_rel' AS Test_Case_Name, 'listing_participant_rel' AS Table_Name, 'listing_id' AS Column_Name, COUNT(1) AS Count, CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM distinct_participant_listing_id_1
	UNION ALL
	--Association Check:
	SELECT 'Category Association check' AS Test_Case_Name,'Listing' AS Table_Name, 'listing_category_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'ERROR' ELSE 'PASS'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND listing_category_id IN (SELECT id FROM listing_category WHERE source_id = {source_id} AND batch_id = {batch_id})
	UNION ALL
	SELECT 'Status Association check' AS Test_Case_Name,'Listing' AS Table_Name, 'listing_status_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'ERROR' ELSE 'PASS'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND listing_status_id IN (SELECT id FROM listing_status WHERE source_id = {source_id} AND batch_id = {batch_id})
	UNION ALL
	SELECT 'Property Type Association check' AS Test_Case_Name,'Listing' AS Table_Name, 'property_type_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'ERROR' ELSE 'PASS'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND property_type_id IN (SELECT id FROM listing_property_type WHERE source_id = {source_id} AND batch_id = {batch_id})
	UNION ALL
	SELECT 'Property Sub Type Association check' AS Test_Case_Name,'Listing' AS Table_Name, 'property_sub_type_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'ERROR' ELSE 'PASS'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND property_sub_type_id IN (SELECT id FROM listing_property_sub_type WHERE source_id = {source_id} AND batch_id = {batch_id})
	UNION ALL
	--Listing Table Cases:
	SELECT 'Bathrooms 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND bathrooms = 0
	UNION ALL
	SELECT 'Full Bathrooms 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'full_bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND full_bathrooms = 0
	UNION ALL
	SELECT 'Half Bathrooms 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'half_bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND half_bathrooms = 0
	UNION ALL
	SELECT 'Three Quarter Bathrooms 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'three_quarter_bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND three_quarter_bathrooms = 0
	UNION ALL
	SELECT 'One Quarter Bathrooms 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'one_quarter_bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND one_quarter_bathrooms = 0
	UNION ALL
	SELECT 'Partial Bathrooms 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'partial_bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND partial_bathrooms = 0
	UNION ALL
	SELECT 'Bedrooms 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'bedrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND bedrooms = 0
	UNION ALL
	SELECT 'Room Count 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'room_count' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND room_count = 0
	UNION ALL
	SELECT 'Paking Spaces 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'num_parking_spaces' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND num_parking_spaces = 0
	UNION ALL
	SELECT 'Number of floors 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'num_floors' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND num_floors = 0
	UNION ALL
	SELECT 'Photo Count 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'photo_count' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND photo_count = 0
	UNION ALL
	SELECT 'Living Area 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'living_area_sq_ft' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND living_area_sq_ft = 0
	UNION ALL
	SELECT 'Year Built 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'year_built' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND year_built = 0
	UNION ALL
	SELECT 'Lot Size 0 or 0.00 check' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (lot_size = 0 OR lot_size = 0.00)
	UNION ALL
	SELECT 'Price 0 check' AS Test_Case_Name,'Listing' AS Table_Name, 'price' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(price AS VarChar) LIKE '0%'
	UNION ALL
	SELECT 'Bathrooms decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(bathrooms AS VarChar) LIKE '%.%'
	UNION ALL
	SELECT 'Full Bathrooms decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'full_bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(full_bathrooms AS VarChar) LIKE '%.%'
	UNION ALL
	SELECT 'Half Bathrooms decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'half_bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(half_bathrooms AS VarChar) LIKE '%.%'
	UNION ALL
	SELECT 'Three Quarter Bathrooms decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'three_quarter_bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(three_quarter_bathrooms AS VarChar) LIKE '%.%'
	UNION ALL
	SELECT 'One Quarter Bathrooms decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'one_quarter_bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(one_quarter_bathrooms AS VarChar) LIKE '%.%'
	UNION ALL
	SELECT 'Partial Bathrooms decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'partial_bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(partial_bathrooms AS VarChar) LIKE '%.%'
	UNION ALL
	SELECT 'Bedrooms decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'bedrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(bedrooms AS VarChar) LIKE '%.%'
	UNION ALL
	SELECT 'Room Count decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'room_count' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(room_count AS VarChar) LIKE '%.%'
	UNION ALL
	SELECT 'Paking Spaces decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'num_parking_spaces' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(num_parking_spaces AS VarChar) LIKE '%.%'
	UNION ALL
	SELECT 'Number of floors decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'num_floors' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(num_floors AS VarChar) LIKE '%.%'
	UNION ALL
	SELECT 'Photo Count decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'photo_count' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(photo_count AS VarChar) LIKE '%.%'
	UNION ALL
	SELECT 'Architecture Style Values check' AS Test_Case_Name,'Listing' AS Table_Name, 'architecture_style' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND 
	(LOWER(architecture_style) LIKE '%none%' OR LOWER(architecture_style) LIKE '%other%' OR LOWER(architecture_style) LIKE '%see remarks%')
	UNION ALL
	SELECT 'Architecture Style Start or End with (,)' AS Test_Case_Name,'Listing' AS Table_Name, 'architecture_style' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (architecture_style LIKE ',%' OR architecture_style LIKE '%,')
	UNION ALL
	SELECT 'Year Built less than 1800' AS Test_Case_Name,'Listing' AS Table_Name, 'year_built' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND year_built < 1800
	UNION ALL
	SELECT 'Listing Category check' AS Test_Case_Name,'Listing' AS Table_Name, 'listing_category' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing l 	join listing_category lc on l.listing_category_id = lc.id WHERE l.source_id = {source_id} AND l.batch_id = {batch_id} AND 
	lc.display_category != initcap(lc.display_category)
	UNION ALL
	SELECT 'Year Built less than 4 digits' AS Test_Case_Name,'Listing' AS Table_Name, 'year_built' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND year_built < 1000
	UNION ALL
	SELECT 'inactive_date < source_creation_date' AS Test_Case_Name,'Listing' AS Table_Name, 'source_creation_date,inactive_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND inactive_date < source_creation_date
	UNION ALL
	SELECT 'modification_timestamp > CURRENT_TIMESTAMP' AS Test_Case_Name,'Listing' AS Table_Name, 'modification_timestamp' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND modification_timestamp > CURRENT_TIMESTAMP
	UNION ALL
	-- Lot Size Test CASES
	SELECT 'Unit is sqft and value in acres' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND LOWER(lot_size_units) LIKE '%sq%' AND lot_size = lot_size_acres 
	UNION ALL
	SELECT 'Unit is acres and value in sqft' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND LOWER(lot_size_units) LIKE '%acr%' AND lot_size = lot_size_sqft
	UNION ALL
	SELECT 'acres/sqft NULL and lot_size Not NULL' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (lot_size_sqft isNULL OR lot_size_acres isNULL) AND lot_size is NOT NULL
	UNION ALL
	SELECT 'acres/sqft Not NULL and lot_size NULL' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (lot_size_sqft is NOT NULL OR lot_size_acres is NOT NULL) AND lot_size isNULL
	UNION ALL
	SELECT 'lot_size NULL and unit Not NULL' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND lot_size isNULL AND lot_size_units is NOT NULL
	UNION ALL
	SELECT 'lot_size NOT NULL and unit NULL' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND lot_size is NOT NULL AND lot_size_units isNULL
	UNION ALL
	SELECT 'lot_size wrong conversion' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND lot_size_acres <> CAST ((lot_size_sqft / 43560) AS NUMERIC (36,2))
	UNION ALL

	--listing_address
	SELECT 'subdivision_name tbd/tba/none/remark Check' AS Test_Case_Name,'Listing_address' AS Table_Name, 'subdivision_name' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND 
	(LOWER(subdivision_name) LIKE '%tbd%' OR LOWER(subdivision_name) LIKE '%tba%' OR LOWER(subdivision_name) LIKE '%remark%' OR LOWER(subdivision_name) LIKE '%none%')
	UNION ALL
	SELECT 'community_name tbd/tba/none/remark Check' AS Test_Case_Name,'Listing_address' AS Table_Name, 'community_name' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND 
	(LOWER(community_name) LIKE '%tbd%' OR LOWER(community_name) LIKE '%tba%' OR LOWER(community_name) LIKE '%remark%' OR LOWER(community_name) LIKE '%none%')
	UNION ALL
	SELECT 'unit_number tbd/tba/none/remark/0 Check' AS Test_Case_Name,'Listing_address' AS Table_Name, 'unit_number' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND 
	(LOWER(unit_number) LIKE '%tbd%' OR LOWER(unit_number) LIKE '%tba%' OR LOWER(unit_number) LIKE '%remark%' OR LOWER(unit_number) LIKE '%none%' OR unit_number = '0')
	UNION ALL
	SELECT 'parcel_id tbd/tba/none/remark/0 Check' AS Test_Case_Name,'Listing_address' AS Table_Name, 'parcel_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND 
	(LOWER(parcel_id) LIKE '%tbd%' OR LOWER(parcel_id) LIKE '%tba%' OR LOWER(parcel_id) LIKE '%remark%' OR LOWER(parcel_id) LIKE '%none%' OR parcel_id = '0')
	UNION ALL
	SELECT 'source_status is not ACTIVE' AS Test_Case_Name,'Listing_address' AS Table_Name, 'source_status' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_status <> 'ACTIVE'
	UNION ALL
	SELECT 'full_street_address tbd/0 Check' AS Test_Case_Name,'Listing_address' AS Table_Name, 'full_street_address' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND (LOWER(full_street_address) LIKE '%tbd%' OR full_street_address LIKE '0%')
	UNION ALL
	SELECT 'full_street_address tbd/0 Check' AS Test_Case_Name,'stage.direct_idx_address' AS Table_Name, 'full_street_address' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM stage.direct_idx_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND (LOWER(full_street_address) LIKE '%tbd%' OR full_street_address LIKE '0%')
	UNION ALL
	SELECT 'NULL full_street_address Check' AS Test_Case_Name,'Listing_address' AS Table_Name, 'full_street_address' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND full_street_address  isNULL
	UNION ALL
	SELECT 'NULL address_token Check' AS Test_Case_Name,'Listing_address' AS Table_Name, 'address_token' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND address_token  isNULL
	UNION ALL
	SELECT 'City not in InitCAPS Check' AS Test_Case_Name,'Listing_address' AS Table_Name, 'city' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND (city = LOWER(city) OR city = UPPER(city))
	UNION ALL
	SELECT 'Community not in CAPS Check' AS Test_Case_Name,'Listing_address' AS Table_Name, 'community_name' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND (community_name <> UPPER(community_name))
	UNION ALL
	SELECT 'Subdivision not in CAPS Check' AS Test_Case_Name,'Listing_address' AS Table_Name, 'subdivision_name' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND (subdivision_name <> UPPER(subdivision_name))
	UNION ALL
	SELECT 'full_street_address not in InitCAPS Check' AS Test_Case_Name,'Listing_address' AS Table_Name, 'full_street_address' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND (full_street_address = LOWER(full_street_address) OR full_street_address = UPPER(full_street_address))
	UNION ALL

	--NULL COLUMN CHECK
	SELECT 'NULL price check' AS Test_Case_Name,'Listing' AS Table_Name, 'price' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND price isNULL
	UNION ALL
	SELECT 'Bathrooms NULL while full/half bath not NULL' AS Test_Case_Name,'Listing' AS Table_Name, 'bathrooms' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND bathrooms isNULL AND (full_bathrooms is NOT NULL OR half_bathrooms is NOT NULL)
	UNION ALL
	SELECT 'NULL media_modification_timestamp check w.r.t photo_count' AS Test_Case_Name,'Listing' AS Table_Name, 'media_modification_timestamp' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (media_modification_timestamp isNULL AND photo_count is NOT NULL)
	UNION ALL
	SELECT 'NULL modification_timestamp Check' AS Test_Case_Name,'Listing' AS Table_Name, 'modification_timestamp' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND modification_timestamp isNULL
	UNION ALL
	SELECT 'NULL mls_board_id Check' AS Test_Case_Name,'Listing' AS Table_Name, 'mls_board_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND mls_board_id isNULL
	UNION ALL
	SELECT 'NULL mls_number check' AS Test_Case_Name,'Listing' AS Table_Name, 'mls_number' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND mls_number isNULL
	UNION ALL
	SELECT 'NULL source_listing_id check' AS Test_Case_Name,'Listing' AS Table_Name, 'source_listing_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_listing_id isNULL
	UNION ALL
	SELECT 'NULL property_type_id check' AS Test_Case_Name,'Listing' AS Table_Name, 'property_type_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND property_type_id isNULL
	UNION ALL
	SELECT 'NULL property_sub_type_id check' AS Test_Case_Name,'Listing' AS Table_Name, 'property_sub_type_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND property_sub_type_id isNULL
	UNION ALL
	SELECT 'NULL listing_category_id check' AS Test_Case_Name,'Listing' AS Table_Name, 'listing_category_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND listing_category_id isNULL
	UNION ALL
	SELECT 'NULL listing_status_id check' AS Test_Case_Name,'Listing' AS Table_Name, 'listing_status_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND listing_status_id isNULL
	UNION ALL
	SELECT 'NULL media_url of listings' AS Test_Case_Name,'Listing' AS Table_Name, 'media_url' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_photo lp LEFT JOIN listing l  ON l.id = lp.listing_id
	WHERE l.source_id = {source_id} AND l.batch_id = {batch_id} AND lp.media_url = ''
	UNION ALL
	--Listing Format Check
	SELECT 'Price Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'Price' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) =0  THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (cast(Price as VARCHAR) !~ '[0-9]' or cast(Price as VARCHAR) NOT like '%.__%' )
	UNION ALL
	SELECT 'disclose_address Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'disclose_address' AS Column_Name,COUNT(1) , CASE WHEN COUNT(1) > 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (disclose_address !='true' or  disclose_address != 'false' or disclose_address is null)
	UNION ALL
	SELECT 'Year_built Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'Year_built' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND cast(Year_built as VARCHAR) !~ '[0-9]'
	UNION ALL
	SELECT 'is_new_construction Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'is_new_construction' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'ERROR' ELSE 'PASS'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (is_new_construction = 'true' OR is_new_construction = 'false' OR is_new_construction isNULL)
	UNION ALL
	SELECT 'Lot Size Units Format check' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size_units' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (LOWER(lot_size_units) NOT LIKE 'sq%' AND LOWER(lot_size_units) NOT LIKE 'acr%')
	UNION ALL
	SELECT 'num_floors Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'num_floors' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (cast(num_floors as VARCHAR) !~ '[0-9]' or cast(num_floors as VARCHAR) like '%.%')
	UNION ALL
	SELECT 'num_parking_spaces Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'num_parking_spaces' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (cast(num_parking_spaces as VARCHAR) !~ '[0-9]' or cast(num_parking_spaces as VARCHAR) like '%.%')
	UNION ALL
	SELECT 'room_count Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'room_count' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (cast(room_count as VARCHAR) !~ '[0-9]' or cast(room_count as VARCHAR) like '%.%')
	UNION ALL
	SELECT 'photo_count Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'photo_count' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (cast(photo_count as VARCHAR) !~ '[0-9]' or cast(photo_count as VARCHAR) like '%.%')
	UNION ALL
	SELECT 'original_price Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'original_price' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (cast(original_price as VARCHAR) !~ '[0-9]' or cast(original_price as VARCHAR) not like '%.__%')
	UNION ALL
	SELECT 'prior_price Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'prior_price' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND (cast(prior_price  as VARCHAR) !~ '[0-9]' or cast(prior_price  as VARCHAR) not like '%.__%')
	UNION ALL
	SELECT 'media_modification_timestamp Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'media_modification_timestamp' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND  cast(media_modification_timestamp as VARCHAR) not LIKE '20__-__-__ __:__:__%+__'
	UNION ALL
	SELECT 'source_status Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'source_status' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_status !='ACTIVE'
	UNION ALL
	SELECT 'price_update_date Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'price_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND cast(price_update_date as VARCHAR) NOT LIKE '20__-__-__ __:__:__+__'
	UNION ALL
	SELECT 'modification_timestamp Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'modification_timestamp' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND cast(modification_timestamp as VARCHAR) NOT LIKE '%20__-__-__ __:__:__%'
	UNION ALL
	SELECT 'source_creation_date Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'source_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND cast(source_creation_date as VARCHAR) NOT LIKE '20__-__-__ __:__:__+__'
	UNION ALL
	SELECT 'source_last_update_date Format Check' AS Test_Case_Name,'Listing' AS Table_Name, 'source_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND cast(source_last_update_date as VARCHAR) NOT LIKE '20__-__-__ __:__:__+__'
	UNION ALL
	SELECT 'Lot Size 2 decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(lot_size AS DECIMAL(15,2)) - lot_size <> 0
	UNION ALL
	SELECT 'Lot Size Acres 2 decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size_acres' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(lot_size_acres AS DECIMAL(15,2)) - lot_size_acres <> 0
	UNION ALL
	SELECT 'Lot Size SQFT 2 decimal check' AS Test_Case_Name,'Listing' AS Table_Name, 'lot_size_sqft' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND CAST(lot_size_sqft AS DECIMAL(15,2)) - lot_size_sqft <> 0
	UNION ALL
	--NULL DATES:
	SELECT 'NULL source_creation_date check' AS Test_Case_Name,'Listing' AS Table_Name, 'source_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_creation_date isNULL
	UNION ALL
	SELECT 'NULL source_last_update_date check' AS Test_Case_Name,'Listing' AS Table_Name, 'source_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_last_update_date isNULL
	UNION ALL
	SELECT 'NULL y_creation_date check' AS Test_Case_Name,'Listing' AS Table_Name, 'y_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND y_creation_date isNULL
	UNION ALL
	SELECT 'NULL y_last_update_date check' AS Test_Case_Name,'Listing' AS Table_Name, 'y_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} AND y_last_update_date isNULL
	UNION ALL
	SELECT 'NULL y_creation_date check' AS Test_Case_Name,'listing_property_type' AS Table_Name, 'y_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_property_type WHERE source_id = {source_id} AND batch_id = {batch_id} AND y_creation_date isNULL
	UNION ALL
	SELECT 'NULL y_creation_date check' AS Test_Case_Name,'listing_property_sub_type' AS Table_Name, 'y_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_property_sub_type WHERE source_id = {source_id} AND batch_id = {batch_id} AND y_creation_date isNULL
	UNION ALL
	SELECT 'NULL source_creation_date check' AS Test_Case_Name,'listing_address' AS Table_Name,  'source_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_creation_date isNULL
	UNION ALL
	SELECT 'NULL source_last_update_date check' AS Test_Case_Name,'listing_address' AS Table_Name, 'source_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_last_update_date isNULL
	UNION ALL
	SELECT 'NULL y_creation_date check' AS Test_Case_Name,'listing_address' AS Table_Name, 'y_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND y_creation_date isNULL
	UNION ALL
	SELECT 'NULL y_last_update_date check' AS Test_Case_Name,'listing_address' AS Table_Name, 'y_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id} AND y_last_update_date isNULL
	UNION ALL
	SELECT 'NULL source_creation_date check' AS Test_Case_Name,'listing_description' AS Table_Name, 'ld.source_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_description ld JOIN listing l ON ld.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND ld.source_creation_date isNULL
	UNION ALL
	SELECT 'NULL source_last_update_date check' AS Test_Case_Name,'listing_description' AS Table_Name, 'ld.source_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_description ld JOIN listing l ON ld.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND ld.source_last_update_date isNULL
	UNION ALL
	SELECT 'NULL y_creation_date check' AS Test_Case_Name,'listing_description' AS Table_Name, 'ld.y_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_description ld JOIN listing l ON ld.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND ld.y_creation_date isNULL
	UNION ALL
	SELECT 'NULL y_last_update_date check' AS Test_Case_Name,'listing_description' AS Table_Name, 'ld.y_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_description ld JOIN listing l ON ld.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND ld.y_last_update_date isNULL
	UNION ALL
	SELECT 'NULL source_creation_date check' AS Test_Case_Name,'listing_openhouse' AS Table_Name, 'lo.source_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_openhouse lo JOIN listing l ON lo.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lo.source_creation_date isNULL
	UNION ALL
	SELECT 'NULL source_last_update_date check' AS Test_Case_Name,'listing_openhouse' AS Table_Name, 'lo.source_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_openhouse lo JOIN listing l ON lo.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lo.source_last_update_date isNULL
	UNION ALL
	SELECT 'NULL y_creation_date check' AS Test_Case_Name,'listing_openhouse' AS Table_Name, 'lo.y_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_openhouse lo JOIN listing l ON lo.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lo.y_creation_date isNULL
	UNION ALL
	SELECT 'NULL y_last_update_date check' AS Test_Case_Name,'listing_openhouse' AS Table_Name, 'lo.y_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_openhouse lo JOIN listing l ON lo.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lo.y_last_update_date isNULL
	UNION ALL
	SELECT 'NULL source_creation_date check' AS Test_Case_Name,'Real_estate_office' AS Table_Name, 'source_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM Real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_creation_date isNULL
	UNION ALL
	SELECT 'NULL source_last_update_date check' AS Test_Case_Name,'Real_estate_office' AS Table_Name, 'source_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM Real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_last_update_date isNULL
	UNION ALL
	SELECT 'NULL y_creation_date check' AS Test_Case_Name,'Real_estate_office' AS Table_Name, 'y_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM Real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id} AND y_creation_date isNULL
	UNION ALL
	SELECT 'NULL y_last_update_date check' AS Test_Case_Name,'Real_estate_office' AS Table_Name, 'y_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM Real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id} AND y_last_update_date isNULL
	UNION ALL
	SELECT 'NULL source_creation_date check' AS Test_Case_Name,'real_estate_participant' AS Table_Name, 'source_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_participant WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_creation_date isNULL
	UNION ALL
	SELECT 'NULL source_last_update_date check' AS Test_Case_Name,'real_estate_participant' AS Table_Name, 'source_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_participant WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_last_update_date isNULL
	UNION ALL
	SELECT 'NULL y_creation_date check' AS Test_Case_Name,'real_estate_participant' AS Table_Name, 'y_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_participant WHERE source_id = {source_id} AND batch_id = {batch_id} AND y_creation_date isNULL
	UNION ALL
	SELECT 'NULL y_last_update_date check' AS Test_Case_Name,'real_estate_participant' AS Table_Name, 'y_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_participant WHERE source_id = {source_id} AND batch_id = {batch_id} AND y_last_update_date isNULL
	UNION ALL
	SELECT 'NULL source_creation_date check' AS Test_Case_Name,'Listing_participant_rel' AS Table_Name, 'lpr.source_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM Listing_participant_rel lpr JOIN listing l ON lpr.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lpr.source_creation_date isNULL
	UNION ALL
	SELECT 'NULL source_last_update_date check' AS Test_Case_Name,'Listing_participant_rel' AS Table_Name, 'lpr.source_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM Listing_participant_rel lpr JOIN listing l ON lpr.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lpr.source_last_update_date isNULL
	UNION ALL
	SELECT 'NULL y_creation_date check' AS Test_Case_Name,'Listing_participant_rel' AS Table_Name, 'lpr.y_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM Listing_participant_rel lpr JOIN listing l ON lpr.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lpr.y_creation_date isNULL
	UNION ALL
	SELECT 'NULL y_last_update_date check' AS Test_Case_Name,'Listing_participant_rel' AS Table_Name, 'lpr.y_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM Listing_participant_rel lpr JOIN listing l ON lpr.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lpr.y_last_update_date isNULL
	UNION ALL
	SELECT 'NULL source_creation_date check' AS Test_Case_Name,'listing_real_estate_office_rel' AS Table_Name, 'lreor.source_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_real_estate_office_rel lreor JOIN listing l ON lreor.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lreor.source_creation_date isNULL
	UNION ALL
	SELECT 'NULL source_last_update_date check' AS Test_Case_Name,'listing_real_estate_office_rel' AS Table_Name, 'lreor.source_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_real_estate_office_rel lreor JOIN listing l ON lreor.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lreor.source_last_update_date isNULL
	UNION ALL
	SELECT 'NULL y_creation_date check' AS Test_Case_Name,'listing_real_estate_office_rel' AS Table_Name, 'lreor.y_creation_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_real_estate_office_rel lreor JOIN listing l ON lreor.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lreor.y_creation_date isNULL
	UNION ALL
	SELECT 'NULL y_last_update_date check' AS Test_Case_Name,'listing_real_estate_office_rel' AS Table_Name, 'lreor.y_last_update_date' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_real_estate_office_rel lreor JOIN listing l ON lreor.listing_id = l.id where l.source_id = {source_id} AND l.batch_id = {batch_id} AND lreor.y_last_update_date isNULL
	UNION ALL

	--Duplicate Check Queries:
	SELECT 'Duplicate mls_number Check' AS Test_Case_Name, 'Listing' AS Table_Name, 'mls_number' AS Column_Name, COUNT(1), CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM (SELECT COUNT(mls_number) FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} GROUP BY mls_number HAVING COUNT(mls_number) > 1 ) AS Dup_mls_number
	UNION ALL
	SELECT 'Duplicate source_listing_id Check' AS Test_Case_Name, 'Listing' AS Table_Name, 'source_listing_id' AS Column_Name, COUNT(1), CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM (SELECT COUNT(source_listing_id) FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id} GROUP BY mls_number HAVING COUNT(source_listing_id) > 1 ) AS Dup_source_listing_id
	UNION ALL
	SELECT 'source_listing_id greater than 2 Check' AS Test_Case_Name, 'listing_description' AS Table_Name, 'listing_id' AS Column_Name, COUNT(1), CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM (SELECT COUNT(ld.listing_id) FROM listing_description ld LEFT JOIN listing l ON l.id = ld.listing_id WHERE l.source_id = {source_id} AND l.batch_id = {batch_id} GROUP BY ld.listing_id HAVING COUNT(ld.listing_id) > 2 ) AS Dup_listing_id
	UNION ALL
	SELECT 'Duplicate listing in school Check' AS Test_Case_Name, 'listing_school' AS Table_Name, 'listing_id' AS Column_Name, COUNT(1), CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM (SELECT COUNT(listing_id) FROM listing_school WHERE source_id = {source_id} AND batch_id = {batch_id} GROUP BY listing_id, name , school_type , mls_school_type HAVING COUNT(listing_id) > 1) AS Dup_school_listing
	UNION ALL
	SELECT 'No more than 3 same listing in school Check' AS Test_Case_Name, 'listing_school' AS Table_Name, 'listing_id' AS Column_Name, COUNT(1), CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM (SELECT COUNT(listing_id) FROM listing_school WHERE source_id = {source_id} AND batch_id = {batch_id} GROUP BY listing_id HAVING COUNT(listing_id) > 3) AS Extra_school_listing
	UNION ALL
	SELECT 'Duplicate listing in school district Check' AS Test_Case_Name, 'listing_school_district' AS Table_Name, 'listing_id' AS Column_Name, COUNT(1), CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM (SELECT COUNT(listing_id) FROM listing_school_district WHERE source_id = {source_id} AND batch_id = {batch_id} GROUP BY listing_id, district , school_category HAVING COUNT(listing_id) > 1) AS Dup_school_district_listing
	UNION ALL
	SELECT 'Duplicate mls_number with same rank office Check' AS Test_Case_Name, 'listing_real_estate_office_rel' AS Table_Name, 'mls_number' AS Column_Name, COUNT(1), CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM (SELECT COUNT (l.mls_number) FROM listing l JOIN listing_real_estate_office_rel lrel ON lrel.listing_id = l.id 
	LEFT JOIN real_estate_office lo ON lo.source_office_id = lrel.office_id AND lo.source_id = l.source_id WHERE l.source_id = {source_id} AND l.batch_id = {batch_id} GROUP BY l.mls_number,lo.source_office_id,lrel.rank HAVING COUNT (l.mls_number) > 1) AS Dup_mls_number_office
	UNION ALL
	SELECT 'Duplicate source_office_id Check' AS Test_Case_Name, 'real_estate_office' AS Table_Name, 'source_office_id' AS Column_Name, COUNT(1), CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM (SELECT COUNT(source_office_id) FROM real_estate_office  WHERE source_id = {source_id} AND batch_id = {batch_id} GROUP BY source_office_id HAVING COUNT(source_office_id) > 1) AS Dup_office
	UNION ALL
	SELECT 'Duplicate office name Check' AS Test_Case_Name, 'real_estate_office' AS Table_Name, 'office_name' AS Column_Name, COUNT(1), CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM (SELECT COUNT(office_name) FROM real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id} GROUP BY office_name HAVING COUNT(office_name) > 1) AS Dup_office_name
	UNION ALL
	SELECT 'Duplicate mls_number with same rank agent Check' AS Test_Case_Name, 'listing_participant_rel' AS Table_Name, 'mls_number' AS Column_Name, COUNT(1), CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM (SELECT COUNT (l.mls_number) FROM listing l JOIN listing_participant_rel lrel ON lrel.listing_id = l.id
	LEFT JOIN real_estate_participant lo ON lo.source_participant_id = lrel.participant_id AND lo.source_id = l.source_id WHERE l.source_id = {source_id} AND l.batch_id = {batch_id} GROUP BY l.mls_number,lo.source_participant_id,lo.participant_id,lo.first_name,lo.last_name,lo.full_name,lrel.rank HAVING COUNT (l.mls_number) > 1) AS Dup_mls_number_agent
	UNION ALL
	SELECT 'Duplicate source_participant_id Check' AS Test_Case_Name, 'real_estate_participant' AS Table_Name, 'source_participant_id' AS Column_Name, COUNT(1), CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR' END AS RESULT
	FROM (SELECT COUNT(source_participant_id) FROM real_estate_participant  WHERE source_id = {source_id} AND batch_id = {batch_id} GROUP BY source_participant_id HAVING COUNT(source_participant_id) > 1) AS Dup_agent
	UNION ALL

	--OFFICES
	SELECT 'office_name NULL Check' AS Test_Case_Name,'real_estate_office' AS Table_Name, 'office_name' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id} AND office_name isNULL
	UNION ALL
	SELECT 'office_id NULL Check' AS Test_Case_Name,'real_estate_office' AS Table_Name, 'office_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id} AND office_id isNULL 
	UNION ALL
	SELECT 'source_office_id NULL Check' AS Test_Case_Name,'real_estate_office' AS Table_Name, 'source_office_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_office_id isNULL
	UNION ALL
	SELECT 'phone_number length Check' AS Test_Case_Name,'real_estate_office' AS Table_Name, 'phone_number' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id} AND LENGTH(phone_number) > 12
	UNION ALL
	SELECT 'office_email format Check' AS Test_Case_Name,'real_estate_office' AS Table_Name, 'office_email' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_office WHERE source_id = {source_id} AND batch_id = {batch_id} AND office_email NOT LIKE '%_@_%.__%'
	UNION ALL
	SELECT 'Rank greater than 4 Check' AS Test_Case_Name,'listing_real_estate_office_rel' AS Table_Name, 'rank' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_real_estate_office_rel lreo JOIN listing l ON lreo.listing_id = l.id WHERE l.source_id = {source_id} AND l.batch_id = {batch_id} AND lreo.rank > 4
	UNION ALL

	--PARTICIPANTS:
	SELECT 'source_participant_id NULL Check' AS Test_Case_Name,'real_estate_participant' AS Table_Name, 'source_participant_id' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_participant WHERE source_id = {source_id} AND batch_id = {batch_id} AND source_participant_id isNULL
	UNION ALL
	SELECT 'First_Name/Last_Name NULL Check' AS Test_Case_Name,'real_estate_participant' AS Table_Name, 'first_name/last_name' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_participant WHERE source_id = {source_id} AND batch_id = {batch_id} AND (first_name isNULL OR last_name isNULL)
	UNION ALL
	SELECT 'full_name concatenation Check' AS Test_Case_Name,'real_estate_participant' AS Table_Name, 'full_name' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_participant WHERE source_id = {source_id} AND batch_id = {batch_id} AND full_name <> CONCAT(first_name , ' ', last_name)
	UNION ALL
	SELECT 'first_name and last_name NULL but full_name NOT NULL Check' AS Test_Case_Name,'real_estate_participant' AS Table_Name, 'full_name' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_participant WHERE source_id = {source_id} AND batch_id = {batch_id} AND (first_name isNULL AND last_name isNULL AND full_name is NOT NULL)
	UNION ALL
	SELECT 'first_name and last_name NOT NULL but full_name NULL Check' AS Test_Case_Name,'real_estate_participant' AS Table_Name, 'full_name' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_participant WHERE source_id = {source_id} AND batch_id = {batch_id} AND (first_name is NOT NULL AND last_name is NOT NULL AND full_name isNULL)
	UNION ALL
	SELECT 'Agent Email format Check' AS Test_Case_Name,'real_estate_participant' AS Table_Name, 'email' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM real_estate_participant WHERE source_id = {source_id} AND batch_id = {batch_id} AND email NOT LIKE '%_@_%.__%'
	UNION ALL
	SELECT 'Rank greater than 4 Check' AS Test_Case_Name,'listing_participant_rel' AS Table_Name, 'rank' AS Column_Name, COUNT(1) , CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'ERROR'END AS RESULT
	FROM listing_participant_rel lpr JOIN listing l ON lpr.listing_id = l.id WHERE l.source_id = {source_id} AND l.batch_id = {batch_id} AND lpr.rank > 4 ORDER BY 2 DESC) 
	SQ WHERE SQ.RESULT = 'ERROR'
    '''
    cur_stage.execute(counts_query)
    counts = cur_stage.fetchall()
    df = pd.DataFrame(counts, columns=['testcase', 'table', 'final_result'])
    for index, row in df.iterrows():
        report.append({
            'table_name': row['table'],
            'check_name': "Test Case",
            'status': 'Failed',
            'results': f"{row['testcase']}, {row['table']}, {row['final_result']}"
        })

    return report
