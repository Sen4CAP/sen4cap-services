SELECT 	split_part(d.product_name, '_', 3) as orbit,
	split_part(d.product_name, '_', 4) as acquisition_date,
	d.product_name as acquisition,
	ds.status_description as status,
	case when d.status_id in (1,2,3,4,41,5) then d.status_reason
		when d.status_id in (6,7,8) then concat('clouds:',coalesce(th.cloud_coverage::varchar,'n/a'),'; snow:',coalesce(th.snow_coverage::varchar,'n/a'),'; failure:',coalesce(regexp_replace(th.failed_reason,E'[\\n]+',' ','g'),'n/a'))
		else null end as status_reason,
	p.name as l2_product,
	coalesce(th.cloud_coverage, -1) as clouds
	FROM downloader_history d
		JOIN downloader_status ds ON ds.id = d.status_id
		LEFT JOIN l1_tile_history th ON th.downloader_history_id = d.id
		LEFT JOIN product p ON REPLACE(p.name, '_L2A_', '_L1TP_') = d.product_name WHERE d.satellite_id = 2 and d.site_id = ?
ORDER BY d.orbit_id, acquisition_date, d.product_name, l2_product;
