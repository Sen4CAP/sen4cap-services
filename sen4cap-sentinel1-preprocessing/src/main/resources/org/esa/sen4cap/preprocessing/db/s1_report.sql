WITH d AS (select dh.*,ds.status_description from downloader_history dh join downloader_status ds on ds.id = dh.status_id)
select 	d.id,
	d.orbit_id as orbit,
 	substr(split_part(d.product_name, '_', 6), 1, 8) as acquisition_date, d.product_name as acquisition,
 	d.status_description as acquisition_status,
 	substr(split_part(i.product_name, '_', 6), 1, 8) as intersection_date,
 	i.product_name as intersected_product,
 	i.status_id as intersected_status,
 	concat(to_char(st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) * 100, '999D99'), '%') as intersection,
 	split_part(p.name, '_', 6) as polarisation,
 	p.name as l2_product,
 	concat(to_char(st_area(st_intersection(d.footprint, p.geog))/st_area(d.footprint) * 100, '999D99'), '%') as l2_coverage,
 	ps.min_value, ps.max_value, ps.mean_value, ps.std_dev, d.status_reason
	from d
	join downloader_history i on i.site_id = d.site_id AND i.orbit_id = d.orbit_id AND i.satellite_id = d.satellite_id and st_intersects(d.footprint, i.footprint) AND DATE_PART('day', d.product_date - i.product_date) BETWEEN 5 AND 7 AND st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) > 0.05
	join product p on p.downloader_history_id = d.id
	left outer join product_stats ps on ps.product_id = p.id
	WHERE d.site_id = ? AND d.satellite_id = 3 and i.id is not null
		and p.name like concat('%', substr(split_part(i.product_name, '_', 6), 1, 15),'%')
union
select 	d.id,
	d.orbit_id as orbit,
 	substr(split_part(d.product_name, '_', 6), 1, 8) as acquisition_date, d.product_name as acquisition,
 	d.status_description as acquisition_status,
 	substr(split_part(i.product_name, '_', 6), 1, 8) as intersection_date,
 	i.product_name as intersected_product,
 	i.status_id as intersected_status,
 	concat(to_char(st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) * 100, '999D99'), '%') as intersection,
 	split_part(p.name, '_', 6) as polarisation,
 	p.name as l2_product,
 	concat(to_char(st_area(st_intersection(d.footprint, p.geog))/st_area(d.footprint) * 100, '999D99'), '%') as l2_coverage,
 	ps.min_value, ps.max_value, ps.mean_value, ps.std_dev, d.status_reason
	from d
	join downloader_history i on i.site_id = d.site_id AND i.orbit_id = d.orbit_id AND i.satellite_id = d.satellite_id and st_intersects(d.footprint, i.footprint) AND DATE_PART('day', d.product_date - i.product_date) BETWEEN 11 AND 13 AND st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) > 0.05
	join product p on p.downloader_history_id = d.id
	left outer join product_stats ps on ps.product_id = p.id
	WHERE d.site_id = ? AND d.satellite_id = 3 and i.id is not null and left(d.product_name, 3) = left(i.product_name, 3)
		and p.name like concat('%', substr(split_part(i.product_name, '_', 6), 1, 15),'%')
union
select 	d.id,
	d.orbit_id as orbit,
	substr(split_part(d.product_name, '_', 6), 1, 8) as acquisition_date, d.product_name as acquisition,
 	ds.status_description as acquisition_status,
 	substr(split_part(i.product_name, '_', 6), 1, 8) as intersection_date,
 	i.product_name as intersected_product,
 	i.status_id as intersected_status,
 	case when i.footprint is null then null else concat(to_char(st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) * 100, '999D99'), '%') end as intersection,
 	null as polarisation,
 	null as l2_product,
 	null as l2_coverage,
 	null as min_value, null as max_value, null as mean_value, null as std_dev, null as status_reason
	from downloader_history d
		join downloader_status ds on ds.id = d.status_id
		left outer join downloader_history i on i.site_id = d.site_id AND i.orbit_id = d.orbit_id AND i.satellite_id = d.satellite_id and st_intersects(d.footprint, i.footprint) AND DATE_PART('day', d.product_date - i.product_date) BETWEEN 5 AND 7 AND st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) > 0.05
	where d.site_id = ? AND d.satellite_id = 3 and d.status_id != 5