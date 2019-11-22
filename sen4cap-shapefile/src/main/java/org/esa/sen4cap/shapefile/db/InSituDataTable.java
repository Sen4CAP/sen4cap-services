/*
 *
 *  * Copyright (C) 2019 CS ROMANIA
 *  *
 *  * This program is free software; you can redistribute it and/or modify it
 *  * under the terms of the GNU General Public License as published by the Free
 *  * Software Foundation; either version 3 of the License, or (at your option)
 *  * any later version.
 *  * This program is distributed in the hope that it will be useful, but WITHOUT
 *  * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 *  * more details.
 *  *
 *  * You should have received a copy of the GNU General Public License along
 *  * with this program; if not, see http://www.gnu.org/licenses/
 *
 */

package org.esa.sen4cap.shapefile.db;

import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.shapefile.entities.VectorRecord;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import javax.sql.DataSource;
import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author Cosmin Cara
 */
public abstract class InSituDataTable<RecordType extends VectorRecord> {
    protected final String name;
    protected final DataSource dataSource;
    protected final Logger logger;

    protected InSituDataTable(String name, DataSource dataSource) {
        this.name = name;
        this.dataSource = dataSource;
        this.logger = Logger.getLogger(getClass().getSimpleName());
    }

    public String getName() {
        return name;
    }

    public abstract List<RecordType> select(Site site, String wkt) throws DataAccessException;

    public abstract int[] insertOrUpdate(List<RecordType> rows) throws DataAccessException;

    public abstract int[] delete(List<RecordType> rows) throws DataAccessException;

    public boolean deleteOlderThan(Site site, Date reference) throws DataAccessException {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Map<String, Object> mapParams = new HashMap<>();
        mapParams.put("siteId", site.getId());
        mapParams.put("updated", reference);
        MapSqlParameterSource params = new MapSqlParameterSource(mapParams);
        return jdbcTemplate.update("DELETE FROM " + name + " WHERE site_id = :siteId AND updated < :updated", params) == 1;
    }
}
