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
import org.esa.sen4cap.shapefile.entities.Parcel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Cosmin Cara
 */
public class LPISTable extends InSituDataTable<Parcel> {

    LPISTable(String name, DataSource dataSource) {
        super(name, dataSource);
    }

    @Override
    public List<Parcel> select(Site site, String wkt) throws DataAccessException {
        List<Parcel> rows = new ArrayList<>();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        final WKTReader wktReader = new WKTReader();
        jdbcTemplate.query(connection ->
                {
                    PreparedStatement statement =
                            connection.prepareStatement("SELECT identifier, orig_land_use_code, sys_land_use_code, area, st_astext(footprint), updated, source, attributes " +
                                    "FROM lpis WHERE site_id = ? AND st_intersects(footprint, st_geogfromtext(?));");
                    statement.setInt(1, site.getId());
                    statement.setString(2, wkt);
                    return statement;
                },
                (resultSet, i) -> {
                    Parcel parcel = new Parcel();
                    parcel.setIdentifier(resultSet.getString(1));
                    parcel.setSite(site);
                    parcel.setOriginalLandUseCode(resultSet.getString(2));
                    parcel.setSystemLandUseCode(resultSet.getInt(3));
                    parcel.setArea(resultSet.getDouble(4));
                    try {
                        parcel.setFootprint(wktReader.read(resultSet.getString(5)));
                    } catch (ParseException e) {
                        logger.warning(e.getMessage());
                    }
                    parcel.setDate(resultSet.getDate(6).toLocalDate());
                    parcel.setSourceFile(resultSet.getString(7));
                    String attributes;
                    if ((attributes = resultSet.getString(8)) != null) {
                        parcel.setAttributes(attributes);
                    }
                    return parcel;
                });
        return rows;
    }

    @Override
    public int[] insertOrUpdate(List<Parcel> rows) throws DataAccessException {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Date updated = Date.valueOf(LocalDate.now());
        return jdbcTemplate.batchUpdate("INSERT INTO lpis (identifier, site_id, orig_land_use_code, sys_land_use_code, area, footprint, updated, source, attributes) " +
                                         "VALUES (?,?,?,?,?,st_geogfromtext(?),?,?,?) ON CONFLICT (identifier) " +
                                                "DO UPDATE SET orig_land_use_code = ?, sys_land_use_code = ?, area = ?, footprint = st_geogfromtext(?), updated = ?, source = ?, attributes = ?",
                                 new BatchPreparedStatementSetter() {
                                     @Override
                                     public void setValues(PreparedStatement preparedStatement, int row) throws SQLException {
                                         Parcel parcel = rows.get(row);
                                         preparedStatement.setString(1, parcel.getIdentifier());
                                         preparedStatement.setShort(2, parcel.getSite().getId());
                                         preparedStatement.setString(3, parcel.getOriginalLandUseCode());
                                         Integer code = parcel.getSystemLandUseCode();
                                         if (code != null) {
                                             preparedStatement.setInt(4, code);
                                         } else {
                                             preparedStatement.setNull(4, Types.INTEGER);
                                         }
                                         Double area = parcel.getArea();
                                         if (area != null) {
                                             preparedStatement.setDouble(5, area);
                                         } else {
                                             preparedStatement.setNull(5, Types.DOUBLE);
                                         }
                                         preparedStatement.setString(6, parcel.getFootprint().toText());
                                         preparedStatement.setDate(7, updated);
                                         preparedStatement.setString(8, parcel.getSourceFile());
                                         preparedStatement.setString(9, parcel.getAttributes());
                                         preparedStatement.setString(10, parcel.getOriginalLandUseCode());
                                         if (code != null) {
                                             preparedStatement.setInt(11, code);
                                         } else {
                                             preparedStatement.setNull(11, Types.INTEGER);
                                         }
                                         if (area != null) {
                                             preparedStatement.setDouble(12, area);
                                         } else {
                                             preparedStatement.setNull(12, Types.DOUBLE);
                                         }
                                         preparedStatement.setString(13, parcel.getFootprint().toText());
                                         preparedStatement.setDate(14, updated);
                                         preparedStatement.setString(15, parcel.getSourceFile());
                                         preparedStatement.setString(16, parcel.getAttributes());
                                     }

                                     @Override
                                     public int getBatchSize() {
                                         return rows.size();
                                     }
                                 });
    }

    @Override
    public int[] delete(List<Parcel> rows) throws DataAccessException {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return jdbcTemplate.batchUpdate("DELETE FROM lpis WHERE identifier = ? AND site_id = ?;",
                                        new BatchPreparedStatementSetter() {
                                            @Override
                                            public void setValues(PreparedStatement preparedStatement, int row) throws SQLException {
                                                Parcel parcel = rows.get(row);
                                                preparedStatement.setString(1, parcel.getIdentifier());
                                                preparedStatement.setShort(2, parcel.getSite().getId());
                                            }

                                            @Override
                                            public int getBatchSize() {
                                                return rows.size();
                                            }
                                        });
    }
}
