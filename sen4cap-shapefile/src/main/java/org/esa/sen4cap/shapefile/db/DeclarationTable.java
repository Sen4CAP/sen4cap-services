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
import org.esa.sen4cap.shapefile.entities.Declaration;
import org.locationtech.jts.geom.Geometry;
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
public class DeclarationTable extends InSituDataTable<Declaration> {

    DeclarationTable(String name, DataSource dataSource) {
        super(name, dataSource);
    }

    @Override
    public List<Declaration> select(Site site, String wkt) {
        List<Declaration> rows = new ArrayList<>();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        final WKTReader wktReader = new WKTReader();
        jdbcTemplate.query(connection ->
                           {
                               PreparedStatement statement =
                                       connection.prepareStatement("SELECT lpis_id, parcel_id, orig_land_use_code, sys_land_use_code, area, st_astext(footprint), updated, source, attributes " +
                                                                           "FROM declaration WHERE site_id = ? AND st_intersects(footprint, st_geogfromtext(?));");
                               statement.setInt(1, site.getId());
                               statement.setString(2, wkt);
                               return statement;
                           },
                           (resultSet, i) -> {
                               Declaration declaration = new Declaration();
                               declaration.setLpisId(resultSet.getString(1));
                               declaration.setParcelId(resultSet.getString(2));
                               declaration.setSite(site);
                               declaration.setOriginalLandUseCode(resultSet.getString(3));
                               declaration.setSystemLandUseCode(resultSet.getInt(4));
                               declaration.setArea(resultSet.getDouble(5));
                               try {
                                   declaration.setFootprint(wktReader.read(resultSet.getString(6)));
                               } catch (ParseException e) {
                                   logger.warning(e.getMessage());
                               }
                               declaration.setDate(resultSet.getDate(7).toLocalDate());
                               declaration.setSourceFile(resultSet.getString(8));
                               declaration.setAttributes(resultSet.getString(9));
                               return declaration;
                           });
        return rows;
    }

    @Override
    public int[] insertOrUpdate(List<Declaration> rows) throws DataAccessException {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Date updated = Date.valueOf(LocalDate.now());
        int[] r = jdbcTemplate.batchUpdate("INSERT INTO declaration (lpis_id, parcel_id, site_id, orig_land_use_code, sys_land_use_code, area, footprint, updated, source, attributes) " +
                                                "VALUES (?,?,?,?,?,?,st_geogfromtext(?),?,?,?) ON CONFLICT (lpis_id, parcel_id) " +
                                                "DO UPDATE SET orig_land_use_code = ?, sys_land_use_code = ?, area = ?, footprint = st_geogfromtext(?), updated = ?, source = ?, attributes = ?",
                                        new BatchPreparedStatementSetter() {
                                            @Override
                                            public void setValues(PreparedStatement preparedStatement, int row) throws SQLException {
                                                Declaration declaration = rows.get(row);
                                                preparedStatement.setString(1, declaration.getLpisId());
                                                preparedStatement.setString(2, declaration.getParcelId());
                                                preparedStatement.setShort(3, declaration.getSite().getId());
                                                preparedStatement.setString(4, declaration.getOriginalLandUseCode());
                                                Integer code = declaration.getSystemLandUseCode();
                                                if (code != null) {
                                                    preparedStatement.setInt(5, code);
                                                } else {
                                                    preparedStatement.setNull(5, Types.INTEGER);
                                                }
                                                Double area = declaration.getArea();
                                                if (area != null) {
                                                    preparedStatement.setDouble(6, area);
                                                } else {
                                                    preparedStatement.setNull(6, Types.DOUBLE);
                                                }
                                                Geometry footprint = declaration.getFootprint();
                                                String wkt = footprint == null ? "POLYGON EMPTY" : footprint.toText();
                                                preparedStatement.setString(7, wkt);
                                                preparedStatement.setDate(8, updated);
                                                preparedStatement.setString(9, declaration.getSourceFile());
                                                preparedStatement.setString(10, declaration.getAttributes());
                                                preparedStatement.setString(11, declaration.getOriginalLandUseCode());
                                                if (code != null) {
                                                    preparedStatement.setInt(12, code);
                                                } else {
                                                    preparedStatement.setNull(12, Types.INTEGER);
                                                }
                                                if (area != null) {
                                                    preparedStatement.setDouble(13, area);
                                                } else {
                                                    preparedStatement.setNull(13, Types.DOUBLE);
                                                }
                                                preparedStatement.setString(14, wkt);
                                                preparedStatement.setDate(15, updated);
                                                preparedStatement.setString(16, declaration.getSourceFile());
                                                preparedStatement.setString(17, declaration.getAttributes());
                                            }

                                            @Override
                                            public int getBatchSize() {
                                                return rows.size();
                                            }
                                        });
        return r;
    }

    @Override
    public int[] delete(List<Declaration> rows) throws DataAccessException {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return jdbcTemplate.batchUpdate("DELETE FROM declaration WHERE lpis_id = ? AND parcel_id = ? AND site_id = ?;",
                                        new BatchPreparedStatementSetter() {
                                            @Override
                                            public void setValues(PreparedStatement preparedStatement, int row) throws SQLException {
                                                Declaration declaration = rows.get(row);
                                                preparedStatement.setString(1, declaration.getLpisId());
                                                preparedStatement.setString(2, declaration.getParcelId());
                                                preparedStatement.setShort(3, declaration.getSite().getId());
                                            }

                                            @Override
                                            public int getBatchSize() {
                                                return rows.size();
                                            }
                                        });
    }
}
