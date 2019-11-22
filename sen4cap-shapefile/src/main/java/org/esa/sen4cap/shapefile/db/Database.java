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

import org.esa.sen2agri.commons.Config;
import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.shapefile.entities.Practice;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.sql.DataSource;
import java.util.logging.Logger;

/**
 * @author Cosmin Cara
 */
public final class Database {

    private static final Logger logger = Logger.getLogger(Database.class.getSimpleName());

    public static void checkMasterTables() throws Exception {
        DataSource dataSource = Config.getPersistenceManager().getDataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT table_name FROM information_schema.tables WHERE table_name = 'lpis'");
        if (!rowSet.next()) {
            logger.warning("LPIS master table was not found and it will be created");
            String[] statements = new String[]{
                    "CREATE TABLE lpis (identifier character varying NOT NULL, " +
                            "site_id smallint NOT NULL, " +
                            "orig_land_use_code character varying NOT NULL, " +
                            "sys_land_use_code integer, " +
                            "area double precision, " +
                            "footprint geography NOT NULL, " +
                            "source character varying NOT NULL, " +
                            "updated date NOT NULL, " +
                            "attributes character varying NULL, " +
                            "CONSTRAINT pk_lpis PRIMARY KEY (identifier), " +
                            "CONSTRAINT fk_site FOREIGN KEY (site_id) REFERENCES site (id) MATCH SIMPLE " +
                            "ON UPDATE NO ACTION ON DELETE NO ACTION ) WITH (OIDS=FALSE);",
                    "ALTER TABLE lpis OWNER TO postgres;",
                    "CREATE INDEX idx_lpis ON lpis USING GIST(footprint);",
                    "CREATE UNIQUE INDEX idx_lpis_2 ON lpis USING BTREE(site_id ASC, identifier varchar_pattern_ops ASC);"
            };
            for (String statement : statements) {
                try {
                    jdbcTemplate.execute(statement);
                } catch (DataAccessException ex) {
                    logger.severe(String.format("SQL Statement failed ('%s'): %s", statement, ex.getMessage()));
                    break;
                }
            }
        }
        rowSet = jdbcTemplate.queryForRowSet("SELECT table_name FROM information_schema.tables WHERE table_name = 'declaration'");
        if (!rowSet.next()) {
            logger.warning("DECLARATION master table was not found and it will be created");
            String[] statements = new String[]{
                    "CREATE TABLE declaration (lpis_id character varying NOT NULL, " +
                            "parcel_id character varying NOT NULL, " +
                            "site_id smallint NOT NULL, " +
                            "orig_land_use_code character varying NOT NULL, " +
                            "sys_land_use_code integer, " +
                            "area double precision, " +
                            "footprint geography NOT NULL, " +
                            "updated date NOT NULL, " +
                            "source character varying NOT NULL, " +
                            "attributes character varying NULL, " +
                            "CONSTRAINT pk_gsaa PRIMARY KEY (lpis_id, parcel_id), " +
                            "CONSTRAINT fk_lpis FOREIGN KEY (lpis_id) REFERENCES lpis (identifier) MATCH SIMPLE " +
                            "ON UPDATE NO ACTION ON DELETE NO ACTION, " +
                            "CONSTRAINT fk_site FOREIGN KEY (site_id) REFERENCES site (id) MATCH SIMPLE " +
                            "ON UPDATE NO ACTION ON DELETE NO ACTION) WITH (OIDS=FALSE);",
                    "ALTER TABLE declaration OWNER TO postgres;",
                    "CREATE INDEX idx_declaration ON declaration USING GIST(footprint);",
                    "CREATE UNIQUE INDEX idx_declaration_2 ON declaration USING BTREE(site_id ASC, lpis_id varchar_pattern_ops ASC, parcel_id varchar_pattern_ops ASC);"
            };
            for (String statement : statements) {
                try {
                    jdbcTemplate.execute(statement);
                } catch (DataAccessException ex) {
                    logger.severe(String.format("SQL Statement failed ('%s'): %s", statement, ex.getMessage()));
                    break;
                }
            }
        }
    }

    public static InSituDataTable getTable(Site site, DataType tableType) throws Exception {
        checkMasterTables();
        InSituDataTable table = null;
        DataSource dataSource = Config.getPersistenceManager().getDataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String tableName = String.format("%s_%s", tableType.baseName(), site.getId());
        final SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT table_name FROM information_schema.tables WHERE table_name = ?",
                                                             tableName);
        if (rowSet.wasNull()) {
            table = createTable(site, tableType);
        } else {
            switch (tableType) {
                case LPIS:
                    table = new LPISTable(tableName, dataSource);
                    break;
                case DECLARATION:
                    table = new DeclarationTable(tableName, dataSource);
                    break;
            }
        }
        return table;
    }

    public static boolean deleteTable(Site site, DataType tableType) throws Exception {
        DataSource dataSource = Config.getPersistenceManager().getDataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String tableName = String.format("%s_%s", tableType.baseName(), site.getId());
        String[] statements = new String[] {
                String.format("DROP RULE insert_%s ON %s;", tableName, tableType.baseName()),
        };
        boolean succeeded = false;
        for (String statement : statements) {
            try {
                jdbcTemplate.execute(statement);
                succeeded = true;
            } catch (DataAccessException ex) {
                succeeded = false;
                logger.severe(String.format("SQL Statement failed ('%s'): %s", statement, ex.getMessage()));
                break;
            }
        }
        return succeeded;
    }

    public static void maintainTable(DataType tableType) throws Exception {
        DataSource dataSource = Config.getPersistenceManager().getDataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String[] statements = new String[] {
                String.format("VACUUM %s", tableType.baseName()),
                String.format("REINDEX TABLE %s", tableType.baseName())
        };
        for (String statement : statements) {
            try {
                logger.fine(statement);
                jdbcTemplate.execute(statement);
            } catch (DataAccessException ex) {
                logger.severe(String.format("SQL Statement failed ('%s'): %s", statement, ex.getMessage()));
                break;
            }
        }
    }

    public static String getL4CProduct(int siteId, int year, Practice practice) throws DataAccessException {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(Config.getPersistenceManager().getDataSource());

        return jdbcTemplate.queryForObject("SELECT full_path FROM product WHERE site_id = ? AND product_type_id = (SELECT id FROM product_type WHERE name = 's4c_l4c') AND extract(year FROM created_timestamp) = ? ORDER BY created_timestamp desc LIMIT 1",
                new Object[] { siteId, year }, String.class);
    }

    private static InSituDataTable createTable(Site site, DataType tableType) throws Exception {
        InSituDataTable newTable = null;
        DataSource dataSource = Config.getPersistenceManager().getDataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String tableName = String.format("%s_%s", tableType.baseName(), site.getId());
        String id = tableType.equals(DataType.LPIS) ? "identifier" : "lpis_id";
        String[] statements = new String[] {
                String.format("CREATE TABLE %s ( CHECK(site_id = %s) ) INHERITS (%s);",
                              tableName, site.getId(), tableType.baseName()),
                String.format("CREATE RULE insert_%s AS \n" +
                                      "ON INSERT TO %s WHERE (site_id = %s)\n" +
                                      "DO INSTEAD INSERT INTO %s VALUES (NEW.*);",
                              tableName, tableType.baseName(), site.getId(), tableName)
        };
        boolean succeeded = false;
        for (String statement : statements) {
            try {
                jdbcTemplate.execute(statement);
                succeeded = true;
            } catch (DataAccessException ex) {
                succeeded = false;
                logger.severe(String.format("SQL Statement failed ('%s'): %s", statement, ex.getMessage()));
                break;
            }
        }
        if (succeeded) {
            switch (tableType) {
                case LPIS:
                    newTable = new LPISTable(tableName, dataSource);
                    break;
                case DECLARATION:
                    newTable = new DeclarationTable(tableName, dataSource);
                    break;
            }
        }
        return newTable;
    }
}
