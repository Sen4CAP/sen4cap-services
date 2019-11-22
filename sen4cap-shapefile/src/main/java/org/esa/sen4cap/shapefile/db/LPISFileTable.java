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
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class LPISFileTable {
    private static final LPISFileTable instance = new LPISFileTable();
    private final JdbcTemplate jdbcTemplate;
    private final Logger logger;

    public static LPISFileTable getInstance() { return instance; }

    private LPISFileTable() {
        this.jdbcTemplate = new JdbcTemplate(Config.getPersistenceManager().getDataSource());
        this.logger = Logger.getLogger(GeneralizedLPISTable.class.getName());
        create();
    }

    private void create() {
        SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT table_name FROM information_schema.tables WHERE table_name = 'lpis_gsaa_file'");
        if (!rowSet.next()) {
            logger.info("Table 'lpis_gsaa_file' was not found and it will be created");
            List<String> statements = new LinkedList<>();
            StringBuilder statementBuilder = new StringBuilder();
            statementBuilder.append("CREATE TABLE lpis_gsaa_file (id serial not null, site_id smallint not null, file_name text not null, file_type text not null,")
                            .append("year int not null, crs text not null, created timestamp with time zone not null,")
                            .append("updated timestamp with time zone not null,")
                            .append("CONSTRAINT pk_lpis_gsaa_file PRIMARY KEY (site_id, file_type, year)) WITH (OIDS=FALSE);");
            statements.add(statementBuilder.toString());
            statementBuilder.setLength(0);
            statementBuilder.append("ALTER TABLE lpis_gsaa_file OWNER TO postgres;");
            statements.add(statementBuilder.toString());
            statementBuilder.setLength(0);
            statementBuilder.append("CREATE SEQUENCE lpis_gsaa_file_sequence INCREMENT BY 1 MINVALUE 1 START WITH 1 NO CYCLE;");
            statements.add(statementBuilder.toString());
            statementBuilder.setLength(0);
            statementBuilder.append("ALTER TABLE lpis_gsaa_file ALTER COLUMN id SET DEFAULT nextval('lpis_gsaa_file_sequence');");
            statements.add(statementBuilder.toString());
            statementBuilder.setLength(0);
            statementBuilder.append("ALTER SEQUENCE lpis_gsaa_file_sequence OWNED BY lpis_gsaa_file.id;");
            for (String statement : statements) {
                try {
                    logger.finest(statement);
                    jdbcTemplate.execute(statement);
                } catch (DataAccessException ex) {
                    logger.severe(String.format("SQL Statement failed ('%s'): %s", statement, ex.getMessage()));
                    break;
                }
            }
        } else {
            logger.warning("Table 'lpis_gsaa_file' already exists");
        }
    }

    private Record newRow() { return new Record(); }

    public int insertOrUpdate(String fileName, Site site, DataType fileType, int year, String crs) throws DataAccessException {
        Record record = new Record();
        record.setSiteId(site.getId());
        record.setFileName(fileName);
        record.setFileType(fileType);
        record.setYear(year);
        record.setCrs(crs);
        return insertOrUpdate(record);
    }

    public int insertOrUpdate(Record fileRecord) throws DataAccessException {
        return jdbcTemplate.update("INSERT INTO lpis_gsaa_file (site_id, file_name, file_type, year, crs, created, updated) " +
                                                "VALUES (?,?,?,?,?,?,?) ON CONFLICT (site_id, file_type, year) DO UPDATE SET " +
                                                "file_name = ?, crs = ?, updated = ?",
                                   preparedStatement -> {
                                       preparedStatement.setInt(1, fileRecord.siteId);
                                       preparedStatement.setString(2, fileRecord.fileName);
                                       preparedStatement.setString(3, fileRecord.fileType.baseName());
                                       preparedStatement.setInt(4, fileRecord.year);
                                       preparedStatement.setString(5, fileRecord.crs);
                                       if (fileRecord.created != null) {
                                           preparedStatement.setTimestamp(6, Timestamp.valueOf(fileRecord.created));
                                       } else {
                                           preparedStatement.setTimestamp(6, Timestamp.valueOf(LocalDateTime.now()));
                                       }
                                       LocalDateTime updated = fileRecord.updated != null ?
                                               fileRecord.updated : LocalDateTime.now();
                                       preparedStatement.setTimestamp(7, Timestamp.valueOf(updated));
                                       preparedStatement.setString(8, fileRecord.fileName);
                                       preparedStatement.setString(9, fileRecord.crs);
                                       preparedStatement.setTimestamp(10, Timestamp.valueOf(LocalDateTime.now()));
                                   });
    }

    public Record select(String fileName) throws DataAccessException {
        return jdbcTemplate.queryForObject("SELECT id, site_id, file_name, file_type, year, crs, created, updated FROM lpis_gsaa_file WHERE file_name = '" + fileName + "'",
                                           (resultSet, i) -> {
                                               Record record = new Record();
                                               record.id = resultSet.getInt(1);
                                               record.siteId = resultSet.getInt(2);
                                               record.fileName = resultSet.getString(3);
                                               record.fileType = DataType.fromBaseName(resultSet.getString(4));
                                               record.year = resultSet.getInt(5);
                                               record.crs = resultSet.getString(6);
                                               record.created = resultSet.getTimestamp(7).toLocalDateTime();
                                               record.updated = resultSet.getTimestamp(8).toLocalDateTime();
                                               return record;
                                           });
    }

    public class Record {
        private int id;
        private int siteId;
        private String fileName;
        private DataType fileType;
        private int year;
        private String crs;
        private LocalDateTime created;
        private LocalDateTime updated;

        public int getId() { return id; }
        public void setId(int id) { this.id = id; }

        public int getSiteId() { return siteId; }
        public void setSiteId(int siteId) { this.siteId = siteId; }

        public String getFileName() { return fileName; }
        public void setFileName(String fileName) { this.fileName = fileName; }

        public DataType getFileType() { return fileType; }
        public void setFileType(DataType fileType) { this.fileType = fileType; }

        public int getYear() { return year; }
        public void setYear(int year) { this.year = year; }

        public String getCrs() { return crs; }
        public void setCrs(String crs) { this.crs = crs; }

        public LocalDateTime getCreated() { return created; }
        public void setCreated(LocalDateTime created) { this.created = created; }

        public LocalDateTime getUpdated() { return updated; }
        public void setUpdated(LocalDateTime updated) { this.updated = updated; }
    }

}
