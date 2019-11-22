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
import org.locationtech.jts.geom.Geometry;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.logging.Logger;

public class GeneralizedLPISTable {
    private static final Map<String, String> mandatoryColumns;
    private static final Map<String, String> optionalColumns;
    private static final Map<String, String> internalColumns;
    private final Map<String, String> additionalColumns;
    private final Map<String, String> rowColumnsTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final String tableName;
    private final String upsertQuery;
    private final String upsertQueryWithoutId;
    private final StringBuilder statementBuilder;
    private final Logger logger;

    static {
        mandatoryColumns = new LinkedHashMap<>();
        mandatoryColumns.put("CR_CO_GSAA", "text");
        mandatoryColumns.put("CR_NA_GSAA", "text");
        mandatoryColumns.put("CR_CO_L4A", "text");
        mandatoryColumns.put("CR_NA_L4A", "text");
        mandatoryColumns.put("CR_CO_DIV", "text");
        mandatoryColumns.put("CR_NA_DIV", "text");
        mandatoryColumns.put("CR_CAT", "smallint not null");
        optionalColumns = new LinkedHashMap<>();
        optionalColumns.put("S1Pix", "integer");
        optionalColumns.put("S2Pix", "integer");
        optionalColumns.put("Area", "double precision");
        optionalColumns.put("ShapeIndex", "double precision");
        optionalColumns.put("Overlap", "bigint");
        optionalColumns.put("GeomValid", "boolean");
        internalColumns = new LinkedHashMap<>();
        internalColumns.put("item_id", "bigserial not null");
        internalColumns.put("file_id", "integer not null");
        internalColumns.put("footprint", "geography");
    }

    public static GeneralizedLPISTable create(Site site, DataType tableType, List<String> additionalColumns) {
        return new GeneralizedLPISTable(site, tableType, additionalColumns);
    }

    public static Set<String> mandatoryColumnNames() {
        return new HashSet<>(mandatoryColumns.keySet());
    }

    public static Set<String> optionalColumnNames() {
        return new HashSet<>(optionalColumns.keySet());
    }

    private GeneralizedLPISTable(Site site, DataType tableType, List<String> additionalColumns) {
        this.tableName = String.format("%s_%s",
                                       tableType.baseName(),
                                       site.getShortName().toLowerCase().replace(" ", "_"));
        this.additionalColumns = new LinkedHashMap<>();
        this.rowColumnsTemplate = new LinkedHashMap<>();
        if (additionalColumns != null) {
            for (String colName : additionalColumns) {
                this.additionalColumns.put(colName, "text");
                this.rowColumnsTemplate.put(colName, null);
            }
        }
        this.jdbcTemplate = new JdbcTemplate(Config.getPersistenceManager().getDataSource());
        this.statementBuilder = new StringBuilder();
        StringBuilder valuesBuilder = new StringBuilder();
        StringBuilder updateBuilder = new StringBuilder();
        this.statementBuilder.append("INSERT INTO ").append(this.tableName).append(" (");
        valuesBuilder.append(" VALUES(");
        updateBuilder.append(" ON CONFLICT (item_id, file_id) DO UPDATE SET ");
        for (String colName : internalColumns.keySet()) {
            this.statementBuilder.append(colName).append(",");
            if ("footprint".equals(colName)) {
                valuesBuilder.append("st_geogfromtext(?),");
                updateBuilder.append(colName).append("=st_geogfromtext(?),");
            } else {
                valuesBuilder.append("?,");
                //updateBuilder.append(colName).append(",");
            }
        }
        for (String colName : mandatoryColumns.keySet()) {
            this.statementBuilder.append(colName).append(",");
            valuesBuilder.append("?,");
            updateBuilder.append(colName).append("=?,");
        }
        for (String colName : optionalColumns.keySet()) {
            this.statementBuilder.append(colName).append(",");
            valuesBuilder.append("?,");
            updateBuilder.append(colName).append("=?,");
        }
        for (String colName : this.additionalColumns.keySet()) {
            this.statementBuilder.append(colName).append(",");
            valuesBuilder.append("?,");
            updateBuilder.append(colName).append("=?,");
        }
        this.statementBuilder.setLength(this.statementBuilder.length() - 1);
        this.statementBuilder.append(")");
        valuesBuilder.setLength(valuesBuilder.length() - 1);
        valuesBuilder.append(")");
        updateBuilder.setLength(updateBuilder.length() - 1);
        this.upsertQuery = this.statementBuilder.toString() + valuesBuilder.toString() + updateBuilder.toString();
        this.upsertQueryWithoutId = this.upsertQuery.replace(this.tableName + " (item_id,", this.tableName  +" (").replace("VALUES(?,", "VALUES(");
        valuesBuilder.setLength(0);
        updateBuilder.setLength(0);
        this.statementBuilder.setLength(0);
        this.logger = Logger.getLogger(GeneralizedLPISTable.class.getName());
        create();
    }

    public void reindex() {
        DataSource dataSource = Config.getPersistenceManager().getDataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String[] statements = new String[] {
                String.format("VACUUM %s", this.tableName),
                String.format("REINDEX TABLE %s", this.tableName)
        };
        for (String statement : statements) {
            try {
                logger.finest(statement);
                jdbcTemplate.execute(statement);
            } catch (DataAccessException ex) {
                logger.severe(String.format("SQL Statement failed ('%s'): %s", statement, ex.getMessage()));
                break;
            }
        }
    }

    public Record newRow() { return new Record(this.rowColumnsTemplate); }

    public int[] insertOrUpdate(List<Record> rows) throws DataAccessException {
        final boolean hasId = rows.get(0).getId() != null;
        String statement = hasId ? this.upsertQuery : this.upsertQueryWithoutId;
        return jdbcTemplate.batchUpdate(statement,
                                        new BatchPreparedStatementSetter() {
                                               @Override
                                               public void setValues(PreparedStatement preparedStatement, int row) throws SQLException {
                                                   Record record = rows.get(row);
                                                   int idx = 1;
                                                   if (hasId) {
                                                       preparedStatement.setLong(idx++, record.id);
                                                   }
                                                   preparedStatement.setInt(idx++, record.fileId);
                                                   Geometry footprint = record.getFootprint();
                                                   String wkt = footprint == null ? "POLYGON EMPTY" : footprint.toText();
                                                   preparedStatement.setString(idx++, wkt);
                                                   preparedStatement.setString(idx++, record.CR_CO_GSAA);
                                                   preparedStatement.setString(idx++, record.CR_NA_GSAA);
                                                   preparedStatement.setString(idx++, record.CR_CO_L4A);
                                                   preparedStatement.setString(idx++, record.CR_NA_L4A);
                                                   preparedStatement.setString(idx++, record.CR_CO_DIV);
                                                   preparedStatement.setString(idx++, record.CR_NA_DIV);
                                                   preparedStatement.setInt(idx++, record.CR_CAT);
                                                   if (record.S1Pix != null) {
                                                       preparedStatement.setInt(idx++, record.S1Pix);
                                                   } else {
                                                       preparedStatement.setNull(idx++, Types.INTEGER);
                                                   }
                                                   if (record.S2Pix != null) {
                                                       preparedStatement.setInt(idx++, record.S2Pix);
                                                   } else {
                                                       preparedStatement.setNull(idx++, Types.INTEGER);
                                                   }
                                                   if (record.Area != null) {
                                                       preparedStatement.setDouble(idx++, record.Area);
                                                   } else {
                                                       preparedStatement.setNull(idx++, Types.DOUBLE);
                                                   }
                                                   if (record.ShapeIndex != null) {
                                                       preparedStatement.setDouble(idx++, record.ShapeIndex);
                                                   } else {
                                                       preparedStatement.setNull(idx++, Types.DOUBLE);
                                                   }
                                                   if (record.Overlap != null) {
                                                       preparedStatement.setLong(idx++, record.Overlap);
                                                   } else {
                                                       preparedStatement.setNull(idx++, Types.BIGINT);
                                                   }
                                                   preparedStatement.setBoolean(idx++, record.GeomValid);
                                                   for (Map.Entry<String, String> col : record.otherColumns.entrySet()) {
                                                       preparedStatement.setString(idx++, col.getValue());
                                                   }
                                                   //preparedStatement.setInt(idx++, record.fileId);
                                                   preparedStatement.setString(idx++, wkt);
                                                   preparedStatement.setString(idx++, record.CR_CO_GSAA);
                                                   preparedStatement.setString(idx++, record.CR_NA_GSAA);
                                                   preparedStatement.setString(idx++, record.CR_CO_L4A);
                                                   preparedStatement.setString(idx++, record.CR_NA_L4A);
                                                   preparedStatement.setString(idx++, record.CR_CO_DIV);
                                                   preparedStatement.setString(idx++, record.CR_NA_DIV);
                                                   preparedStatement.setInt(idx++, record.CR_CAT);
                                                   if (record.S1Pix != null) {
                                                       preparedStatement.setInt(idx++, record.S1Pix);
                                                   } else {
                                                       preparedStatement.setNull(idx++, Types.INTEGER);
                                                   }
                                                   if (record.S2Pix != null) {
                                                       preparedStatement.setInt(idx++, record.S2Pix);
                                                   } else {
                                                       preparedStatement.setNull(idx++, Types.INTEGER);
                                                   }
                                                   if (record.Area != null) {
                                                       preparedStatement.setDouble(idx++, record.Area);
                                                   } else {
                                                       preparedStatement.setNull(idx++, Types.DOUBLE);
                                                   }
                                                   if (record.ShapeIndex != null) {
                                                       preparedStatement.setDouble(idx++, record.ShapeIndex);
                                                   } else {
                                                       preparedStatement.setNull(idx++, Types.DOUBLE);
                                                   }
                                                   if (record.Overlap != null) {
                                                       preparedStatement.setLong(idx++, record.Overlap);
                                                   } else {
                                                       preparedStatement.setNull(idx++, Types.BIGINT);
                                                   }
                                                   preparedStatement.setBoolean(idx++, record.GeomValid);
                                                   for (Map.Entry<String, String> col : record.otherColumns.entrySet()) {
                                                       preparedStatement.setString(idx++, col.getValue());
                                                   }
                                               }

                                               @Override
                                               public int getBatchSize() {
                                                   return rows.size();
                                               }
                                           });
    }

    private void create() {
        SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT table_name FROM information_schema.tables WHERE table_name = ?",
                                                       this.tableName);
        if (!rowSet.next()) {
            logger.info(String.format("Table %s was not found and it will be created", this.tableName));
            List<String> statements = new LinkedList<>();
            statementBuilder.append("CREATE TABLE ").append(this.tableName).append(" (");
            for (Map.Entry<String, String> entry : internalColumns.entrySet()) {
                statementBuilder.append(entry.getKey()).append(" ").append(entry.getValue()).append(", ");
            }
            for (Map.Entry<String, String> entry : mandatoryColumns.entrySet()) {
                statementBuilder.append(entry.getKey()).append(" ").append(entry.getValue()).append(", ");
            }
            for (Map.Entry<String, String> entry : optionalColumns.entrySet()) {
                statementBuilder.append(entry.getKey()).append(" ").append(entry.getValue()).append(", ");
            }
            for (Map.Entry<String, String> entry : this.additionalColumns.entrySet()) {
                statementBuilder.append(entry.getKey()).append(" ").append(entry.getValue()).append(", ");
            }
            statementBuilder.append("CONSTRAINT pk_").append(this.tableName)
                    .append(" PRIMARY KEY (item_id, file_id)) WITH (OIDS=FALSE);");
            statements.add(statementBuilder.toString());
            statementBuilder.setLength(0);
            statementBuilder.append("ALTER TABLE ").append(this.tableName).append(" OWNER TO postgres;");
            statements.add(statementBuilder.toString());
            statementBuilder.setLength(0);
            statementBuilder.append("CREATE INDEX idx_").append(this.tableName)
                    .append(" ON ").append(this.tableName).append(" USING GIST(footprint);");
            statements.add(statementBuilder.toString());
            statementBuilder.setLength(0);
            statementBuilder.append("CREATE SEQUENCE ").append(this.tableName).append("_sequence INCREMENT BY 1 MINVALUE 1 START WITH 1 NO CYCLE;");
            statements.add(statementBuilder.toString());
            statementBuilder.setLength(0);
            statementBuilder.append("ALTER TABLE ").append(this.tableName)
                    .append(" ALTER COLUMN item_id SET DEFAULT nextval('").append(this.tableName).append("_sequence');");
            statements.add(statementBuilder.toString());
            statementBuilder.setLength(0);
            /*statementBuilder.append("ALTER SEQUENCE ").append(this.tableName)
                    .append("_sequence OWNED BY ").append(this.tableName).append(".item_id;");
            statements.add(statementBuilder.toString());
            statementBuilder.setLength(0);*/
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
            logger.warning(String.format("Table %s already exists", this.tableName));
        }
    }

    public class Record {
        private Long id;
        private int fileId;
        private String CR_CO_GSAA;
        private String CR_NA_GSAA;
        private String CR_CO_L4A;
        private String CR_NA_L4A;
        private String CR_CO_DIV;
        private String CR_NA_DIV;
        private int CR_CAT;
        private Integer S1Pix;
        private Integer S2Pix;
        private Double Area;
        private Double ShapeIndex;
        private Long Overlap;
        private boolean GeomValid;
        private Geometry footprint;
        private Map<String, String> otherColumns;

        Record(Map<String, String> otherColumns) {
            this.otherColumns = new LinkedHashMap<>(otherColumns);
        }

        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }

        public int getFileId() { return fileId; }
        public void setFileId(int fileId) { this.fileId = fileId; }

        public String getCR_CO_GSAA() { return CR_CO_GSAA; }
        public void setCR_CO_GSAA(String CR_CO_GSAA) { this.CR_CO_GSAA = CR_CO_GSAA; }

        public String getCR_NA_GSAA() { return CR_NA_GSAA; }
        public void setCR_NA_GSAA(String CR_NA_GSAA) { this.CR_NA_GSAA = CR_NA_GSAA; }

        public String getCR_CO_L4A() { return CR_CO_L4A; }
        public void setCR_CO_L4A(String CR_CO_L4A) { this.CR_CO_L4A = CR_CO_L4A; }

        public String getCR_NA_L4A() { return CR_NA_L4A; }
        public void setCR_NA_L4A(String CR_NA_L4A) { this.CR_NA_L4A = CR_NA_L4A; }

        public String getCR_CO_DIV() { return CR_CO_DIV; }
        public void setCR_CO_DIV(String CR_CO_DIV) { this.CR_CO_DIV = CR_CO_DIV; }

        public String getCR_NA_DIV() { return CR_NA_DIV; }
        public void setCR_NA_DIV(String CR_NA_DIV) { this.CR_NA_DIV = CR_NA_DIV; }

        public int getCR_CAT() { return CR_CAT; }
        public void setCR_CAT(int CR_CAT) { this.CR_CAT = CR_CAT; }

        public Integer getS1Pix() { return S1Pix; }
        public void setS1Pix(Integer s1Pix) { S1Pix = s1Pix; }

        public Integer getS2Pix() { return S2Pix; }
        public void setS2Pix(Integer s2Pix) { S2Pix = s2Pix; }

        public Double getArea() { return Area; }
        public void setArea(Double area) { Area = area; }

        public Double getShapeIndex() { return ShapeIndex; }
        public void setShapeIndex(Double shapeIndex) { ShapeIndex = shapeIndex; }

        public Long getOverlap() { return Overlap; }
        public void setOverlap(Long overlap) { Overlap = overlap; }

        public boolean isGeomValid() { return GeomValid; }
        public void setGeomValid(boolean geomValid) { GeomValid = geomValid; }

        public Geometry getFootprint() { return footprint; }
        public void setFootprint(Geometry footprint) { this.footprint = footprint; }

        public String getColumn(String columnName) {
            if (!this.otherColumns.containsKey(columnName)) {
                throw new IllegalArgumentException(String.format("columnName=%s", columnName));
            }
            return this.otherColumns.get(columnName);
        }

        public void setColumn(String columnName, String value) {
            if (!this.otherColumns.containsKey(columnName)) {
                throw new IllegalArgumentException(String.format("columnName=%s", columnName));
            }
            this.otherColumns.put(columnName, value);
        }
    }

}
