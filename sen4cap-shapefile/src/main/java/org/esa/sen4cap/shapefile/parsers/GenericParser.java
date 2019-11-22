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

package org.esa.sen4cap.shapefile.parsers;

import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.shapefile.db.DataType;
import org.esa.sen4cap.shapefile.db.GeneralizedLPISTable;
import org.esa.sen4cap.shapefile.db.LPISFileTable;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import ro.cs.tao.ProgressListener;
import ro.cs.tao.messaging.ProgressNotifier;
import ro.cs.tao.messaging.Topic;
import ro.cs.tao.security.SystemPrincipal;
import ro.cs.tao.services.commons.StartupBase;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class GenericParser {
    private static final int BATCH_SIZE = 5000;
    private final Logger logger;
    private final ProgressListener progressListener;
    private final int year;
    private final DataType fileType;
    private final Site site;
    private final Path shapeFilePath;
    private final String reprojectionCode;
    private String topic;
    private GeneralizedLPISTable table;
    private final Map<String, Integer> conversionFlags;

    public static GenericParser create(Site site, DataType type, int year, Path shapeFilePath, String reprojectionCode) {
        return new GenericParser(site, type, year, shapeFilePath, reprojectionCode);
    }

    private GenericParser(Site site, DataType fileType, int year, Path shapeFilePath, String reprojectionCode) {
        this.logger = Logger.getLogger(getClass().getName());
        this.shapeFilePath = shapeFilePath;
        this.topic = this.shapeFilePath.getParent().getName(this.shapeFilePath.getParent().getNameCount() - 1).toString() +
                "/" + this.shapeFilePath.getFileName().toString() + " progress";
        this.site = site;
        this.fileType = fileType;
        this.year = year;
        this.reprojectionCode = reprojectionCode;
        this.conversionFlags = new HashMap<>();
        this.conversionFlags.put("NewID", 0);
        this.conversionFlags.put("newid", 0);
        this.conversionFlags.put("seqid", 0);
        this.conversionFlags.put("seq_id", 0);
        this.conversionFlags.put("CR_CAT", 0);
        this.conversionFlags.put("S1Pix", 0);
        this.conversionFlags.put("S2Pix", 0);
        this.progressListener = new ProgressNotifier(SystemPrincipal.instance(), getClass().getSimpleName(), Topic.create(this.topic));
    }

    public GeneralizedLPISTable getTable() { return table; }

    public long parse(long offset) throws Exception {
        String shapeFileName = this.shapeFilePath.getFileName().toString();
        markStart(topic);
        Path prjFile = this.shapeFilePath.resolveSibling(shapeFileName.replace(".shp", ".qpj"));
        if (!Files.exists(prjFile)) {
            prjFile = this.shapeFilePath.resolveSibling(shapeFileName.replace(".shp", ".prj"));
        }
        String wkt = new String(Files.readAllBytes(prjFile));
        CoordinateReferenceSystem sourceCrs = null;
        String crs = null;
        try {
            sourceCrs = CRS.parseWKT(wkt);
            if (sourceCrs != null) {
                if (sourceCrs.getIdentifiers() != null && sourceCrs.getIdentifiers().size() > 0) {
                    ReferenceIdentifier identifier = sourceCrs.getIdentifiers().stream().findFirst().get();
                    crs = identifier.getCodeSpace() + ":" + identifier.getCode();
                } else {
                    String lowerCase = sourceCrs.getName().toString().toLowerCase();
                    int idx = lowerCase.indexOf("utm_zone");
                    if (idx > 0) {
                        idx = lowerCase.lastIndexOf("_");
                        crs = "EPSG:32" + (lowerCase.endsWith("n") ? "6" : "7") + lowerCase.substring(idx + 1, idx + 3);
                    } else {
                        crs = wkt;
                    }
                }
            }
        } catch (FactoryException e) {
            logger.warning(String.format("Cannot determine the source CRS: %s", e.getMessage()));
        }
        MathTransform mathTransform = null;
        if (this.reprojectionCode != null && sourceCrs != null) {
            CoordinateReferenceSystem targetCrs = CRS.decode(this.reprojectionCode);
            try {
                mathTransform = CRS.findMathTransform(sourceCrs, targetCrs);
            } catch (Exception ex) {
                // fall back to lenient transform if Bursa Wolf parameters are not provided
                mathTransform = CRS.findMathTransform(sourceCrs, targetCrs, true);
            }
        }
        LPISFileTable fileTable = LPISFileTable.getInstance();
        if (fileTable.insertOrUpdate(shapeFileName, this.site, this.fileType, this.year, this.reprojectionCode != null ? this.reprojectionCode : crs) == 0) {
            throw new Exception(String.format("Cannot insert/update the file record for %s", shapeFileName));
        }
        LPISFileTable.Record fileRecord = fileTable.select(shapeFileName);
        int fileId = fileRecord.getId();
        ShapefileDataStore dataStore = new ShapefileDataStore(this.shapeFilePath.toUri().toURL());
        dataStore.setMemoryMapped(true);
        String typeName = dataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore.getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE;
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        long total = collection.size();
        int counter = 0;
        long records = 0;
        List<GeneralizedLPISTable.Record> batch = new ArrayList<>(BATCH_SIZE);
        final Set<String> mandatoryColumnNames = GeneralizedLPISTable.mandatoryColumnNames();
        final Set<String> optionalColumnNames = GeneralizedLPISTable.optionalColumnNames();
        String idColumn = null;
        try (FeatureIterator<SimpleFeature> tester = collection.features()) {
            if (tester.hasNext()) {
                SimpleFeature next = tester.next();
                if (next.getAttributeCount() == 0) {
                    markEnd();
                    throw new IOException(String.format("This parser is not intended for '%s' (no attributes found)",
                                                        shapeFileName));
                }
                if (!canParse(next)) {
                    markEnd();
                    throw new IOException(String.format("This parser is not intended for '%s' (expected attributes not found; maybe wrong shapefile type?)",
                                                        shapeFileName));
                }
                List<String> columns = next.getProperties().stream()
                                                           .map(p -> p.getName().getLocalPart())
                                                           .collect(Collectors.toList());
                columns.removeAll(mandatoryColumnNames);
                columns.removeAll(optionalColumnNames);
                table = GeneralizedLPISTable.create(this.site, this.fileType, columns);
                idColumn = idColName(next);
            }
        }
        if (table == null) {
            throw new Exception("Cannot parse file");
        }
        logger.info(String.format("%s contains %s records", shapeFileName, collection.size()));
        try (FeatureIterator<SimpleFeature> features = collection.features();
             OutputStream os = Files.newOutputStream(getLogFile(shapeFileName), StandardOpenOption.CREATE)) {
            while (features.hasNext()) {
                if (offset > 0 && ++counter <= offset) {
                    features.next();
                    continue;
                } else {
                    if (offset > 0) {
                        records = offset;
                        offset = -1;
                        counter = 0;
                    }
                }
                try {
                    SimpleFeature feature = features.next();
                    GeneralizedLPISTable.Record parcel = parseFeature(table, idColumn, mandatoryColumnNames, optionalColumnNames, feature, mathTransform);
                    if (parcel != null) {
                        parcel.setFileId(fileId);
                        batch.add(parcel);
                    }
                    if (++counter == BATCH_SIZE) {
                        records += BATCH_SIZE;
                        try {
                            table.insertOrUpdate(batch);
                        } catch (Exception inner) {
                            String message = inner.getMessage();
                            int idx;
                            if ((idx = message.indexOf("Detail: ")) >= 0) {
                                message = message.substring(idx, message.indexOf(".", idx)).replace("Detail: ", "");
                            }
                            os.write(message.getBytes());
                            os.write(10);
                            logger.fine("Error saving to database: " + message);
                        }
                        batch.clear();
                        counter = 0;
                        progressListener.notifyProgress((double) records / (double) total);
                    }
                } catch (Exception ex) {
                    logger.warning("Parser error: " + ex.getMessage());
                }
            }
            if (batch.size() > 0) {
                records += batch.size();
                try {
                    table.insertOrUpdate(batch);
                } catch (Exception ex) {
                    String message = ex.getMessage();
                    int idx;
                    if ((idx = message.indexOf("Detail: ")) >= 0) {
                        message = message.substring(idx, message.indexOf(".", idx)).replace("Detail: ", "");
                    }
                    os.write(message.getBytes());
                    os.write(10);
                    logger.fine("Error saving to database: " + message);
                }
                batch.clear();
                progressListener.notifyProgress((double) records / (double) total);
            }
            os.flush();
        }
        markEnd();
        return records;
    }

    private boolean canParse(SimpleFeature sample) {
        return GeneralizedLPISTable.mandatoryColumnNames().stream()
                .allMatch(n -> sample.getAttribute(n) != null || sample.getAttribute(n.toLowerCase()) != null);
    }

    private String idColName(SimpleFeature sample) {
        if (sample.getAttribute("NewID") != null) {
            return "NewID";
        } else if (sample.getAttribute("newid") != null) {
            return "newid";
        } else if (sample.getAttribute("seq_id") != null) {
            return "seq_id";
        } else if (sample.getAttribute("seqid") != null) {
            return "seqid";
        } else {
            return null;
        }
    }

    protected GeneralizedLPISTable.Record parseFeature(GeneralizedLPISTable table,
                                                       String idColumn,
                                                       Set<String> mandatoryFields,
                                                       Set<String> optionalFields,
                                                       SimpleFeature feature, MathTransform reprojection) throws Exception {
        GeneralizedLPISTable.Record declaration = table.newRow();
        Geometry geometry = (Geometry) feature.getDefaultGeometry();
        if (geometry != null && reprojection != null) {
            geometry = JTS.transform((Geometry) feature.getDefaultGeometry(), reprojection);
        }
        declaration.setGeomValid(geometry != null);
        declaration.setFootprint(geometry);
        Object attribute;
        Integer flag;
        if (idColumn != null) {
            attribute = feature.getAttribute(idColumn);
            try {
                if ((flag = this.conversionFlags.get(idColumn)) == 0) {
                    declaration.setId(Long.parseLong(attribute.toString()));
                } else if (flag == 1) {
                    declaration.setId((long) Double.parseDouble(attribute.toString()));
                }
            } catch (NumberFormatException nfe) {
                try {
                    declaration.setId((long) Double.parseDouble(attribute.toString()));
                    logger.warning("Field [" + idColumn + "] was found as double, expected long. Cast will be performed");
                    this.conversionFlags.put(idColumn, 1);
                } catch (NumberFormatException nfe2) {
                    logger.warning("Field [" + idColumn + "] was found neither as double nor as long. Field will not be read");
                    this.conversionFlags.put(idColumn, 2);
                }
            }
        }
        if ((attribute = feature.getAttribute("CR_CO_GSAA")) != null) {
            declaration.setCR_CO_GSAA(attribute.toString());
        }
        if ((attribute = feature.getAttribute("CR_NA_GSAA")) != null) {
            declaration.setCR_NA_GSAA(attribute.toString());
        }
        if ((attribute = feature.getAttribute("CR_CO_L4A")) != null) {
            declaration.setCR_CO_L4A(attribute.toString());
        }
        if ((attribute = feature.getAttribute("CR_NA_L4A")) != null) {
            declaration.setCR_NA_L4A(attribute.toString());
        }
        if ((attribute = feature.getAttribute("CR_CO_DIV")) != null) {
            declaration.setCR_CO_DIV(attribute.toString());
        }
        if ((attribute = feature.getAttribute("CR_NA_DIV")) != null) {
            declaration.setCR_NA_DIV(attribute.toString());
        }
        try {
            attribute = feature.getAttribute("CR_CAT");
            if ((flag = this.conversionFlags.get("CR_CAT")) == 0) {
                declaration.setCR_CAT(Integer.parseInt(attribute.toString()));
            } else if (flag == 1) {
                declaration.setCR_CAT((int) Double.parseDouble(attribute.toString()));
            }
        } catch (NumberFormatException nfe) {
            try {
                declaration.setCR_CAT((int) Double.parseDouble(feature.getAttribute("CR_CAT").toString()));
                logger.warning("Field [CR_CAT] was found as double, expected int. Cast will be performed");
                this.conversionFlags.put("CR_CAT", 1);
            } catch (NumberFormatException nfe2) {
                logger.warning("Field [CR_CAT] was found neither as double nor as int. Field will not be read");
                this.conversionFlags.put("CR_CAT", 2);
            }
        }
        if ((attribute = feature.getAttribute("S1Pix")) != null) {
            try {
                if ((flag = this.conversionFlags.get("S1Pix")) == 0) {
                    declaration.setS1Pix(Integer.parseInt(attribute.toString()));
                } else if (flag == 1) {
                    declaration.setS1Pix((int) Double.parseDouble(attribute.toString()));
                }
            } catch (NumberFormatException nfe) {
                try {
                    declaration.setS1Pix((int) Double.parseDouble(attribute.toString()));
                    logger.warning("Field [S1Pix] was found as double, expected int. Cast will be performed");
                    this.conversionFlags.put("S1Pix", 1);
                } catch (NumberFormatException nfe2) {
                    logger.warning("Field [S1Pix] was found neither as double nor as int. Field will not be read");
                    this.conversionFlags.put("S1Pix", 2);
                }
            }
        }
        if ((attribute = feature.getAttribute("S2Pix")) != null) {
            try {
                if ((flag = this.conversionFlags.get("S2Pix")) == 0) {
                    declaration.setS2Pix(Integer.parseInt(attribute.toString()));
                } else if (flag == 1) {
                    declaration.setS2Pix((int) Double.parseDouble(attribute.toString()));
                }
            } catch (NumberFormatException nfe) {
                try {
                    declaration.setS2Pix((int) Double.parseDouble(attribute.toString()));
                    logger.warning("Field [S2Pix] was found as double, expected int. Cast will be performed");
                    this.conversionFlags.put("S2Pix", 1);
                } catch (NumberFormatException nfe2) {
                    logger.warning("Field [S2Pix] was found neither as double nor as int. Field will not be read");
                    this.conversionFlags.put("S2Pix", 2);
                }
            }
        }
        if ((attribute = feature.getAttribute("Area")) != null) {
            declaration.setArea(Double.parseDouble(attribute.toString()));
        } else if ((attribute = feature.getAttribute("Shape_Area")) != null) {
            declaration.setArea(Double.parseDouble(attribute.toString()));
        }
        if ((attribute = feature.getAttribute("ShapeIndex")) != null) {
            declaration.setShapeIndex(Double.parseDouble(attribute.toString()));
        } else if ((attribute = feature.getAttribute("Shape_Index")) != null) {
            declaration.setShapeIndex(Double.parseDouble(attribute.toString()));
        }
        declaration.setOverlap(null);
        feature.getProperties().forEach(p -> {
            String  propName = p.getName().toString();
            if (!mandatoryFields.contains(propName) && !optionalFields.contains(propName) &&
                    (idColumn != null && !idColumn.equals(propName))) {
                Object attr = feature.getAttribute(propName);
                if (attr != null) {
                    if (!(attr instanceof Geometry)) {
                        declaration.setColumn(propName, String.valueOf(attr));
                    } else {
                        declaration.setColumn(propName, ((Geometry) attr).toText());
                    }
                } else {
                    declaration.setColumn(propName, null);
                }
            }
        });
        return declaration;
    }

    private Path getLogFile(String shapeFileName) throws IOException {
        Path logPath = StartupBase.homeDirectory().resolve("log");
        Files.createDirectories(logPath);
        return logPath.resolve(shapeFileName + ".error.log");
    }

    private void markStart(String subActivity) { this.progressListener.started(subActivity); }

    private void markEnd() {
        this.progressListener.ended();
    }
}
