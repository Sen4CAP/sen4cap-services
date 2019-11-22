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
import org.esa.sen4cap.shapefile.entities.VectorRecord;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Base class for LPIS and Declarations parsers.
 *
 * @author Cosmin Cara
 */
public abstract class Parser<RecordType extends VectorRecord> {
    protected static final int BATCH_SIZE = 5000;
    protected final Path shapeFilePath;
    protected final Site site;
    protected final LocalDate date;
    protected final String fileName;
    protected Logger logger;
    private String topic;
    private Set<String> mandatoryAttributeNames;
    private ProgressListener progressListener;


    protected Parser(Site site, Path shapeFilePath) {
        this.logger = Logger.getLogger(getClass().getSimpleName());
        this.site = site;
        this.shapeFilePath = shapeFilePath;
        this.date = LocalDate.now();
        this.fileName = shapeFilePath.getFileName().toString();
        this.topic = this.shapeFilePath.getParent().getName(this.shapeFilePath.getParent().getNameCount() - 1).toString() +
                "/" + this.fileName + " progress";
        this.mandatoryAttributeNames = mandatoryAttributes();
        this.progressListener = new ProgressNotifier(SystemPrincipal.instance(), getClass().getSimpleName(), Topic.create(this.topic));
    }

    public long parse(Consumer<List<RecordType>> persister, long offset) throws Exception {
        String shapeFileName = this.shapeFilePath.getFileName().toString();
        markStart(topic);
        Path prjFile = this.shapeFilePath.resolveSibling(shapeFileName.replace(".shp", ".qpj"));
        if (!Files.exists(prjFile)) {
            prjFile = this.shapeFilePath.resolveSibling(shapeFileName.replace(".shp", ".prj"));
        }
        String wkt = new String(Files.readAllBytes(prjFile));
        CoordinateReferenceSystem sourceCrs = null;
        try {
            sourceCrs = CRS.parseWKT(wkt);
        } catch (FactoryException e) {
            logger.warning(String.format("Cannot determine the source CRS: %s", e.getMessage()));
        }
        CoordinateReferenceSystem targetCrs = CRS.decode("EPSG:4326");
        MathTransform mathTransform;
        try {
            mathTransform = CRS.findMathTransform(sourceCrs, targetCrs);
        } catch (Exception ex) {
            // fall back to lenient transform if Bursa Wolf parameters are not provided
            mathTransform = CRS.findMathTransform(sourceCrs, targetCrs, true);
        }
        ShapefileDataStore dataStore = new ShapefileDataStore(this.shapeFilePath.toUri().toURL());
        dataStore.setMemoryMapped(true);
        String typeName = dataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore.getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE;
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        long total = collection.size();
        int counter = 0;
        long records = 0;
        List<RecordType> batch = new ArrayList<>(BATCH_SIZE);
        if (persister != null) {
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
                }
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
                        RecordType parcel = parseFeature(feature, mathTransform);
                        if (parcel != null) {
                            parcel.setSite(this.site);
                            parcel.setDate(this.date);
                            parcel.setSourceFile(this.fileName);
                            feature.getProperties().forEach(p -> {
                                String  propName = p.getName().toString();
                                if (!this.mandatoryAttributeNames.contains(propName)) {
                                    Object attribute = feature.getAttribute(propName);
                                    if (!(attribute instanceof Geometry)) {
                                        parcel.addAttribue(propName, String.valueOf(attribute));
                                    }
                                }
                            });
                            batch.add(parcel);
                        }
                        if (++counter == BATCH_SIZE) {
                            records += BATCH_SIZE;
                            try {
                                persister.accept(batch);
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
                        persister.accept(batch);
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
        }
        markEnd();
        return records;
    }

    protected boolean canParse(SimpleFeature sample) {
        return this.mandatoryAttributeNames.stream()
                .allMatch(n -> sample.getAttribute(n) != null || sample.getAttribute(n.toLowerCase()) != null);
    }

    protected abstract RecordType parseFeature(SimpleFeature feature, MathTransform reprojection) throws Exception;

    protected abstract Set<String> mandatoryAttributes();

    protected Path getLogFile(String shapeFileName) throws IOException {
        Path logPath = StartupBase.homeDirectory().resolve("log");
        Files.createDirectories(logPath);
        return logPath.resolve(shapeFileName + ".error.log");
    }

    private void markStart(String subActivity) { this.progressListener.started(subActivity); }

    private void markEnd() {
        this.progressListener.ended();
    }

}
