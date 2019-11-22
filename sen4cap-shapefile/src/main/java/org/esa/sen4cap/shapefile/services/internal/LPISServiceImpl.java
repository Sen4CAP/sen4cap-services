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

package org.esa.sen4cap.shapefile.services.internal;

import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.shapefile.db.DataType;
import org.esa.sen4cap.shapefile.db.Database;
import org.esa.sen4cap.shapefile.db.LPISTable;
import org.esa.sen4cap.shapefile.entities.Parcel;
import org.esa.sen4cap.shapefile.parsers.Parser;
import org.esa.sen4cap.shapefile.parsers.ParserFactory;
import org.esa.sen4cap.shapefile.services.LPISService;
import org.springframework.stereotype.Service;

import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * @author Cosmin Cara
 */
@Service("lpisService")
public class LPISServiceImpl implements LPISService {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    public long updateDatabase(Site site, Path shapeFile, long offset) throws Exception {
        LPISTable lpisTable = (LPISTable) Database.getTable(site, DataType.LPIS);
        long total = 0;
        if (Files.isRegularFile(shapeFile)) {
            Parser<Parcel> parser = (Parser<Parcel>) ParserFactory.create(DataType.LPIS, site, shapeFile);
            total = parser.parse(lpisTable::insertOrUpdate, offset);
        } else {
            total = Files.walk(shapeFile, 2, FileVisitOption.FOLLOW_LINKS)
                         .filter(f -> Files.isRegularFile(f) && f.getFileName().toString().toLowerCase().endsWith(".shp"))
                         .mapToLong(f -> {
                             long results = 0;
                             try {
                                 Parser<Parcel> parser = (Parser<Parcel>) ParserFactory.create(DataType.LPIS, site, f);
                                 results = parser.parse(lpisTable::insertOrUpdate, offset);
                                 parser = null;
                             } catch (Exception ex) {
                                 Logger.getLogger(LPISService.class.getSimpleName()).warning(ex.getMessage());
                             }
                             return results;
                         }).sum();
        }
        executor.submit(() -> {
            try {
                Logger.getLogger(LPISService.class.getSimpleName()).info("Updating index structures");
                Database.maintainTable(DataType.LPIS);
                Logger.getLogger(LPISService.class.getSimpleName()).info("Index update completed");
            } catch (Exception e) {
                Logger.getLogger(LPISService.class.getSimpleName()).severe(e.getMessage());
            }
        });
        return total;
    }

    @Override
    public List<Parcel> getParcels(Site site, String wktPolygon) throws Exception {
        List<Parcel> results;
        LPISTable lpisTable = (LPISTable) Database.getTable(site, DataType.LPIS);
        if (lpisTable != null) {
            results = lpisTable.select(site, wktPolygon);
        } else {
            results = new ArrayList<>();
        }
        return results;
    }
}
