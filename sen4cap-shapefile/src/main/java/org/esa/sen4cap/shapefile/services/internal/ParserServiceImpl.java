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
import org.esa.sen4cap.shapefile.db.GeneralizedLPISTable;
import org.esa.sen4cap.shapefile.parsers.GenericParser;
import org.esa.sen4cap.shapefile.services.ParserService;
import org.springframework.stereotype.Service;

import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

@Service("parserService")
public class ParserServiceImpl implements ParserService {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private GeneralizedLPISTable lastTable;

    public void setLastTable(GeneralizedLPISTable lastTable) { this.lastTable = lastTable; }

    @Override
    public long updateDatabase(Site site, DataType fileType, int year, Path shapeFile, String epsgCode, long offset) throws Exception {
        long total = 0;
        if (Files.isRegularFile(shapeFile)) {
            GenericParser parser = GenericParser.create(site, fileType, year, shapeFile, epsgCode);
            total = parser.parse(offset);
            setLastTable(parser.getTable());
        } else {
            total = Files.walk(shapeFile, 2, FileVisitOption.FOLLOW_LINKS)
                    .filter(f -> f.getFileName().toString().toLowerCase().endsWith(".shp"))
                    .mapToLong(f -> {
                        long results = 0;
                        try {
                            GenericParser parser = GenericParser.create(site, fileType, year, shapeFile, epsgCode);
                            results = parser.parse(offset);
                            setLastTable(parser.getTable());
                        } catch (Exception ex) {
                            Logger.getLogger(ParserService.class.getSimpleName()).warning(ex.getMessage());
                        }
                        return results;
                    }).sum();
        }
        executor.submit(() -> {
            try {
                Logger.getLogger(ParserService.class.getSimpleName()).info("Updating index structures");
                if (lastTable != null) {
                    lastTable.reindex();
                } else {
                    Logger.getLogger(ParserService.class.getSimpleName()).severe("No table to reindex!");
                }
                Logger.getLogger(ParserService.class.getSimpleName()).info("Index update completed");
            } catch (Exception e) {
                Logger.getLogger(ParserService.class.getSimpleName()).severe(e.getMessage());
            }
        });
        return total;
    }
}
