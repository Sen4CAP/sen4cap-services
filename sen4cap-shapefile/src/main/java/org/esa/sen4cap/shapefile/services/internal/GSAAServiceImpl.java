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
import org.esa.sen4cap.shapefile.db.DeclarationTable;
import org.esa.sen4cap.shapefile.entities.Declaration;
import org.esa.sen4cap.shapefile.parsers.Parser;
import org.esa.sen4cap.shapefile.parsers.ParserFactory;
import org.esa.sen4cap.shapefile.services.GSAAService;
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
@Service("gsaaService")
public class GSAAServiceImpl implements GSAAService {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    public long updateDatabase(Site site, Path shapeFile, long offset) throws Exception {
        DeclarationTable table = (DeclarationTable) Database.getTable(site, DataType.DECLARATION);
        long total = 0;
        if (Files.isRegularFile(shapeFile)) {
            Parser<Declaration> parser = (Parser<Declaration>) ParserFactory.create(DataType.DECLARATION, site, shapeFile);
            total = parser.parse(table::insertOrUpdate, offset);
        } else {
            total = Files.walk(shapeFile, 2, FileVisitOption.FOLLOW_LINKS)
                    .filter(f -> f.getFileName().toString().toLowerCase().endsWith(".shp"))
                    .mapToLong(f -> {
                        long results = 0;
                        try {
                            Parser<Declaration> parser = (Parser<Declaration>) ParserFactory.create(DataType.DECLARATION, site, f);
                            results = parser.parse(table::insertOrUpdate, offset);
                        } catch (Exception ex) {
                            Logger.getLogger(GSAAService.class.getSimpleName()).warning(ex.getMessage());
                        }
                        return results;
                    }).sum();
        }
        executor.submit(() -> {
            try {
                Logger.getLogger(GSAAService.class.getSimpleName()).info("Updating index structures");
                Database.maintainTable(DataType.DECLARATION);
                Logger.getLogger(GSAAService.class.getSimpleName()).info("Index update completed");
            } catch (Exception e) {
                Logger.getLogger(GSAAService.class.getSimpleName()).severe(e.getMessage());
            }
        });
        return total;
    }

    @Override
    public List<Declaration> getDeclarations(Site site, String wktPolygon) throws Exception {
        List<Declaration> results;
        DeclarationTable table = (DeclarationTable) Database.getTable(site, DataType.DECLARATION);
        if (table != null) {
            results = table.select(site, wktPolygon);
        } else {
            results = new ArrayList<>();
        }
        return results;
    }
}
