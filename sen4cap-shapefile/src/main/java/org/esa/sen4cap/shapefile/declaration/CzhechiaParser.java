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

package org.esa.sen4cap.shapefile.declaration;

import org.docx4j.openpackaging.packages.SpreadsheetMLPackage;
import org.docx4j.openpackaging.parts.SpreadsheetML.WorkbookPart;
import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.shapefile.entities.Declaration;
import org.esa.sen4cap.shapefile.parsers.Parser;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.operation.MathTransform;
import org.xlsx4j.org.apache.poi.ss.usermodel.DataFormatter;
import org.xlsx4j.sml.Cell;
import org.xlsx4j.sml.Row;
import org.xlsx4j.sml.SheetData;
import org.xlsx4j.sml.Worksheet;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author Cosmin Cara
 */
@DECLARATION(countryCode = "CZE")
public class CzhechiaParser extends Parser<Declaration> {

    public CzhechiaParser(Site site, Path shapeFilePath) {
        super(site, shapeFilePath);
    }

    @Override
    protected Set<String> mandatoryAttributes() { return null; }

    @Override
    protected boolean canParse(SimpleFeature sample) {
        return true;
    }

    @Override
    public long parse(Consumer<List<Declaration>> persister, long offset) throws Exception {
        String shapeFileName = this.shapeFilePath.getFileName().toString();
        int records = 0;
        try (InputStream is = Files.newInputStream(this.shapeFilePath);
             OutputStream os = Files.newOutputStream(getLogFile(shapeFileName), StandardOpenOption.CREATE)) {
            SpreadsheetMLPackage xlsxPackage = SpreadsheetMLPackage.load(is);
            WorkbookPart workbook = xlsxPackage.getWorkbookPart();
            Worksheet worksheet = workbook.getWorksheet(0).getContents();
            SheetData data = worksheet.getSheetData();
            int counter = 0;
            List<Declaration> batch = new ArrayList<>(BATCH_SIZE);
            DataFormatter formater = new DataFormatter();
            for (Row row : data.getRow()) {
                final List<Cell> cells = row.getC();
                if (!"A1".equals(cells.get(0).getR())) {
                    try {
                        List<Declaration> declarations = parseFeature(cells, formater);
                        batch.addAll(declarations);
                        counter += declarations.size();
                        if (counter >= BATCH_SIZE) {
                            records += counter;
                            try {
                                persister.accept(batch);
                            } catch (Exception inner) {
                                String message = inner.getMessage();
                                int idx = -1;
                                if ((idx = message.indexOf("Detail: ")) >= 0) {
                                    message = message.substring(idx, message.indexOf(".", idx)).replace("Detail: ", "");
                                }
                                os.write(message.getBytes());
                                os.write(10);
                                logger.fine("Error saving to database: " + message);
                            }
                            logger.info(String.format("[%s] Processed %s records", shapeFileName, records));
                            batch.clear();
                            counter = 0;
                        }
                    } catch (Exception ex) {
                        logger.warning("Parser error: " + ex.getMessage());
                    }
                }
            }
            if (batch.size() > 0) {
                records += batch.size();
                try {
                    persister.accept(batch);
                } catch (Exception ex) {
                    String message = ex.getMessage();
                    int idx = -1;
                    if ((idx = message.indexOf("Detail: ")) >= 0) {
                        message = message.substring(idx, message.indexOf(".", idx)).replace("Detail: ", "");
                    }
                    os.write(message.getBytes());
                    os.write(10);
                    logger.fine("Error saving to database: " + message);
                }
                logger.info(String.format("[%s] Processed %s records", shapeFileName, records));
                batch.clear();
            }
            os.flush();
        }
        return records;
    }

    private List<Declaration> parseFeature(List<Cell> cells, DataFormatter formatter) throws  Exception {
        List<Declaration> declarations = new ArrayList<>();
        String lpisId = formatter.formatCellValue(cells.get(1));
        String value;
        for (int i = 4; i < Math.min(24, cells.size()); i += 2) {
            value = formatter.formatCellValue(cells.get(i));
            if (value != null && !value.isEmpty() && !"0".equals(value)) {
                Declaration declaration = new Declaration();
                declaration.setLpisId(lpisId);
                declaration.setParcelId("plod" + String.valueOf((i - 4) / 2 + 1));
                declaration.setOriginalLandUseCode(value);
                declaration.setArea(Double.parseDouble(formatter.formatCellValue(cells.get(i + 1))));
                declaration.setDate(this.date);
                declaration.setSite(this.site);
                declaration.setSourceFile(this.fileName);
                declarations.add(declaration);
            } else {
                break;
            }
        }
        return declarations;
    }

    @Override
    protected Declaration parseFeature(SimpleFeature feature, MathTransform reprojection) throws Exception {
        // nothing to do
        return null;
    }
}
