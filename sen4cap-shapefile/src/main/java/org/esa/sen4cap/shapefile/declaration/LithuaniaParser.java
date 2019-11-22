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

import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.shapefile.entities.Declaration;
import org.esa.sen4cap.shapefile.parsers.Parser;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.operation.MathTransform;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Cosmin Cara
 */
@DECLARATION(countryCode = "LTU")
public class LithuaniaParser extends Parser<Declaration> {
    public LithuaniaParser(Site site, Path shapeFilePath) {
        super(site, shapeFilePath);
    }

    @Override
    protected Set<String> mandatoryAttributes() {
        return new HashSet<String>() {{
            add("KZS");
            add("DKL_NR");
            add("PLOTAS");
            add("PSL_KODAS");
        }};
    }

    @Override
    protected Declaration parseFeature(SimpleFeature feature, MathTransform reprojection) throws Exception {
        Declaration declaration = new Declaration();
        declaration.setFootprint(JTS.transform((Geometry) feature.getDefaultGeometry(), reprojection));
        declaration.setLpisId(feature.getAttribute("KZS").toString());
        declaration.setParcelId(feature.getAttribute("DKL_NR").toString());
        Object area = feature.getAttribute("PLOTAS");
        if (area == null) {
            area = feature.getAttribute("plotas");
        }
        if (area != null) {
            declaration.setArea(Double.parseDouble(area.toString()));
        }
        declaration.setOriginalLandUseCode(feature.getAttribute("PSL_KODAS").toString());
        return declaration;
    }
}
