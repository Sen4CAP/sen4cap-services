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
@DECLARATION(countryCode = "ROU")
public class RomaniaParser extends Parser<Declaration> {

    public RomaniaParser(Site site, Path shapeFilePath) {
        super(site, shapeFilePath);
    }

    @Override
    protected Set<String> mandatoryAttributes() {
        return new HashSet<String>() {{
            add("sirsup_cod");
            add("bloc_nr");
            add("parcel_nr");
            add("crop_nr");
            add("area_decla");
            add("crop_code");
        }};
    }

    @Override
    protected Declaration parseFeature(SimpleFeature feature, MathTransform reprojection) throws Exception {
        Declaration declaration = new Declaration();
        declaration.setFootprint(JTS.transform((Geometry) feature.getDefaultGeometry(), reprojection));
        declaration.setLpisId(feature.getAttribute("sirsup_cod").toString() + "-" +
                         feature.getAttribute("bloc_nr").toString());
        declaration.setParcelId(feature.getAttribute("parcel_nr").toString() + "-" +
                                feature.getAttribute("crop_nr").toString());
        declaration.setArea((Double) feature.getAttribute("area_decla"));
        declaration.setOriginalLandUseCode(feature.getAttribute("crop_code").toString());
        return declaration;
    }
}
