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

package org.esa.sen4cap.shapefile.lpis;

import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.shapefile.entities.Parcel;
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
@LPIS(countryCode = "LTU")
public class LithuaniaParser extends Parser<Parcel> {
    public LithuaniaParser(Site site, Path shapeFilePath) {
        super(site, shapeFilePath);
    }

    @Override
    protected Set<String> mandatoryAttributes() {
        return new HashSet<String>() {{
            add("BLOKAS_ID");
            add("PLOTAS_HA");
            add("GKODAS");
        }};
    }

    @Override
    protected Parcel parseFeature(SimpleFeature feature, MathTransform reprojection) throws Exception {
        Parcel parcel = null;
        try {
            parcel = new Parcel();
            parcel.setFootprint(JTS.transform((Geometry) feature.getDefaultGeometry(), reprojection));
            parcel.setIdentifier(feature.getAttribute("BLOKAS_ID").toString());
            parcel.setArea((Double) feature.getAttribute("PLOTAS_HA"));
            parcel.setOriginalLandUseCode(feature.getAttribute("GKODAS").toString());
        } catch (Exception ex) {
            logger.warning(String.format("Error parsing feature %s: %s", feature.toString(), ex.getMessage()));
        }
        return parcel;
    }
}
