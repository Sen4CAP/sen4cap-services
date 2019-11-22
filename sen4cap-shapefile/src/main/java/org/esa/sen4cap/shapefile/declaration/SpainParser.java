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
@DECLARATION(countryCode = "ESP")
public class SpainParser extends Parser<Declaration> {
    public SpainParser(Site site, Path shapeFilePath) {
        super(site, shapeFilePath);
    }

    @Override
    protected Set<String> mandatoryAttributes() {
        return new HashSet<String>() {{
            add("C_PROV_EXP");
            add("C_MUNI_CAT");
            add("C_AGREGADO");
            add("C_ZONA");
            add("C_POLIGONO");
            add("C_PARCELA");
            add("C_RECINTO");
            add("C_SAC_EXPE");
            add("C_NUME_EXP");
            add("L_SEMBRADA");
            add("C_PRODUCTO");
            add("C_NUM_ORDE");
        }};
    }

    @Override
    protected Declaration parseFeature(SimpleFeature feature, MathTransform reprojection) throws Exception {
        Declaration declaration = new Declaration();
        Geometry geometry = (Geometry) feature.getDefaultGeometry();
        if (geometry != null) {
            declaration.setFootprint(JTS.transform(geometry, reprojection));
        }
        String lpisId = feature.getAttribute("C_PROV_EXP").toString() + "-" +
                        feature.getAttribute("C_MUNI_CAT").toString() + "-" +
                        feature.getAttribute("C_AGREGADO").toString() + "-" +
                        feature.getAttribute("C_ZONA").toString() + "-" +
                        feature.getAttribute("C_POLIGONO").toString() + "-" +
                        feature.getAttribute("C_PARCELA").toString() + "-" +
                        feature.getAttribute("C_RECINTO").toString();
        declaration.setLpisId(lpisId);
        String parcelId = feature.getAttribute("C_PROV_EXP").toString() + "-" +
                          feature.getAttribute("C_SAC_EXPE").toString() + "-" +
                          feature.getAttribute("C_NUME_EXP").toString() + "-" +
                          feature.getAttribute("C_NUM_ORDE").toString();
        declaration.setParcelId(parcelId);
        declaration.setArea((Double) feature.getAttribute("L_SEMBRADA"));
        declaration.setOriginalLandUseCode(feature.getAttribute("C_PRODUCTO").toString());
        return declaration;
    }

}
