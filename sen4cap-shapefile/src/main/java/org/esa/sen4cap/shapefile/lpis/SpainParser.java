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
 * Parser for Spain LPIS shape files. Structure of a record:
 *      DN_OID      identifier of the record (not of the parcel)
 *      SUPERFICIE  area
 *      PROVINCIA	province
 *      MUNICIPIO	town
 *      AGREGADO	own subdivision, that normaly does not exists and is 0
 *      ZONA	    other town subdivision, that normaly does not exists and is 0
 *      POLIGONO	Group of parcels that belong to a block of land, it is a cadastral subdivision
 *      PARCELA	    cadastral parcel
 *      RECINTO	    Actual parcel that the farmer crops. This is the LPIS. "parcelas" could have some "recintos" inside with different land cover
 *      USO_SIGPAC  ? maybe land cover class ?
 *      COEF_REGAD	COEF=Coeficiente (Coefficient) and REGAD=Regadío (Irrigation).
 *                  It goes from 0 (no irrigation allowed) to 100% (irrigation allowed on all the area within the parcel).
 *                  It represents the percentage of the area that could be legally irrigated.
 *
 * @author Cosmin Cara
 */
@LPIS(countryCode = "ESP")
public class SpainParser extends Parser<Parcel> {

    public SpainParser(Site site, Path shapeFilePath) { super(site, shapeFilePath); }

    @Override
    protected Set<String> mandatoryAttributes() {
        return new HashSet<String>() {{
            add("PROVINCIA");
            add("MUNICIPIO");
            add("AGREGADO");
            add("ZONA");
            add("POLIGONO");
            add("PARCELA");
            add("RECINTO");
        }};
    }

    @Override
    protected Parcel parseFeature(SimpleFeature feature, MathTransform reprojection) throws Exception {
        Parcel parcel = new Parcel();
        parcel.setFootprint(JTS.transform((Geometry) feature.getDefaultGeometry(), reprojection));
        String identifier = feature.getAttribute("PROVINCIA").toString() + "-" +
                            feature.getAttribute("MUNICIPIO").toString() + "-" +
                            feature.getAttribute("AGREGADO").toString() + "-" +
                            feature.getAttribute("ZONA").toString() + "-" +
                            feature.getAttribute("POLIGONO").toString() + "-" +
                            feature.getAttribute("PARCELA").toString() + "-" +
                            feature.getAttribute("RECINTO").toString();
        parcel.setIdentifier(identifier);
        parcel.setArea((Double) feature.getAttribute("SUPERFICIE"));
        parcel.setOriginalLandUseCode(feature.getAttribute("USO_SIGPAC").toString());
        return parcel;
    }
}
