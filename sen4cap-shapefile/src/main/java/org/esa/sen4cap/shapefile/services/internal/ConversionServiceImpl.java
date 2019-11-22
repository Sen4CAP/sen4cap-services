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

import no.ecc.vectortile.VectorTileEncoder;
import org.esa.sen4cap.shapefile.services.ConversionService;
import org.locationtech.jts.geom.Geometry;

import java.util.Collection;
import java.util.HashSet;

/**
 * @author Cosmin Cara
 */
public class ConversionServiceImpl implements ConversionService {
    @Override
    public byte[] encodeSingle(Geometry geometry) {
        Collection<Geometry> single = new HashSet<>();
        single.add(geometry);
        return encode(single);
    }

    @Override
    public byte[] encode(Collection<Geometry> geometries) {
        if (geometries == null || geometries.isEmpty()) {
            return null;
        }
        VectorTileEncoder encoder = new VectorTileEncoder();
        geometries.forEach(geometry -> encoder.addFeature("VectorTiles", null, geometry));
        return encoder.encode();
        /*JtsLayer layer = new JtsLayer("VectorTiles", geometries);
        JtsMvt mvt = new JtsMvt(layer);
        return MvtEncoder.encode(mvt);*/
    }
}
