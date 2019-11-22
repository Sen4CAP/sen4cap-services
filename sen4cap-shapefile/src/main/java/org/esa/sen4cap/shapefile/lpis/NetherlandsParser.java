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
 * Parser for Netherlands LPIS shape files. Structure of a record:
 *      FUNCTIONEEL_ID	identification of the parcel
 *      BEGINGELDIGHEID	commencement of the parcel
 *      EINDGELDIGHEID	finality of the parcel
 *      GRONDBEDEKKING	crop code of the main crop
 *      AANGEVRAAGD	income support declared on the parcel, J of N
 *      IND_EA	EFA declared on the parcel, J of N
 *      EAOPPERVLAKTE	area of the EFA main crop, weighing factor included
 *      GRONDBEDEKKING2EAOPP	area of the EFA catch crop, weighing factor included
 *      Omschr_gewas 	description of the main crop in the BBR-application
 *      CODE_BEHEERPAKKET1	code of the 1st overlapping ANLB-package
 *      Omschr_BHRpakket1 	description  of the 1st ANLB-package
 *      Perc_overlap1 	overlap in percentage of the 1st ANLB-package with the BBR-parcel
 *      CODE_BEHEERPAKKET2 	code of the 2nd overlapping ANLB-package
 *      Omschr_BHRpakket2	description of the 2nd ANLB-package
 *      Perc_overlap2 	overlap in percentage of the 2nd ANLB-package with the BBR-parcel
 *      Teledetectie_controle 	Is BBR-parcel controlled by teledetection, J of N
 *      NVWA_controle 	Is BBR-parcel controlled by NVWA, J of N
 *      SECTOR_ID	is sector-id of the overlapping ANLB-parcel in a 1 to 1 relationship or more (BBR-parcels) to 1 relationship, this as a link to the NVWA control results
 *      GRONDBEDEKKING2	crop code of the catch crop in case of EFA
 *      Omschr_gewas_2	description of the catch crop in case of EFA
 *      GRNDBDK2_CAT	category in case of a catch crop EFA
 *
 * @author Cosmin Cara
 */
@LPIS(countryCode = "NLD")
public class NetherlandsParser extends Parser<Parcel> {

    public NetherlandsParser(Site site, Path shapeFilePath) {
        super(site, shapeFilePath);
    }

    @Override
    protected Set<String> mandatoryAttributes() {
        return new HashSet<String>() {{
            add("FUNCTIONEE");
            add("SHAPE_Area");
            add("GRONDBEDEK");
        }};
    }

    @Override
    protected Parcel parseFeature(SimpleFeature feature, MathTransform reprojection) throws Exception {
        Parcel parcel = new Parcel();
        parcel.setFootprint(JTS.transform((Geometry) feature.getDefaultGeometry(), reprojection));
        parcel.setIdentifier(feature.getAttribute("FUNCTIONEE").toString());
        parcel.setArea((Double) feature.getAttribute("SHAPE_Area"));
        parcel.setOriginalLandUseCode(feature.getAttribute("GRONDBEDEK").toString());
        return parcel;
    }
}
