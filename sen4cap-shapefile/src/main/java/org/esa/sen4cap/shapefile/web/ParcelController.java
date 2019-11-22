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

package org.esa.sen4cap.shapefile.web;

import org.esa.sen2agri.commons.Config;
import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.shapefile.entities.ParcelInfo;
import org.esa.sen4cap.shapefile.entities.Practice;
import org.esa.sen4cap.shapefile.services.internal.ParcelQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import ro.cs.tao.services.commons.ControllerBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Controller
@RequestMapping("/parcel")
public class ParcelController  extends ControllerBase {

    private Logger logger = Logger.getLogger(ParcelController.class.getName());

    @Autowired
    private ParcelQueryService parcelQueryService;

    @RequestMapping(method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> getParcelInfo(@RequestParam("site") String siteCode,
                                           @RequestParam("id") String id,
                                           @RequestParam("practice") Practice practice,
                                           @RequestParam("year") int year) {
        ResponseEntity<?> result;
        try {
            if (id == null || id.isEmpty()) {
                throw new IllegalArgumentException("Invalid parcel identifier");
            }
            List<Site> sites = Config.getPersistenceManager().getAllSites();
            List<ParcelInfo> parcels;
            List<ParcelInfo> allParcels = new ArrayList<>();
            boolean countryCodeFound = false;
            String[] parcelIds = id.split(",");
            String siteCodeLC = siteCode.toLowerCase();
            for (Site site : sites) {
                try {
                    if (site.getName().toLowerCase().contains(siteCodeLC)) {
                        countryCodeFound = true;
                        parcels = parcelQueryService.getParcelInfo(site, year, practice, parcelIds);
                        // add the extracted parcels to the returning parcels list
                        allParcels.addAll(parcels);
                        // get the parcel ids that were not yet extracted from the previous countries
                        parcelIds = getRemainingParcelIds(allParcels, parcelIds);
                        // all parcels extracted, no need to check other countries
                        if (parcelIds.length == 0) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    logger.warning(String.format("Error extracting the parcel information for site %s. Error was %s",
                            site.getName(), e.getMessage()));
                }
            }
            if (!countryCodeFound) {
                throw new IllegalArgumentException("Invalid country code");
            }
            if (allParcels.size() == 0) {
                throw new IOException(String.format("None of the parcels requested in the list were " +
                        "not found in the sites with code %s for year %d", siteCode, year));
            }
            result = prepareResult(allParcels);
        } catch (Exception e) {
            result = handleException(e);
        }
        return result;
    }

    private String[] getRemainingParcelIds(List<ParcelInfo> parcels, String[] parcelIds) {
        List<String> retList = new ArrayList<>();
        for (String parcelId: parcelIds) {
            if (parcels.stream().filter(x -> x.getId().equals(parcelId)).findFirst().orElse(null) == null) {
                retList.add(parcelId);
            }
        }
        return retList.toArray(new String[0]);
    }
}
