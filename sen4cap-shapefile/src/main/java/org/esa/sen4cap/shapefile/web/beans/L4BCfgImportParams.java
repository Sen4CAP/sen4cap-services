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

package org.esa.sen4cap.shapefile.web.beans;

import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class L4BCfgImportParams implements ImportParams {
    private final String siteShortName;
    private final int year;
    private final String l4bCfgFile;
    private final String mowingStartDate;

    public L4BCfgImportParams(String siteShortName, int year, String l4bCfgFile, String mowingStartDate) {
        this.siteShortName = siteShortName;
        this.year = year;
        this.l4bCfgFile = l4bCfgFile;
        this.mowingStartDate = mowingStartDate;
    }

    @Override
    public List<String> toArguments() {
        final List<String> args = new ArrayList<>();
        args.add("-s");
        args.add(siteShortName);
        args.add("-y");
        args.add(String.valueOf(year));
        args.add("-i");
        args.add(l4bCfgFile);
        if (mowingStartDate != null) {
            args.add("-d");
            args.add(mowingStartDate);
        }
        return args;
    }

    @Override
    public String toJson() {
        JSONObject json = new JSONObject();
        json.put("site", siteShortName);
        json.put("year", year);
        json.put("l4bCfgFile", l4bCfgFile);
        if (mowingStartDate != null) {
            json.put("mowingStartDate", mowingStartDate);
        }
        return json.toString();
    }
}
