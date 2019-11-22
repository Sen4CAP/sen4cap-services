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

public class L4CPracticeImportParams implements ImportParams {
    private final String siteShortName;
    private final int year;
    private final String practice;
    private final String l4cPracticeFile;

    public L4CPracticeImportParams(String siteShortName, int year, String practice, String l4cPracticeFile) {
        this.siteShortName = siteShortName;
        this.year = year;
        this.practice = practice;
        this.l4cPracticeFile = l4cPracticeFile;
    }

    @Override
    public List<String> toArguments() {
        final List<String> args = new ArrayList<>();
        args.add("-s");
        args.add(siteShortName);
        args.add("-y");
        args.add(String.valueOf(year));
        args.add("-p");
        args.add(practice);
        args.add("-i");
        args.add(l4cPracticeFile);
        return args;
    }

    @Override
    public String toJson() {
        JSONObject json = new JSONObject();
        json.put("site", siteShortName);
        json.put("year", year);
        json.put("practice", practice);
        json.put("l4cPracticeFile", l4cPracticeFile);
        return json.toString();
    }
}
