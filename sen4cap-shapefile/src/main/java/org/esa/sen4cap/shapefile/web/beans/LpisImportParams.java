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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LpisImportParams implements ImportParams {
    private final int siteId;
    private final int year;
    private final String[] parcelColumns;
    private final String[] holdingColumns;
    private final String cropCodeColumn;
    private final String lpisFile;
    private final String lutFile;
    private final Mode mode;

    public LpisImportParams(int siteId, int year, String[] parcelColumns, String[] holdingColumns, String cropCodeColumn, String lpisFile, String lutFile, Mode mode) {
        this.siteId = siteId;
        this.year = year;
        this.parcelColumns = parcelColumns;
        this.holdingColumns = holdingColumns;
        this.cropCodeColumn = cropCodeColumn;
        this.lpisFile = lpisFile;
        this.lutFile = lutFile;
        this.mode = mode != null ? mode : Mode.UPDATE;
    }

    @Override
    public List<String> toArguments() {
        final List<String> args = new ArrayList<>();
        args.add("-s");
        args.add(String.valueOf(siteId));
        args.add("--year");
        args.add(String.valueOf(year));
        if (lpisFile != null) {
            args.add("--parcel-id-cols");
            args.addAll(Arrays.asList(parcelColumns));
            args.add("--holding-id-cols");
            args.addAll(Arrays.asList(holdingColumns));
            args.add("--crop-code-col");
            args.add(cropCodeColumn);
            args.add("--lpis");
            args.add(lpisFile);
        }
        if (lutFile != null) {
            args.add("--lut");
            args.add(lutFile);
        }
        args.add("--mode");
        args.add(this.mode.name().toLowerCase());
        return args;
    }

    @Override
    public String toJson() {
        JSONObject json = new JSONObject();
        json.put("s", siteId);
        json.put("year", year);
        if (lpisFile != null) {
            json.put("parcel-id-cols",toJsonArray(parcelColumns));
            json.put("holding-id-cols", toJsonArray(holdingColumns));
            json.put("crop-code-col", cropCodeColumn);
            json.put("lpis", lpisFile);
        }
        if (lutFile != null) {
            json.put("lut", lutFile);
        }
        json.put("mode", mode.name().toLowerCase());
        return json.toString();
    }

    private JSONArray toJsonArray(String[] arr) {
        JSONArray jsonArr = new JSONArray();
        Arrays.stream(arr).forEach(e -> { jsonArr.add(e); });
        return jsonArr;
    }

    public enum Mode {
        REPLACE,
        UPDATE,
        INCREMENTAL
    }
}
