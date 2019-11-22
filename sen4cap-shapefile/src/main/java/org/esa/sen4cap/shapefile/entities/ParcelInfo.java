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

package org.esa.sen4cap.shapefile.entities;

import java.time.LocalDate;
import java.util.LinkedHashMap;

public class ParcelInfo {
    private String id;
    private String origId;
    private IntervalInfos practice;
    private IntervalInfos harvest;
    private LinkedHashMap<LocalDate, Double> ndvi;
    private LinkedHashMap<LocalDate, Double> amp;
    private LinkedHashMap<LocalDate, Double> coh;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOrigId() {
        return origId;
    }

    public void setOrigId(String origId) {
        this.origId = origId;
    }

    public IntervalInfos getHarvest() {
        return harvest;
    }

    public void setHarvest(IntervalInfos harvest) {
        this.harvest = harvest;
    }

    public IntervalInfos getPractice() {
        return practice;
    }

    public void setPractice(IntervalInfos practice) {
        this.practice = practice;
    }

    public LinkedHashMap<LocalDate, Double> getNdvi() {
        return ndvi;
    }

    public void setNdvi(LinkedHashMap<LocalDate, Double> ndvi) {
        this.ndvi = ndvi;
    }

    public void addNdvi(LocalDate date, double value) {
        if (this.ndvi == null) {
            this.ndvi = new LinkedHashMap<>();
        }
        this.ndvi.put(date, value);
    }

    public LinkedHashMap<LocalDate, Double> getAmp() {
        return amp;
    }

    public void setAmp(LinkedHashMap<LocalDate, Double> amp) {
        this.amp = amp;
    }

    public void addAmp(LocalDate date, double value) {
        if (this.amp == null) {
            this.amp = new LinkedHashMap<>();
        }
        this.amp.put(date, value);
    }

    public LinkedHashMap<LocalDate, Double> getCoh() {
        return coh;
    }

    public void setCoh(LinkedHashMap<LocalDate, Double> coh) {
        this.coh = coh;
    }

    public void addCoh(LocalDate date, double value) {
        if (this.coh == null) {
            this.coh = new LinkedHashMap<>();
        }
        this.coh.put(date, value);
    }
}
