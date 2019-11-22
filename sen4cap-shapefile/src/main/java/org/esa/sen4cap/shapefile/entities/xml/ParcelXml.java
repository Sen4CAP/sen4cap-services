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

package org.esa.sen4cap.shapefile.entities.xml;

import org.esa.sen4cap.shapefile.entities.IntervalInfos;
import org.esa.sen4cap.shapefile.entities.ParcelInfo;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.time.LocalDate;
import java.util.List;

@XmlRootElement(name = "fid")
public class ParcelXml {
    private String id;
    private String origId;
    private IntervalInfos harvest;
    private IntervalInfos practice;
    private List<Measure> ndvis;
    private List<Measure> amps;
    private List<Measure> cohs;

    @XmlAttribute(name = "id")
    public String getId() {
        return id;
    }

    public void setOrigId(String origId) {
        this.origId = origId;
    }

    @XmlAttribute(name = "orig_id")
    public String getOrigId() {
        return origId;
    }

    public void setId(String id) {
        this.id = id;
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

    @XmlElementWrapper(name = "ndvis")
    @XmlElement(name = "ndvi")
    public List<Measure> getNdvis() {
        return ndvis;
    }

    public void setNdvis(List<Measure> ndvis) {
        this.ndvis = ndvis;
    }

    @XmlElementWrapper(name = "amps")
    @XmlElement(name = "amp")
    public List<Measure> getAmps() {
        return amps;
    }

    public void setAmps(List<Measure> amps) {
        this.amps = amps;
    }

    @XmlElementWrapper(name = "cohs")
    @XmlElement(name = "coh")
    public List<Measure> getCohs() {
        return cohs;
    }

    public void setCohs(List<Measure> cohs) {
        this.cohs = cohs;
    }

    public ParcelInfo toInfo() {
        ParcelInfo info = new ParcelInfo();
        info.setId(this.id);
        info.setOrigId(this.origId);
        info.setHarvest(this.harvest);
        info.setPractice(this.practice);
        if (this.ndvis != null) {
            for (Measure measure : this.ndvis) {
                info.addNdvi(LocalDate.parse(measure.getDate()), Double.parseDouble(measure.getValue()));
            }
        }
        if (this.amps != null) {
            for (Measure measure : this.amps) {
                info.addAmp(LocalDate.parse(measure.getDate()), Double.parseDouble(measure.getValue()));
            }
        }
        if (this.cohs != null) {
            for (Measure measure : this.cohs) {
                info.addCoh(LocalDate.parse(measure.getDate()), Double.parseDouble(measure.getValue()));
            }
        }
        return info;
    }
}
