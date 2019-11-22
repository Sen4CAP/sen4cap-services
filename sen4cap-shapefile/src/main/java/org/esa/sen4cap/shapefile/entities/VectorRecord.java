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

import org.esa.sen2agri.entities.Site;
import org.locationtech.jts.geom.Geometry;
import ro.cs.tao.eodata.Attribute;
import ro.cs.tao.serialization.MediaType;
import ro.cs.tao.serialization.SerializationException;
import ro.cs.tao.serialization.Serializer;
import ro.cs.tao.serialization.SerializerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Cosmin Cara
 */
public abstract class VectorRecord {
    private static final Serializer<Attribute, String> serializer;

    static {
        try {
            serializer = SerializerFactory.create(Attribute.class, MediaType.JSON);
            serializer.setFormatOutput(false);
        } catch (SerializationException e) {
            throw new InstantiationError(e.getMessage());
        }
    }

    private Site site;
    private String originalLandUseCode;
    private Integer systemLandUseCode;
    private Double area;
    private Geometry footprint;
    private LocalDate date;
    private String sourceFile;
    private List<Attribute> attributeList;

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public String getOriginalLandUseCode() {
        return originalLandUseCode;
    }

    public void setOriginalLandUseCode(String originalLandUseCode) {
        this.originalLandUseCode = originalLandUseCode;
    }

    public Integer getSystemLandUseCode() {
        return systemLandUseCode;
    }

    public void setSystemLandUseCode(Integer systemLandUseCode) {
        this.systemLandUseCode = systemLandUseCode;
    }

    public Double getArea() {
        return area;
    }

    public void setArea(Double area) {
        this.area = area;
    }

    public Geometry getFootprint() {
        return footprint;
    }

    public void setFootprint(Geometry footprint) {
        this.footprint = footprint;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public String getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(String sourceFile) {
        this.sourceFile = sourceFile;
    }

    public String getAttributes() {
        try {
            return attributeList != null ? serializer.serialize(attributeList, "") : null;
        } catch (SerializationException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void setAttributes(String attributes) {
        try {
            this.attributeList = serializer.deserialize(Attribute.class, attributes);
        } catch (SerializationException e) {
            e.printStackTrace();
        }
    }

    public void addAttribue(String name, String value) {
        if (attributeList == null) {
            attributeList = new ArrayList<>();
        }
        Attribute attr = new Attribute();
        attr.setName(name);
        attr.setValue(value);
        attributeList.add(attr);
    }
}
