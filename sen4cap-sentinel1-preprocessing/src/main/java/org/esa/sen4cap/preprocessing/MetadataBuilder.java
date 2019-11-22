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

package org.esa.sen4cap.preprocessing;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.esa.sen2agri.entities.enums.OrbitType;
import org.esa.sen4cap.entities.enums.ProductType;
import org.locationtech.jts.geom.Geometry;
import ro.cs.tao.eodata.enums.PixelType;

import java.io.IOException;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.stream.Collectors;

public class MetadataBuilder {
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
    private String productName;
    private ProductType productType;
    private Geometry productFootprint;
    private PixelType pixelType;
    private int width;
    private int height;
    private String crs;
    private String productFormat;
    private String master;
    private String masterDate;
    private Geometry masterFootprint;
    private String slave;
    private String slaveDate;
    private Geometry slaveFootprint;
    private int relativeOrbit;
    private OrbitType orbitType;
    private Polarisation polarisation;
    private Instant startTime;
    private Instant endTime;
    private long productSize;
    private Double min;
    private Double max;
    private Double mean;
    private Double stdDev;
    private Integer[] histogram;
    private String acquisition;
    private LocalDateTime acquisitionDate;
    private Integer intersectionDateOffset;
    private LocalDateTime createdOn;

    MetadataBuilder() { }

    private MetadataBuilder(MetadataBuilder other) {
        this.productName = other.productName;
        this.productType = other.productType;
        this.productFootprint = other.productFootprint;
        this.pixelType = other.pixelType;
        this.width = other.width;
        this.height = other.height;
        this.crs = other.crs;
        this.productFormat = other.productFormat;
        this.master = other.master;
        this.masterDate = other.masterDate;
        this.masterFootprint = other.masterFootprint;
        this.slave = other.slave;
        this.slaveDate = other.slaveDate;
        this.slaveFootprint = other.slaveFootprint;
        this.relativeOrbit = other.relativeOrbit;
        this.orbitType = other.orbitType;
        this.polarisation = other.polarisation;
        this.startTime = other.startTime;
        this.endTime = other.endTime;
        this.productSize = other.productSize;
        this.min = other.min;
        this.max = other.max;
        this.mean = other.mean;
        this.stdDev = other.stdDev;
        this.histogram = other.histogram;
        this.acquisition = other.acquisition;
        this.acquisitionDate = other.acquisitionDate;
        this.intersectionDateOffset = other.intersectionDateOffset;
        this.createdOn = other.createdOn;
    }

    @Override
    public String toString() {
        Document document = DocumentHelper.createDocument();
        Element productElement = document.addElement("product").addAttribute("type", productType.shortName());
        productElement.addElement("name").addText(productName);
        productElement.addElement("acquisition").addText(acquisition != null ? acquisition : "n/a");
        productElement.addElement("acquisitionDate").addText(acquisitionDate != null ? acquisitionDate.format(dateFormatter) : "n/a");
        productElement.addElement("intersectionDateOffset").addText(intersectionDateOffset != null ? String.valueOf(intersectionDateOffset) : "n/a");
        productElement.addElement("format").addText(productFormat);
        productElement.addElement("pixelType").addText(pixelType != null ? pixelType.name() : "n/a");
        productElement.addElement("size").addAttribute("unit", "bytes").addText(String.valueOf(productSize));
        productElement.addElement("footprint").addText(productFootprint != null ? productFootprint.toText() : "n/a");
        productElement.addElement("relativeOrbit").addText(String.valueOf(relativeOrbit));
        productElement.addElement("orbitType").addText(orbitType != null ? orbitType.name() : "n/a");
        productElement.addElement("polarisation").addText(polarisation != null ? polarisation.name() : "n/a");
        LocalDateTime created = createdOn != null ? createdOn : LocalDateTime.now();
        productElement.addElement("created").addText(created.format(dateFormatter));
        if (startTime != null && endTime != null) {
            Duration elapsed = Duration.between(startTime, endTime);
            productElement.addElement("processedIn").addText(String.format("%2d:%2d:%2d",
                                                                           elapsed.toHours(),
                                                                           elapsed.toMinutes() % 60,
                                                                           elapsed.getSeconds() % 3600));
        }
        Element masterElement = productElement.addElement("master");
        masterElement.addElement("name").addText(master);
        masterElement.addElement("sensingDate").addText(masterDate);
        masterElement.addElement("footprint").addText(masterFootprint != null ? masterFootprint.toText() : "n/a");
        if (slave != null) {
            Element slaveElement = productElement.addElement("slave");
            slaveElement.addElement("name").addText(slave);
            slaveElement.addElement("sensingDate").addText(slaveDate);
            slaveElement.addElement("footprint").addText(slaveFootprint != null ? slaveFootprint.toText() : "n/a");
        }
        if (min != null && max != null && mean != null && stdDev != null) {
            Element statsElement = productElement.addElement("statistics");
            statsElement.addElement("min").addText(String.valueOf(min));
            statsElement.addElement("max").addText(String.valueOf(max));
            statsElement.addElement("mean").addText(String.valueOf(mean));
            statsElement.addElement("stdDev").addText(String.valueOf(stdDev));
            if (histogram != null) {
                Element histElement = statsElement.addElement("histogram");
                histElement.addAttribute("bins", String.valueOf(histogram.length));
                histElement.addText(Arrays.stream(histogram)
                                            .map(String::valueOf)
                                            .collect(Collectors.joining()));
            }
        }
        StringWriter writer = new StringWriter();
        XMLWriter xmlWriter = new XMLWriter(writer, OutputFormat.createPrettyPrint());
        try {
            xmlWriter.write(document);
            xmlWriter.flush();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return writer.toString();
    }

    MetadataBuilder withProductName(String productName) {
        this.productName = productName;
        return this;
    }

    MetadataBuilder withAcquisition(String acquisition) {
        this.acquisition = acquisition;
        return this;
    }

    MetadataBuilder withAcquisitionDate(LocalDateTime acquisitionDate) {
        this.acquisitionDate = acquisitionDate;
        return this;
    }

    MetadataBuilder withCreatedOn(LocalDateTime createdOn) {
        this.createdOn = createdOn;
        return this;
    }

    MetadataBuilder withIntersectionOffset(int offset) {
        this.intersectionDateOffset = offset;
        return this;
    }

    MetadataBuilder withProductType(ProductType productType) {
        this.productType = productType;
        return this;
    }

    MetadataBuilder withProductFootprint(Geometry productFootprint) {
        this.productFootprint = productFootprint;
        return this;
    }

    MetadataBuilder withPixelType(PixelType pixelType) {
        this.pixelType = pixelType;
        return this;
    }

    MetadataBuilder withWidth(int width) {
        this.width = width;
        return this;
    }

    MetadataBuilder withHeight(int height) {
        this.height = height;
        return this;
    }

    MetadataBuilder withCrs(String crs) {
        this.crs = crs;
        return this;
    }

    MetadataBuilder withProductFormat(String productFormat) {
        this.productFormat = productFormat;
        return this;
    }

    MetadataBuilder withMaster(String master) {
        this.master = master;
        return this;
    }

    MetadataBuilder withMasterDate(String masterDate) {
        this.masterDate = masterDate;
        return this;
    }

    MetadataBuilder withMasterFootprint(Geometry masterFootprint) {
        this.masterFootprint = masterFootprint;
        return this;
    }

    MetadataBuilder withSlave(String slave) {
        this.slave = slave;
        return this;
    }

    MetadataBuilder withSlaveDate(String slaveDate) {
        this.slaveDate = slaveDate;
        return this;
    }

    MetadataBuilder withSlaveFootprint(Geometry slaveFootprint) {
        this.slaveFootprint = slaveFootprint;
        return this;
    }

    MetadataBuilder withRelativeOrbit(int relativeOrbit) {
        this.relativeOrbit = relativeOrbit;
        return this;
    }

    MetadataBuilder withOrbitType(OrbitType orbitType) {
        this.orbitType = orbitType;
        return this;
    }

    MetadataBuilder withPolarisation(Polarisation polarisation) {
        this.polarisation = polarisation;
        return this;
    }

    MetadataBuilder withStartTime(Instant startTime) {
        this.startTime = startTime;
        return this;
    }

    MetadataBuilder withEndTime(Instant endTime) {
        this.endTime = endTime;
        return this;
    }

    MetadataBuilder withProductSize(long productSize) {
        this.productSize = productSize;
        return this;
    }

    MetadataBuilder withMinimum(double minimum) {
        this.min = minimum;
        return this;
    }

    MetadataBuilder withMaximum(double maximum) {
        this.max = maximum;
        return this;
    }

    MetadataBuilder withMean(double mean) {
        this.mean = mean;
        return this;
    }

    MetadataBuilder withStandardDeviation(double stdDev) {
        this.stdDev = stdDev;
        return this;
    }

    MetadataBuilder withHistogram(Integer[] histogram) {
        this.histogram = histogram;
        return this;
    }

    public MetadataBuilder clone() {
        return new MetadataBuilder(this);
    }
}
