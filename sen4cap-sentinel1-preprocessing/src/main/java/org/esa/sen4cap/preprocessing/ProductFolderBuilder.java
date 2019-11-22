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

import org.esa.sen2agri.entities.enums.ProductType;

public class ProductFolderBuilder {
    private StringBuilder builder;
    private Integer siteId;
    private String masterDate;
    private String slaveDate;
    private String relativeOrbit;
    private Polarisation polarisation;
    private ProductType productType;

    public ProductFolderBuilder() {
        this.builder = new StringBuilder();
    }

    public ProductFolderBuilder withSiteId(int siteId) {
        this.siteId = siteId;
        return this;
    }

    public ProductFolderBuilder withMasterDate(String masterDate) {
        this.masterDate = masterDate;
        return this;
    }

    public ProductFolderBuilder withSlaveDate(String slaveDate) {
        this.slaveDate = slaveDate;
        return this;
    }

    public ProductFolderBuilder withRelativeOrbit(String relativeOrbit) {
        this.relativeOrbit = relativeOrbit;
        return this;
    }

    public ProductFolderBuilder withPolarisation(Polarisation polarisation) {
        this.polarisation = polarisation;
        return this;
    }

    public ProductFolderBuilder withProductType(ProductType productType) {
        this.productType = productType;
        return this;
    }

    public String build() {
        if (this.siteId == null || this.masterDate == null || this.relativeOrbit == null || this.polarisation == null) {
            throw new IllegalArgumentException("Inconsistent product name");
        }
        this.builder.setLength(0);
        this.builder.append("SEN4CAP").append("_")
                    .append("L2A").append("_")
                    .append("S").append(siteId).append("_")
                    .append("V").append(masterDate).append("_");
        if (this.slaveDate != null) {
            this.builder.append(slaveDate).append("_");
        }
        this.builder.append(polarisation.name()).append("_")
                    .append(relativeOrbit);
        if (this.productType != null) {
            this.builder.append("_").append(productType.shortName().replace("l2-", "").toUpperCase());
        }
        return this.builder.toString();
    }
}
