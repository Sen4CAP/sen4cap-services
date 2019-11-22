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

package org.esa.sen4cap.entities.enums;

import ro.cs.tao.TaoEnum;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;

/**
 * @author Cosmin Cara
 */
@XmlEnum(Short.class)
public enum ProductType implements TaoEnum<Short> {

    @XmlEnumValue("1")
    L2A(1, "l2a", "L2A Atmospheric correction"),
    @XmlEnumValue("3")
    L3B_MONODATE(3, "l3b_lai_monodate", "L3B LAI mono-date product"),
    @XmlEnumValue("4")
    L4A(4, "s4c_l4a", "L4A Crop type product"),
    @XmlEnumValue("5")
    L4B(5, "s4c_l4b", "L4B Grassland Mowing product"),
    @XmlEnumValue("6")
    L4C(6, "s4c_l4c", "L4C Agricultural Practices product"),
    @XmlEnumValue("7")
    L1C(7, "l1c", "L1C product"),
    @XmlEnumValue("8")
    L3C_REPROCESSED(8, "l3c_lai_reproc", "L3C LAI Reprocessed product"),
    @XmlEnumValue("10")
    L2A_AMP(10, "s1_l2a_amp", "Sentinel 1 L2 Amplitude product"),
    L2A_COHE(11, "s1_l2a_cohe", "Sentinel 1 L2 Coherence product"),
    LPIS(14, "lpis", "LPIS raster product");

    private final short value;
    private final String shortName;
    private final String description;

    ProductType(int value, String shortName, String description) {
        this.value = (short) value;
        this.shortName = shortName;
        this.description = description;
    }

    public String shortName() { return shortName; }

    @Override
    public String friendlyName() { return this.description; }

    @Override
    public Short value() { return this.value; }
}
