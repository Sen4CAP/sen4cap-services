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

@XmlEnum(Integer.class)
public enum Processor implements TaoEnum<Integer> {
    @XmlEnumValue("1")
    L2A(1, "l2a", "L2A Atmospheric Corrections"),
    @XmlEnumValue("3")
    L3B(3, "l3b_lai", "L3B Vegetation Status"),
    @XmlEnumValue("4")
    L4A(4, "s4c_l4a", "L4A Crop Type"),
    @XmlEnumValue("5")
    L4B(5, "s4c_l4b", "LB Grassland Mowing"),
    @XmlEnumValue("6")
    L4C(6, "s4c_l4c", "L4C Agricultural Practices"),
    @XmlEnumValue("7")
    L2S1(7, "l2-s1", "L2-S1 Pre-Processor"),
    @XmlEnumValue("8")
    LPIS(8, "lpis", "LPIS/GSAA");

    private final int value;
    private final String shortName;
    private final String description;

    Processor(int value, String shortName, String description) {
        this.value = value;
        this.shortName = shortName;
        this.description = description;
    }

    public String shortName() { return shortName; }

    @Override
    public String friendlyName() { return this.description; }

    @Override
    public Integer value() { return this.value; }
}
