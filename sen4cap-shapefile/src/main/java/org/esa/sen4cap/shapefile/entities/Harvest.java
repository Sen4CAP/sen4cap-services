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

import ro.cs.tao.serialization.LocalDateAdapter;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

@Deprecated
public class Harvest {
    private String ps;
    private String pe;

    public Harvest() { }

    public Harvest(String ps, String pe) {
        this.ps = ps;
        this.pe = pe;
    }

    @XmlJavaTypeAdapter(LocalDateAdapter.class)
    @XmlAttribute(name = "ps")
    public String getPs() {
        return ps;
    }

    public void setPs(String ps) {
        this.ps = ps;
    }

    @XmlJavaTypeAdapter(LocalDateAdapter.class)
    @XmlAttribute(name = "pe")
    public String getPe() {
        return pe;
    }

    public void setPe(String pe) {
        this.pe = pe;
    }
}
