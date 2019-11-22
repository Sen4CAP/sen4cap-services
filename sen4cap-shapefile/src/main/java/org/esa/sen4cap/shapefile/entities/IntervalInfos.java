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

import javax.xml.bind.annotation.XmlAttribute;

public class IntervalInfos {
    private String start;
    private String end;

    public IntervalInfos() { }

    public IntervalInfos(String start, String end) {
        this.start = start;
        this.end = end;
    }

    //@XmlJavaTypeAdapter(LocalDateAdapter.class)
    @XmlAttribute(name = "start")
    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    //@XmlJavaTypeAdapter(LocalDateAdapter.class)
    @XmlAttribute(name = "end")
    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }
}
