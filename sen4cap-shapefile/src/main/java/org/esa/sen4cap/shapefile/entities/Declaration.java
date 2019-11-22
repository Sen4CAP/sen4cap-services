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

/**
 * @author Cosmin Cara
 */
public class Declaration extends VectorRecord {
    private String lpisId;
    private String parcelId;

    public Declaration() { }

    public String getLpisId() {
        return lpisId;
    }
    public void setLpisId(String lpisId) {
        this.lpisId = lpisId;
    }

    public String getParcelId() {
        return parcelId;
    }
    public void setParcelId(String parcelId) {
        this.parcelId = parcelId;
    }

}
