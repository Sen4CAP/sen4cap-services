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

package org.esa.sen4cap.shapefile.db;

import java.util.Arrays;

/**
 * @author Cosmin Cara
 */
public enum DataType {
    LPIS("lpis"),
    DECLARATION("declaration");

    private final String baseName;

    DataType(String baseName) { this.baseName = baseName; }

    public String baseName() { return baseName; }

    public static DataType fromBaseName(String name) {
        return Arrays.stream(values()).filter(v -> v.baseName.equals(name)).findFirst().orElse(null);
    }
}
