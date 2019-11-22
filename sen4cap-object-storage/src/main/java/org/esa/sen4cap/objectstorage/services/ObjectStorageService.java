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

package org.esa.sen4cap.objectstorage.services;

import org.esa.sen2agri.entities.HighLevelProduct;
import org.esa.sen2agri.entities.Site;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * @author Cosmin Cara
 */
public interface ObjectStorageService {
    int delete(String containerName, String keyFilter) throws IOException;
    void copyProduct(Site site, HighLevelProduct product, Consumer<HighLevelProduct> callback) throws IOException;
    void moveProduct(Site site, HighLevelProduct product, Consumer<HighLevelProduct> callback) throws IOException;
}
