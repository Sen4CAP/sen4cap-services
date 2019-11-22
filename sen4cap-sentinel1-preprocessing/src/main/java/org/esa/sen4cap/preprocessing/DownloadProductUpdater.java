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

import org.esa.sen2agri.commons.Config;
import org.esa.sen2agri.db.PersistenceManager;
import org.esa.sen2agri.entities.DownloadProduct;
import ro.cs.tao.eodata.DataHandlingException;
import ro.cs.tao.eodata.OutputDataHandler;

import java.util.List;
import java.util.logging.Logger;

public class DownloadProductUpdater implements OutputDataHandler<DownloadProduct> {
    private final PersistenceManager persistenceManager = Config.getPersistenceManager();
    private final Logger logger = Logger.getLogger(getClass().getName());

    public DownloadProductUpdater() { }

    @Override
    public Class<DownloadProduct> isIntendedFor() { return DownloadProduct.class; }

    @Override
    public int getPriority() { return 0; }

    @Override
    public List<DownloadProduct> handle(List<DownloadProduct> list) throws DataHandlingException {
        if (list == null) {
            return null;
        }
        DownloadProduct existing;
        for (DownloadProduct product : list) {
            try {
                existing = this.persistenceManager.getProductByName(product.getSiteId(), product.getProductName());
                String currentStatus = existing.getStatusReason();
                currentStatus = currentStatus != null && !currentStatus.isEmpty() ? currentStatus : null;
                product.setStatusReason(currentStatus);
                product = this.persistenceManager.save(product);
                logger.fine(String.format("Product updated [name=%s, status=%s, reason=%s]",
                                          product.getProductName(), product.getStatusId().name(), product.getStatusReason()));
            } catch (Exception ex) {
                logger.severe(String.format("Error updating product %s. Reason: %s",
                                            product.getProductName(), ex.getMessage()));
            }
        }
        return list;
    }
}
