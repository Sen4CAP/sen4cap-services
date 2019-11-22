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
package org.esa.sen4cap.objectstorage.scheduling;

import org.esa.sen2agri.commons.Config;
import org.esa.sen2agri.entities.DataSourceConfiguration;
import org.esa.sen2agri.entities.HighLevelProduct;
import org.esa.sen2agri.entities.Site;
import org.esa.sen2agri.scheduling.AbstractJob;
import org.esa.sen2agri.scheduling.JobDescriptor;
import org.esa.sen4cap.objectstorage.services.ObjectStorageService;
import org.esa.sen4cap.objectstorage.services.internal.ObjectStorageServiceImpl;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author Cosmin Cara
 */
public class MoveToOSJob extends AbstractJob {

    private final ObjectStorageService objectStorageService;

    public MoveToOSJob() {
        super();
        this.objectStorageService = new ObjectStorageServiceImpl();
        this.id = "Move";
    }

    @Override
    public String configKey() { return ConfigurationKeys.OBJECT_STORAGE_MOVE_ENABLED; }

    @Override
    public String groupName() { return "ObjectStorage"; }

    @Override
    public boolean isSingleInstance() { return true; }

    @Override
    public JobDescriptor createDescriptor(int rateInMinutes) {
        return new JobDescriptor()
                .setName(getId())
                .setGroup(groupName())
                .setFireTime(LocalDateTime.now().plusSeconds(5))
                .setRate(15);
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        super.execute(jobExecutionContext);
        SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void executeImpl(JobDataMap dataMap) {
        //List<Site> sites = (List<Site>) dataMap.get("sites");
        Site site = (Site) dataMap.get("site");
        DataSourceConfiguration cfg = (DataSourceConfiguration) dataMap.get("downloadConfig");
        if (!site.isEnabled()) {
            return;
        }
        if (!Config.isFeatureEnabled(site.getId(), configKey())) {
            logger.info(String.format(MESSAGE, site.getShortName(), cfg.getSatellite().name(),
                                      "Move to ObjectStorage disabled"));
            return;
        }
        Set<Integer> productTypeIds = new HashSet<>();
        Collections.addAll(productTypeIds,
                           Config.getSettingValues(ConfigurationKeys.OBJECT_STORAGE_MOVE_PRODUCT_TYPES,
                                                   new Integer[0]));
        if (!productTypeIds.isEmpty()) {
            final List<HighLevelProduct> products = this.persistenceManager.getMovableProducts(site,
                                                                                               productTypeIds);
            logger.fine(String.format("Found %s products eligible to move to Object Storage (site %s)",
                                      products.size(),
                                      site.getShortName()));
            final boolean deleteAfterMove = Config.getAsBoolean(ConfigurationKeys.OBJECT_STORAGE_MOVE_DELETE_AFTER, false);
            for (HighLevelProduct product : products) {
                Config.getWorkerFor(cfg).submit(() -> {
                    logger.fine(String.format(MESSAGE, site.getShortName(), cfg.getSatellite().name(),
                                              String.format("%s product '%s' to ObjectStorage",
                                                            deleteAfterMove ? "Moving" : "Copying",
                                                            product.getProductName())));
                    if (deleteAfterMove) {
                        moveProduct(site, product, this::onCompleted);
                    } else {
                        copyProduct(site, product, this::onCompleted);
                    }
                });
            }
        } else {
            logger.info(String.format(MESSAGE, site.getShortName(), cfg.getSatellite().name(),
                                      "No product types defined for moving to Object Storage"));
        }
    }

    private void copyProduct(Site site, HighLevelProduct product, Consumer<HighLevelProduct> callback) {
        try {
            this.objectStorageService.copyProduct(site, product, null);
            if (callback != null) {
                callback.accept(product);
            }
        } catch (IOException ex) {
            logger.severe(String.format("Failed to copy product '%s': %s", product.getProductName(), ex.getMessage()));
        }
    }

    private void moveProduct(Site site, HighLevelProduct product, Consumer<HighLevelProduct> callback) {
        try {
            this.objectStorageService.moveProduct(site, product, null);
            if (callback != null) {
                callback.accept(product);
            }
        } catch (IOException ex) {
            logger.severe(String.format("Failed to move product '%s': %s", product.getProductName(), ex.getMessage()));
        }
    }

    private void onCompleted(HighLevelProduct product) {
        product.setArchived(true);
        this.persistenceManager.setArchived(product);
    }
}
