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

package org.esa.sen4cap.preprocessing.scheduling;

import org.esa.sen2agri.commons.Config;
import org.esa.sen2agri.entities.DownloadProduct;
import org.esa.sen2agri.entities.Site;
import org.esa.sen2agri.entities.enums.Satellite;
import org.esa.sen2agri.entities.enums.Status;
import org.esa.sen2agri.scheduling.AbstractJob;
import org.esa.sen2agri.scheduling.JobDescriptor;
import org.esa.sen4cap.preprocessing.Configuration;
import org.esa.sen4cap.preprocessing.MasterChoice;
import org.esa.sen4cap.preprocessing.Polarisation;
import org.esa.sen4cap.preprocessing.Sentinel1L2Processor;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;
import ro.cs.tao.utils.FileUtilities;
import ro.cs.tao.utils.async.Parallel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Sentinel1Level2Job extends AbstractJob {
    private static final Set<String> runningJobs = Collections.synchronizedSet(new HashSet<>());
    private static final int daysOffset;
    private static final int parallelism;
    private static final String targetRootPath;
    private static final Set<Satellite> filter;

    static {
        targetRootPath = Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_OUTPUT_PATH);
        parallelism = Integer.parseInt(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_PARALLELISM));
        daysOffset = Integer.parseInt(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_DAYS_BACK));
        filter = new HashSet<>();
        filter.add(Satellite.Sentinel1);
    }

    private static Path getSiteRootPath(Site site) {
        return Paths.get(targetRootPath.replace("{site}", site.getShortName()));
    }

    public Sentinel1Level2Job() {
        super();
        this.id = "S1 Pre-Processing";
    }

    @Override
    public Set<Satellite> getSatelliteFilter() { return filter; }

    @Override
    public String configKey() { return ConfigurationKeys.S1_PROCESSOR_ENABLED; }

    @Override
    public String groupName() { return "S1-Preprocessing"; }

    @Override
    public JobDescriptor createDescriptor(int rateInMinutes) {
        return new JobDescriptor()
                .setName(getId())
                .setGroup(groupName())
                .setFireTime(LocalDateTime.now().plusSeconds(30))
                .setRate(Integer.parseInt(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_INTERVAL)));
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        super.execute(jobExecutionContext);
        SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
    }

    private void processSite(Site site) {
        final boolean latestFirst = Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_REVERSE_ACQUISITION_DATE));
        final List<DownloadProduct> products = this.persistenceManager.getDownloadedProducts(site.getId(),
                                                                                             Satellite.Sentinel1,
                                                                                             LocalDate.now().minusDays(Integer.parseInt(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_WAIT_FOR_ORBIT_FILES))),
                                                                                             latestFirst);
        // products that appear to be processed, but there are no L2 products issued from them
        final List<DownloadProduct> stalledProducts = this.persistenceManager.getStalledProducts(site.getId(), daysOffset);
        logger.info(String.format("Found %d stalled products", stalledProducts.size()));
        products.addAll(stalledProducts);

        // products that may have failed to be processed due to insufficient memory (maybe other concurrent processing)
        final List<DownloadProduct> recoverableFailedProducts = this.persistenceManager.getProducts(site.getId(),
                                                                                                    Satellite.Sentinel1.value(),
                                                                                                    Status.PROCESSING_FAILED,
                                                                                                    "Cannot construct DataBuffer");
        products.addAll(recoverableFailedProducts);
        products.sort(Comparator.comparing(DownloadProduct::getProductDate));
        logger.info(String.format("Found %d products eligible for pre-processing", products.size()));
        final Path targetRoot = getSiteRootPath(site);
        try {
            Files.createDirectories(targetRoot);
            if (!FileUtilities.isPathWriteable(targetRoot)) {
                throw new IOException(String.format("Folder '%s' is not writable. Job will exit.", targetRoot));
            }
            String tempPath = Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_WORK_DIR);
            if (tempPath != null && !tempPath.isEmpty()) {
                Path tmpPath = Paths.get(tempPath);
                Files.createDirectories(tmpPath);
                if (!FileUtilities.isPathWriteable(tmpPath)) {
                    throw new IOException(String.format("Folder '%s' is not writable. Job will exit.", tmpPath));
                }
            }
        } catch (IOException e) {
            logger.severe(String.format("Cannot create folder %s or folder doesn't have write permissions. Details: %s", targetRoot, e.getMessage()));
            return;
        }
        final String polarisationList = Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_POLARISATIONS);
        final String[] polarisations = polarisationList.split(";");
        final Set<Polarisation> enabledPolarisations = new HashSet<>();
        if (polarisations.length > 0) {
            for (String polarisation : polarisations) {
                enabledPolarisations.add(Polarisation.valueOf(polarisation));
            }
        } else {
            Collections.addAll(enabledPolarisations, Polarisation.values());
        }
        logger.fine(String.format("Sentinel-1 pre-processing enabled for polarisations %s",
                                  enabledPolarisations.stream().map(Enum::name).collect(Collectors.joining(","))));
        final MasterChoice master = MasterChoice.valueOf(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_MASTER));
        logger.fine(String.format("Sentinel-1 pre-processing will use %s products as master", master.name()));
        logger.fine(String.format("Execution parallelism set to %d", parallelism));
        final AtomicInteger idx = new AtomicInteger(products.size());
        if (parallelism == 1 || products.size() == 1) {
            for (DownloadProduct product : products) {
                try {
                    checkPath(product);
                    logger.info(String.format("Processing acquisition %s", product.getProductName()));
                    new Sentinel1L2Processor(persistenceManager).processAcquisition(product, master, enabledPolarisations, site, targetRoot, false);
                    logger.info(String.format("Remaining acquisitions to process: %d", idx.decrementAndGet()));
                } catch (Exception e) {
                    logger.severe(String.format("Cannot process acquisition '%s'. Reason: %s",
                                                product.getProductName(), e.getMessage()));
                }
            }
        } else {
            Parallel.ForEach(products,
                             parallelism,
                             p -> {
                                 try {
                                     checkPath(p);
                                     logger.info(String.format("Processing acquisition %s", p.getProductName()));
                                     new Sentinel1L2Processor(persistenceManager).processAcquisition(p, master, enabledPolarisations, site, targetRoot, false);
                                     logger.info(String.format("Remaining acquisitions to process: %d", idx.decrementAndGet()));
                                 } catch (Exception e) {
                                     logger.severe(String.format("Cannot process acquisition '%s'. Reason: %s",
                                                                 p.getProductName(), e.getMessage()));
                                 }
                             });
        }
    }

    @Override
    protected void executeImpl(JobDataMap jobDataMap) {
        Site site = (Site) jobDataMap.get("site");
        if (!Config.isFeatureEnabled(site.getId(), configKey())) {
            logger.info(String.format(MESSAGE, site.getShortName(), Satellite.Sentinel1.name(),
                                      "Sentinel-1 pre-processing disabled"));
            return;
        }
        if (runningJobs.contains(site.getName())) {
            logger.warning(String.format("A job is already running for the site '%s'", site.getName()));
            return;
        }
        runningJobs.add(site.getName());
        try {
            processSite(site);
        } catch (Exception e) {
            logger.severe(String.format("Exception occurred while processing site %s: %s", site.getName(), e.getMessage()));
        } finally {
            runningJobs.remove(site.getName());
        }
    }

    private void checkPath(DownloadProduct product) throws IOException {
        final String fullPath = product.getFullPath();
        if (fullPath == null) {
            throw new IOException(String.format("Product [%s].fullPath is NULL", product.getProductName()));
        }
        Path productLocalPath = Paths.get(fullPath);
        if (!FileUtilities.isPathAccessible(productLocalPath)) {
            // try to re-create dangling symlinks, if the case
            if (Files.isSymbolicLink(productLocalPath)) {
                Path linkedProductPath = Files.readSymbolicLink(productLocalPath);
                if (!FileUtilities.isPathAccessible(linkedProductPath)) {
                    throw new IOException(String.format("Product '%s' is not readable from path '%s'",
                                                        product.getProductName(), fullPath));
                } else {
                    // real path seems to be accessible, re-create the link
                    Files.delete(productLocalPath);
                    FileUtilities.link(linkedProductPath, productLocalPath);
                }
            } else {
                throw new IOException(String.format("Product '%s' is not readable from path '%s'",
                                                    product.getProductName(), fullPath));
            }
        }
    }
}
