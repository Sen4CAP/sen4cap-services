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
import org.esa.sen2agri.entities.*;
import org.esa.sen2agri.entities.enums.ActivityStatus;
import org.esa.sen2agri.entities.enums.Status;
import org.esa.sen2agri.services.JobHelper;
import org.esa.sen4cap.preprocessing.scheduling.ConfigurationKeys;
import org.json.simple.JSONObject;
import org.locationtech.jts.geom.Geometry;
import ro.cs.tao.products.sentinels.Sentinel1ProductHelper;
import ro.cs.tao.products.sentinels.SentinelProductHelper;
import ro.cs.tao.utils.FileUtilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Sentinel1L2Processor implements BiConsumer<HighLevelProduct, Boolean> {
    private final PersistenceManager persistenceManager;
    private final Logger logger;
    private final Map<String, Integer> computeFlags;
    private final ProcessorRuntimeConfiguration cfg;

    public Sentinel1L2Processor(PersistenceManager persistenceManager) {
        this.persistenceManager = persistenceManager;
        this.logger = Logger.getLogger(Sentinel1L2Processor.class.getName());
        this.computeFlags = new HashMap<>();
        cfg = ProcessorRuntimeConfiguration.get();
    }

    public void processAcquisition(DownloadProduct product, MasterChoice master,
                                   Set<Polarisation> enabledPolarisations, Site site, Path targetRoot, boolean overwrite) {
        Status status;
        String error = null;
        final String productName = product.getProductName();
        try {
            if (product.getFullPath().endsWith("$value")) {
                logger.severe(String.format("Product %s was selected for processing (status %s), but its path [%s] looks like it has not been downloaded. It will be skipped.",
                                            productName, product.getStatusId().name(), product.getFullPath()));
                updateProductFields(product, Status.FAILED, "Product not downloaded successfully");
            } else {
                updateProductFields(product, Status.PROCESSING, null);
                List<DownloadProduct> intersectingProducts = getIntersectingProducts(product, cfg.daysBack());
                int flags;
                if (intersectingProducts.size() == 0) {
                    intersectingProducts = getIntersectingProducts(product, 2 * cfg.daysBack());
                    flags = ProcessFlag.AMPLITUDE;
                } else {
                    flags = ProcessFlag.AMPLITUDE | ProcessFlag.COHERENCE;
                }
                if (overwrite | cfg.overwriteExistingProducts()) {
                    flags |= ProcessFlag.OVERWRITE;
                }
                this.computeFlags.put(productName, flags);
                logger.fine(String.format("Sentinel-1 Level2 processor received flags: AMPLITUDE=%s, COHERENCE=%s, OVERWRITE=%s",
                                          ProcessFlag.isSet(flags, ProcessFlag.AMPLITUDE),
                                          ProcessFlag.isSet(flags, ProcessFlag.COHERENCE),
                                          ProcessFlag.isSet(flags, ProcessFlag.OVERWRITE)));
                if (intersectingProducts.size() == 0) {
                    logger.info(String.format("No previous products for %s, skipping", productName));
                    updateProductFields(product, Status.DOWNLOADED, "No previous product");
                } else {
                    if (!alreadyProcessed(product, intersectingProducts.size(), enabledPolarisations.size())) {
                        status = Status.PROCESSING;
                        DownloadProduct masterProduct, slaveProduct;
                        for (int i = 0; i < intersectingProducts.size(); i++) {
                            DownloadProduct iProduct = intersectingProducts.get(i);
                            if (iProduct.getFullPath().endsWith("$value")) {
                                logger.severe(String.format("Product %s (status %s) was chosen as intersecting product %s, but its path [%s] looks like it has not been downloaded. It will be skipped.",
                                                            iProduct.getProductName(), iProduct.getStatusId().name(), productName, iProduct.getFullPath()));
                                status = Status.PROCESSING_FAILED;
                                break;
                            }
                            String fullPath = iProduct.getFullPath();
                            if (fullPath == null) {
                                logger.severe(String.format("Product [%s].fullPath", productName));
                                return;
                            } else if (!FileUtilities.isPathAccessible(Paths.get(fullPath))) {
                                logger.severe(String.format("Product '%s' is not readable from path '%s'",
                                                            productName, fullPath));
                                return;
                            }
                            DownloadProduct[] masterSlave = chooseMasterSlave(master, product, iProduct);
                            if (masterSlave == null) {
                                status = Status.PROCESSING_FAILED;
                                break;
                            } else {
                                masterProduct = masterSlave[0];
                                slaveProduct = masterSlave[1];
                            }
                            if (cfg.shouldeplaceLinks()) {
                                try {
                                    copyLocally(Paths.get(masterProduct.getFullPath()));
                                    copyLocally(Paths.get(slaveProduct.getFullPath()));
                                } catch (IOException ex) {
                                    logger.severe("Cannot bring locally the master product and/or the slave product. Reason: " +
                                                          ex.getMessage());
                                    try {
                                        restoreLink(Paths.get(masterProduct.getFullPath()));
                                        restoreLink(Paths.get(slaveProduct.getFullPath()));
                                    } catch (IOException ex2) {
                                        logger.severe("Cannot restore links. Reason: " + ex.getMessage());
                                        status = Status.PROCESSING_FAILED;
                                        continue;
                                    }
                                }
                            }
                            String masterZipPath = ensureUncompressed(masterProduct, site);
                            String slaveZipPath = ensureUncompressed(slaveProduct, site);
                            try {
                                for (Polarisation polarisation : enabledPolarisations) {
                                    try {
                                        int flag = this.computeFlags.get(productName);
                                        // If multiple intersections:
                                        // Amplitude (if enabled) is computed only for the first pair
                                        // Coherence (if enabled) is computed for all pairs => AMPLITUDE flag is reset
                                        flag = (i == 0 ? flag : ProcessFlag.resetBit(flag, ProcessFlag.AMPLITUDE));
                                        String message = processProductPair(site, masterProduct, slaveProduct,
                                                                            targetRoot, polarisation,
                                                                            product.equals(masterProduct) || productName.startsWith(master.name()),
                                                                            flag);
                                        if (message != null) {
                                            status = Status.PROCESSING_FAILED;
                                            error = message;
                                        } else {
                                            status = Status.PROCESSED;
                                        }
                                    } catch (Exception e) {
                                        error = String.format("Failed to process pair [%s,%s]. Reason: %s",
                                                              productName, iProduct.getProductName(),
                                                              e.getMessage());

                                        status = Status.PROCESSING_FAILED;
                                    }
                                    if (status == Status.PROCESSING_FAILED) {
                                        break;
                                    }
                                }
                            } finally {
                                cleanup(masterProduct, masterZipPath);
                                cleanup(slaveProduct, slaveZipPath);
                            }
                            if (cfg.shouldeplaceLinks()) {
                                try {
                                    replaceWithLink(Paths.get(masterProduct.getFullPath()));
                                    replaceWithLink(Paths.get(slaveProduct.getFullPath()));
                                } catch (IOException ex) {
                                    logger.severe(String.format("Cannot replace local folder with symlink. Reason: %s",
                                                                ex.getMessage()));
                                }
                            }
                            if ((this.computeFlags.get(productName) & ProcessFlag.COHERENCE) == 0) {
                                break;
                            } else {
                                logger.fine(String.format("%d intersections remaining to process for product %s",
                                                          intersectingProducts.size() - i + 1, productName));
                            }
                        }
                        updateProductFields(product, status, error);
                    } else {
                        updateProductFields(product, Status.PROCESSED, error);
                    }
                }
            }
        } finally {
            this.computeFlags.remove(productName);
        }
    }

    private String processProductPair(Site site, DownloadProduct masterProduct, DownloadProduct slaveProduct,
                                      Path targetRoot, Polarisation polarisation,
                                      boolean useMasterBand, int processFlag) {
        final JSONObject json = new JSONObject();
        json.put("site", site.getId());
        json.put("master", masterProduct.getFullPath());
        json.put("slave", slaveProduct.getFullPath());
        json.put("polarisation", polarisation.name());
        json.put("amplitude", ProcessFlag.isSet(processFlag, ProcessFlag.AMPLITUDE));
        json.put("coherence", ProcessFlag.isSet(processFlag, ProcessFlag.COHERENCE));
        json.put("overwriteExisting", ProcessFlag.isSet(processFlag, ProcessFlag.OVERWRITE));
        final Job job = JobHelper.createJob(site, cfg.processor(), json.toString());
        String error = null;
        try {
            Sentinel1Level2Worker processor = new Sentinel1Level2Worker(cfg, targetRoot);
            processor.setCompletionCallback(this);
            int dldProductId = masterProduct.getProductDate().compareTo(slaveProduct.getProductDate()) >= 0 ?
                    masterProduct.getId() : slaveProduct.getId();
            String message = processor.createProducts(job, dldProductId, site.getId(), polarisation, masterProduct.getOrbitType(),
                                                      Paths.get(masterProduct.getFullPath()),
                                                      Paths.get(slaveProduct.getFullPath()),
                                                      useMasterBand, processFlag, cfg.stepTimeout());
            if (message != null) {
                error = String.format("Intersecting product: %s, polarisation: %s, error: %s",
                                      slaveProduct.getProductName(), polarisation.name(), message);
                JobHelper.update(job, ActivityStatus.ERROR);
            } else {
                JobHelper.update(job, ActivityStatus.FINISHED);
            }
        } catch (Exception ex) {
            logger.severe(String.format("Failed to process pair {%s, %s}. Reason: %s",
                                        masterProduct.getProductName(), slaveProduct.getProductName(),
                                        ex.getMessage()));
            error = String.format("Intersecting product: %s, polarisation: %s, error: %s",
                                  slaveProduct.getProductName(), polarisation.name(), ex.getMessage());
            JobHelper.update(job, ActivityStatus.ERROR);
        }
        return error;
    }

    @Override
    public void accept(HighLevelProduct product, Boolean status) {
        final String name = product.getProductName();
        if (status) {
            try {
                product = this.persistenceManager.save(product);
                ProductDetails details;
                if ((details = product.getProductDetails()) != null) {
                    details.setId(product.getId());
                    this.persistenceManager.save(details);
                }
            } catch (Exception e) {
                logger.severe(String.format("Cannot persist product %s. Reason: %s",
                                           name, e.getMessage()));
            }
            logger.info(String.format("Successfully created product %s", name));
        } else {
            logger.severe(String.format("Failed to create product %s", name));
        }
    }

    private DownloadProduct[] chooseMasterSlave(MasterChoice config, DownloadProduct product1, DownloadProduct product2) {
        final boolean isProductS1A = isS1A(product1);
        final boolean isIProductS1A = isS1A(product2);
        final boolean coherenceEnabled = (this.computeFlags.get(product1.getProductName()) & ProcessFlag.COHERENCE) > 0;
        if (coherenceEnabled && isProductS1A && isIProductS1A) {
            logger.warning(String.format("Products must come from different sensors [found %s, %s]",
                                         product1.getProductName(), product2.getProductName()));
            return null;
        }
        final DownloadProduct masterProduct, slaveProduct;
        if (coherenceEnabled) {
            switch (config) {
                case S1A:
                    masterProduct = isProductS1A ? product1 : product2;
                    slaveProduct = isProductS1A ? product2 : product1;
                    break;
                case S1B:
                    masterProduct = isProductS1A ? product2 : product1;
                    slaveProduct = isProductS1A ? product1 : product2;
                    break;
                case OLDEST:
                    masterProduct = product2;
                    slaveProduct = product1;
                    break;
                case NEWEST:
                default:
                    masterProduct = product1;
                    slaveProduct = product2;
            }
        } else {
            masterProduct = product2;
            slaveProduct = product1;
        }
        return new DownloadProduct[] { masterProduct, slaveProduct };
    }

    private List<DownloadProduct> getIntersectingProducts(DownloadProduct product, int daysBack) {
        List<DownloadProduct> results;
        String productName = product.getProductName();
        results = this.persistenceManager.getIntersectingProducts(productName, daysBack, cfg.intersectionThreshold());
        if (results != null && results.size() > 0) {
            Sentinel1ProductHelper helper = (Sentinel1ProductHelper) SentinelProductHelper.create(productName);
            final String orbit = helper.getOrbit();
            results = results.stream() .filter(p -> orbit.equals(SentinelProductHelper.create(productName).getOrbit()))
                    .collect(Collectors.toList());
            Geometry productFootprint = product.getFootprint();
            results.sort(Collections.reverseOrder(Comparator.comparingDouble(o -> o.getFootprint().intersection(productFootprint).getArea())));
        }
        return results;
    }

    private boolean isZipped(DownloadProduct product) {
        return (".zip".equalsIgnoreCase(FileUtilities.getExtension(product.getFullPath())));
    }

    private String ensureUncompressed(DownloadProduct product, Site site) {
        String zipFilePath = null;
        try {
            final Path downloadPath = Paths.get(Config.getSetting(ConfigurationKeys.SENTINEL1_DOWNLOAD_DIRECTORY,
                                                                  "/mnt/archive/dwn_def/s1/default")).resolve(site.getShortName());
            if (isZipped(product)) {
                zipFilePath = product.getFullPath();
            }
            if (zipFilePath != null) {
                logger.finest(String.format("Product %s is zipped, will unzip", product.getProductName()));
                final Path zipPath = Paths.get(product.getFullPath());
                //FileUtilities.unzip(zipPath, zipPath.getParent(), true);
                FileUtilities.unzip(zipPath, downloadPath, true);
                //masterProduct.setFullPath(zipPath.getParent().resolve(masterProduct.getProductName()).toString());
                product.setFullPath(downloadPath.resolve(product.getProductName()).toString());
            } else if (cfg.copyProductsLocally()) {
                logger.finest(String.format("Product %s will be temporarily be copied locally", product.getProductName()));
                int copied = FileUtilities.copy(Paths.get(product.getFullPath()), downloadPath);
                logger.finest(String.format("Copied %d files locally", copied));
                zipFilePath = product.getFullPath();
                product.setFullPath(downloadPath.resolve(product.getProductName()).toString());
            }
        } catch (Exception ex) {
            logger.severe(String.format("Cannot uncompress/copy the product %s. Reason: %s", product.getProductName(), ex.getMessage()));
        }
        return zipFilePath;
    }

    private void cleanup(DownloadProduct product, String zipPath) {
        Path target = null;
        try {
            if (product != null && zipPath != null) {
                target = Paths.get(product.getFullPath()).getParent();
                target = target.resolve(product.getProductName());
                FileUtilities.deleteTree(target);
                product.setFullPath(zipPath);
            }
        } catch (IOException e) {
            logger.severe(String.format("Cannot cleanup folder %s. Reason: %s",
                                        target, e.getMessage()));
        }
    }

    private boolean alreadyProcessed(DownloadProduct product, int intersections, int polarisations) {
        List<HighLevelProduct> producedProducts = persistenceManager.getProducedProducts(product.getId());
        if (producedProducts == null || producedProducts.size() == 0) {
            return false;
        }
        int expected = (intersections + 1) * polarisations;
        if (producedProducts.size() < expected) {
            logger.warning(String.format("Product %s was previously processed, but the expected results count is not good " +
                                                 "[found:%d {ids: %s}; expected: %d]",
                                         product.getProductName(),
                                         producedProducts.size(),
                                         String.join(",", producedProducts.stream()
                                                 .map(p -> String.valueOf(p.getId()))
                                                 .collect(Collectors.toSet())),
                                         expected));
            return false;
        }
        return true;
    }

    private void updateProductFields(DownloadProduct product, Status status, String reason) {
        product.setStatusId(status);
        if (status == Status.PROCESSED) {
            product.setNbRetries((short) 0);
            product.setStatusReason(null);
        } else if (status == Status.PROCESSING_FAILED){
            product.setNbRetries((short) (product.getNbRetries() + 1));
        }
        if (reason != null) {
            String prdStatusReason = product.getStatusReason();
            if (prdStatusReason == null || prdStatusReason.isEmpty()) {
                prdStatusReason = String.format("[%s] %s;",
                                                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                                                reason);
            } else {
                prdStatusReason += String.format("[%s] %s;",
                                                 LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                                                 reason);
            }
            prdStatusReason = prdStatusReason.replace('\0',' ');
            String[] messages;
            if ((messages = prdStatusReason.split(";")).length > 3) {
                prdStatusReason = String.join(";", Arrays.copyOfRange(messages, 1, messages.length));
            }
            product.setStatusReason(prdStatusReason);
            logger.warning(String.format("Product %s was not processed [%s]",
                                         product.getProductName(), prdStatusReason));
        }
        persistenceManager.save(product);
    }

    private void copyLocally(Path localLink) throws IOException {
        if (localLink == null) {
            throw new IOException("Invalid symlink");
        } else if (!Files.isSymbolicLink(localLink)) {
            throw new IOException(String.format("%s is not a symlink", localLink));
        } else if (!Files.exists(localLink.toRealPath())) {
            throw new IOException(String.format("Target from symlink %s not found or not accessible",
                                                localLink));
        }
        Path parentFolder = localLink.getParent();
        Path newLink = parentFolder.resolve(localLink.getFileName().toString() + ".bak");
        logger.finest(String.format("Backup link %s to %s", localLink, newLink));
        Files.move(localLink, newLink);
        logger.finest(String.format("Resolving content of link %s to %s",
                                    localLink, parentFolder));
        Instant start = Instant.now();
        logger.finest(String.format("Copied %d files from %s to %s in %s",
                                    FileUtilities.copy(newLink.toRealPath(), parentFolder),
                                    newLink.toRealPath(),
                                    parentFolder.resolve(localLink.getFileName()),
                                    Duration.between(start, Instant.now())));
        parentFolder.resolve(localLink.getFileName());
    }

    private void replaceWithLink(Path localFolder) throws IOException {
        if (localFolder == null) {
            throw new IOException("Invalid folder");
        } else if (!Files.isDirectory(localFolder)) {
            throw new IOException(String.format("%s is not a folder", localFolder));
        } else if (!Files.exists(localFolder)) {
            throw new IOException(String.format("Folder %s not found", localFolder));
        }
        Path link = Paths.get(localFolder.toString() + ".bak");
        if (Files.exists(link)) {
            logger.finest(String.format("Deleting folder %s", localFolder));
            FileUtilities.deleteTree(localFolder);
            restoreLink(localFolder);
        } else {
            logger.warning(String.format("Link %s not found", link));
        }
    }

    private void restoreLink(Path original) throws IOException {
        logger.finest(String.format("Restoring %s to %s",
                                    Paths.get(original.toString() + ".bak"),
                                    original));
        Files.move(Paths.get(original.toString() + ".bak"), original);
    }

    private boolean isS1A(DownloadProduct product) { return product.getProductName().startsWith("S1A"); }
}
