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

package org.esa.sen4cap.preprocessing.web;

import org.esa.sen2agri.commons.Config;
import org.esa.sen2agri.db.PersistenceManager;
import org.esa.sen2agri.entities.DownloadProduct;
import org.esa.sen2agri.entities.HighLevelProduct;
import org.esa.sen2agri.entities.Site;
import org.esa.sen2agri.entities.enums.OrbitType;
import org.esa.sen2agri.entities.enums.Satellite;
import org.esa.sen2agri.services.DownloadService;
import org.esa.sen2agri.services.SiteHelper;
import org.esa.sen4cap.entities.enums.ProductType;
import org.esa.sen4cap.preprocessing.Configuration;
import org.esa.sen4cap.preprocessing.MasterChoice;
import org.esa.sen4cap.preprocessing.Polarisation;
import org.esa.sen4cap.preprocessing.Sentinel1L2Processor;
import org.esa.sen4cap.preprocessing.scheduling.ConfigurationKeys;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import ro.cs.tao.eodata.metadata.DecodeStatus;
import ro.cs.tao.eodata.metadata.MetadataInspector;
import ro.cs.tao.serialization.GeometryAdapter;
import ro.cs.tao.services.commons.ControllerBase;
import ro.cs.tao.spi.ServiceRegistryManager;
import ro.cs.tao.utils.FileUtilities;
import ro.cs.tao.utils.async.Parallel;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Cosmin Cara
 */
@Controller
@RequestMapping("/s1l2")
public class Sentinel1L2ProcessorController extends ControllerBase {

    private static final Map<Pattern, ProductType> folderPatterns = new HashMap<Pattern, ProductType>() {{
        put(Pattern.compile("(S2[A-B])_MSIL2A_(\\d{8}T\\d{6})_(N\\d{4})_R(\\d{3})_T(\\d{2}\\w{3})_(\\d{8}T\\d{6})(?:.SAFE)?"), ProductType.L2A);
        put(Pattern.compile("S2([AB])_MSIL2A_(\\d{8}T\\d{6})_(N\\d{4})_R(\\d{3})_T(\\d{2}\\w{3})_(\\d{8}T\\d{6})(?:.SAFE)?"), ProductType.L2A);
        put(Pattern.compile("LC08_L2A_(\\d{6})_(\\d{8})_(\\d{8})_(\\d{2})_(T[12]|RT)"), ProductType.L2A);
        put(Pattern.compile("(SEN4CAP_L2A_S\\d{1,3}_V)(\\d{8}T\\d{6})_(\\d{8}T\\d{6})_V[HV]_(\\d{3})"), ProductType.L2A_AMP);
    }};
    private static final Pattern S1L2FilePattern = Pattern.compile("(SEN4CAP_L2A_S\\d{1,3}_V)(\\d{8}T\\d{6})_(\\d{8}T\\d{6})_V[HV]_(\\d{3})_(AMP|COHE).tif");

    @Autowired
    private DownloadService downloadService;

    @Autowired
    private SiteHelper siteHelper;

    private final Set<Integer> tasks = new HashSet<>();

    /**
     * Returns the actual configuration values for the Sentinel-1 L2 processor
     */
    @RequestMapping(value = "/config", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> getConfiguration() {
        try {
            return prepareResult(Configuration.getActualSettings());
        } catch (Exception e) {
            return handleException(e);
        }
    }

    /**
     * Returns the metadata of the product specified by the given path, provided a MetadataInspector is found
     * for the identified product.
     * @param path  The path of the product
     */
    @RequestMapping(value = "/", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> inspectProduct(@RequestParam("path") String path) {
        try {
            Path productPath = Paths.get(URLDecoder.decode(path, StandardCharsets.UTF_8.toString()));
            Set<MetadataInspector> services = ServiceRegistryManager.getInstance().getServiceRegistry(MetadataInspector.class)
                    .getServices();
            MetadataInspector metadataInspector =
                    services.stream()
                            .filter(i -> DecodeStatus.INTENDED == i.decodeQualification(productPath))
                            .findFirst()
                            .orElse(services.stream()
                                            .filter(i -> DecodeStatus.SUITABLE == i.decodeQualification(productPath))
                                            .findFirst()
                                            .orElse(null));
            MetadataInspector.Metadata metadata = null;
            if (metadataInspector != null) {
                metadata = metadataInspector.getMetadata(productPath);
            }
            return new ResponseEntity<>(metadata, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.OK);
        }
    }

    /**
     * Triggers the reprocessing of a Sentinel-1 L1 SLC acquisition, belonging to a site, for the given polarisations.
     * @param siteCode          The site short name
     * @param productName       The acquisition name
     * @param polarisationList  The polarisation(s) for which to do the reprocessing. Defaults to VV,VH.
     */
    @RequestMapping(value = "/reprocess", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> processProducts(@RequestParam("siteCode") String siteCode,
                                             @RequestParam("product") String productName,
                                             @RequestParam(name = "polarisation", required = false, defaultValue = "VV,VH") String polarisationList) {
        try {
            final PersistenceManager persistenceManager = Config.getPersistenceManager();
            final Site site = persistenceManager.getSiteByShortName(siteCode);
            if (site == null) {
                throw new IllegalArgumentException("Site not found. Maybe id was passed instead of short name?");
            }
            final DownloadProduct product = persistenceManager.getProductByName(site.getId(), productName);
            if (product == null) {
                throw new IllegalArgumentException(String.format("Product %s not found for site '%s'", productName, siteCode));
            }
            final MasterChoice master = MasterChoice.valueOf(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_MASTER));
            final Path targetRoot = Paths.get(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_OUTPUT_PATH).replace("{site}", site.getShortName()));
            Files.createDirectories(targetRoot);
            String[] polarisations = polarisationList.split(",");
            final Set<Polarisation> enabledPolarisations = new HashSet<>();
            if (polarisations.length > 0) {
                for (String polarisation : polarisations) {
                    enabledPolarisations.add(Polarisation.valueOf(polarisation));
                }
            } else {
                Collections.addAll(enabledPolarisations, Polarisation.values());
            }
            final int taskHashCode = Objects.hash(site, product, enabledPolarisations);
            if (!tasks.contains(taskHashCode)) {
                tasks.add(taskHashCode);
                asyncExecute(() -> {
                    String message = null;
                    try {
                        Sentinel1L2Processor processor = new Sentinel1L2Processor(persistenceManager);
                        processor.processAcquisition(product, master, enabledPolarisations, site, targetRoot, true);
                    } catch (Exception ex) {
                        message = ex.getMessage();
                        error(message);
                    } finally {
                        taskCompleted(taskHashCode, message);
                    }
                });
            } else {
                return new ResponseEntity<>("Same project pair already submitted and in progress", HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            return handleException(e);
        }
        return new ResponseEntity<>("Request submitted for processing. Please check log for details.", HttpStatus.OK);
    }

    /**
     * Triggers importing into the system database the L2 products from the given folder.
     * Supported products are: S1 L2 (amplitude, coherence), S2 L2A (MACCS or MAJA format).
     * A subsequent request for the same folder and site will not be processed, unless the current operation completes.
     * @param siteCode  The site short name
     * @param folder    The folder from which to import the products
     */
    @RequestMapping(value = "/import/l2/{site}", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> importProducts(@PathVariable("site") String siteCode, @RequestParam("folder") String folder) {
        try {
            Site site = Config.getPersistenceManager().getSiteByShortName(siteCode);
            if (site == null) {
                throw new IllegalArgumentException("Site not found. Maybe id was passed instead of short name?");
            }
            Path sourcePath = Paths.get(folder);
            if (!Files.exists(sourcePath)) {
                throw new IOException(String.format("Folder %s not found or not accessible", folder));
            }
            final int hashCode = Objects.hash(siteCode, folder);
            if (!tasks.contains(hashCode)) {
                tasks.add(hashCode);
                asyncExecute(() -> {
                    String message = null;
                    try {
                        message = importProductsDelegate(site, sourcePath);
                    } catch (Exception ex) {
                        message = ex.getMessage();
                        error(message);
                    } finally {
                        taskCompleted(hashCode, message);
                    }
                });
            } else {
                return new ResponseEntity<>(String.format("Folder '%s' already submitted for site %s",
                                                          folder, siteCode),
                                            HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>("Request submitted. Please check log for details.", HttpStatus.OK);
    }

    private boolean pathMatches(String pathName) {
        return folderPatterns.keySet().stream().anyMatch(p -> p.matcher(pathName).matches());
    }

    private ProductType matchedProductType(String pathName) {
        return folderPatterns.entrySet().stream().filter(e -> e.getKey().matcher(pathName).matches()).findFirst().get().getValue();
    }

    private String importProductsDelegate(Site site, Path sourcePath) throws IOException {
        Set<MetadataInspector> services = ServiceRegistryManager.getInstance()
                                                                .getServiceRegistry(MetadataInspector.class)
                                                                .getServices();
        if (services == null) {
            throw new IOException("No metadata inspector found");
        }
        final Map<Path, ProductType> folders = Files.walk(sourcePath, 1)
                .filter(f -> Files.isDirectory(f) && pathMatches(f.getFileName().toString()))
                .collect(Collectors.toMap(Function.identity(), p -> matchedProductType(p.getFileName().toString())));
        final int size = folders.size();
        final AtomicInteger count = new AtomicInteger(0);
        info("Found %d products in folder %s", size, sourcePath);
        Parallel.ForEach(folders.entrySet(), (e) -> {
            final Path productFolder = e.getKey();
            try {
                switch (e.getValue()) {
                    case L2A:
                        importOpticalL2Product(productFolder, site);
                        break;
                    case L2A_AMP:
                    case L2A_COHE:
                        importSARL2Product(productFolder, site);
                        break;
                }
                count.incrementAndGet();
            } catch (Exception ex) {
                error(String.format("Error while processing folder %s. Message: %s", sourcePath, ex.getMessage()));
            }
        });
        return String.format("%d products imported into database", count.get());
    }

    private void importSARL2Product(Path productFolder, Site site) throws IOException {
        PersistenceManager persistenceManager = Config.getPersistenceManager();
        List<Path> files = Files.walk(productFolder, 1).collect(Collectors.toList());
        for (Path file : files) {
            Matcher matcher = S1L2FilePattern.matcher(file.getFileName().toString());
            if (matcher.matches()) {
                HighLevelProduct existing = null;
                String productName = FileUtilities.getFilenameWithoutExtension(file);
                try {
                    existing = persistenceManager.getHighLevelProductByName(site.getId(), productName);
                    if (existing != null) {
                        if (file.toString().equals(existing.getFullPath())) {
                            warn("Product %s already exists in database [id = %d]", productName, existing.getId());
                            continue;
                        } else {
                            warn("Product %s exists in database [id = %d] but with a different path [%s]. It will be updated.",
                                 productName, existing.getFullPath());
                        }
                    }
                } catch (Exception e) {
                    warn("Check for existing product %s failed", productName);
                }
                Set<MetadataInspector> services = ServiceRegistryManager.getInstance()
                        .getServiceRegistry(MetadataInspector.class)
                        .getServices();
                if (services == null) {
                    throw new IOException("No metadata inspector found");
                }
                MetadataInspector inspector = services.stream()
                        .filter(i -> DecodeStatus.INTENDED == i.decodeQualification(file))
                        .findFirst()
                        .orElse(services.stream()
                                        .filter(i -> DecodeStatus.SUITABLE == i.decodeQualification(file))
                                        .findFirst()
                                        .orElse(null));
                if (inspector == null) {
                    warn("No suitable metadata inspector found for product %s", file);
                    continue;
                }
                HighLevelProduct product = existing == null ? new HighLevelProduct() : existing;
                product.setProductName(FileUtilities.getFilenameWithoutExtension(file));
                product.setFullPath(file.toString());
                product.setArchived(false);
                product.setInserted(LocalDateTime.now());
                LocalDateTime first = LocalDateTime.parse(matcher.group(2),
                                                          DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss"));
                LocalDateTime second = LocalDateTime.parse(matcher.group(3),
                                                           DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss"));
                int relativeOrbit = Integer.parseInt(matcher.group(4));
                product.setProductType(Enum.valueOf(ProductType.class, "L2A_" + matcher.group(5)).value());
                product.setCreated(first.compareTo(second) > 0 ? first : second);
                product.setProcessorId(persistenceManager.getProcessor(ProductType.L2A_AMP.shortName()).getId());
                product.setSatellite(Satellite.Sentinel1);
                product.setSiteId(site.getId());
                product.setRelativeOrbit(relativeOrbit);
                product.setTiles(new String[]{String.valueOf(relativeOrbit)});
                try {
                    DownloadProduct downloadProduct =
                            persistenceManager.getProductByName(site.getId(),
                                                                "%" + product.getCreated()
                                                                        .format(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")));
                    if (downloadProduct != null) {
                        product.setDownloadProductId(downloadProduct.getId());
                    }
                } catch (Exception e) {
                    warn("[%s] Cannot determine originating product", product.getProductName());
                }
                try {
                    MetadataInspector.Metadata metadata = inspector.getMetadata(file);
                    if (metadata != null) {
                        product.setFootprint(new GeometryAdapter().marshal(metadata.getFootprint()));
                        Map<String, Double> statistics = metadata.getStatistics();
                        if (statistics != null && statistics.size() == 4) {
                            if (statistics.values().stream().allMatch(v -> Double.compare(v, 0.0) == 0)) {
                                warn("Product %s seems to have only NODATA (statistics are 0)",
                                     product.getProductName());
                            }
                        } else {
                            warn("Cannot determine statistics for product %s", product.getProductName());
                        }
                        product.setOrbitType(OrbitType.valueOf(metadata.getOrbitDirection().toString()));
                    } else {
                        throw new Exception("null metadata");
                    }
                } catch (Exception e) {
                    warn("Cannot determine footprint for product %s. Reason: %s", product.getProductName(), e.getMessage());
                    continue;
                }
                try {
                    persistenceManager.save(product);
                } catch (Exception e) {
                    error("Cannot persist product %s. Reason: %s", product.getProductName(), e.getMessage());
                }
            }
        }
    }

    private void importOpticalL2Product(Path productFolder, Site site) throws IOException {
        PersistenceManager persistenceManager = Config.getPersistenceManager();
        Set<MetadataInspector> services = ServiceRegistryManager.getInstance()
                .getServiceRegistry(MetadataInspector.class)
                .getServices();
        if (services == null) {
            throw new IOException("No metadata inspector found");
        }
        MetadataInspector inspector = services.stream()
                .filter(i -> DecodeStatus.INTENDED == i.decodeQualification(productFolder))
                .findFirst()
                .orElse(services.stream()
                                .filter(i -> DecodeStatus.SUITABLE == i.decodeQualification(productFolder))
                                .findFirst()
                                .orElse(null));
        if (inspector == null) {
            warn("No suitable metadata inspector found for product %s", productFolder);
            throw new IOException(String.format("No suitable metadata inspector found for product %s", productFolder));
        }
        MetadataInspector.Metadata metadata = inspector.getMetadata(productFolder);
        if (metadata != null) {
            HighLevelProduct existing = null;
            String productName = FileUtilities.getFilenameWithoutExtension(productFolder);
            try {
                existing = persistenceManager.getHighLevelProductByName(site.getId(), productName);
                if (existing != null) {
                    if (productFolder.toString().equals(existing.getFullPath())) {
                        warn("Product %s already exists in database [id = %d]", productName, existing.getId());
                        return;
                    } else {
                        warn("Product %s exists in database [id = %d] but with a different path [%s]. It will be updated.",
                             productName, existing.getFullPath());
                    }
                }
            } catch (Exception e) {
                warn("Check for existing product %s failed", productName);
            }

            HighLevelProduct product = existing == null ? new HighLevelProduct() : existing;
            product.setProductName(FileUtilities.getFilenameWithoutExtension(productFolder));
            product.setFullPath(productFolder.toString());
            product.setArchived(false);
            product.setInserted(LocalDateTime.now());
            product.setProductType(ProductType.L2A.value());
            product.setCreated(metadata.getAquisitionDate());
            product.setProcessorId(persistenceManager.getProcessor(ProductType.L2A_AMP.shortName()).getId());
            product.setSatellite(Satellite.Sentinel1);
            product.setSiteId(site.getId());
            //product.setRelativeOrbit(relativeOrbit);
            //product.setTiles(new String[]{String.valueOf(relativeOrbit)});
            try {
                DownloadProduct downloadProduct =
                        persistenceManager.getProductByName(site.getId(),
                                                            "%" + product.getCreated()
                                                                    .format(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")));
                if (downloadProduct != null) {
                    product.setDownloadProductId(downloadProduct.getId());
                }
            } catch (Exception e) {
                warn("[%s] Cannot determine originating product", product.getProductName());
            }
            try {
                product.setFootprint(new GeometryAdapter().marshal(metadata.getFootprint()));
            } catch (Exception e) {
                warn("Cannot determine footprint for product %s", product.getProductName());
                throw new IOException(e);
            }
            Map<String, Double> statistics = metadata.getStatistics();
            if (statistics != null && statistics.size() == 4) {
                if (statistics.values().stream().allMatch(v -> Double.compare(v, 0.0) == 0)) {
                    warn("Product %s seems to have only NODATA (statistics are 0)",
                         product.getProductName());
                }
            } else {
                warn("Cannot determine statistics for product %s", product.getProductName());
            }
            try {
                persistenceManager.save(product);
            } catch (Exception e) {
                error("Cannot persist product %s. Reason: %s", product.getProductName(), e.getMessage());
            }
        } else {
            throw new IOException("null metadata");
        }
    }

    private void taskCompleted(int taskCode, String message) {
        tasks.remove(taskCode);
        if (message == null) {
            info("Processing completed");
        } else {
            info("Processing completed with errors: " + message);
        }
    }

}
