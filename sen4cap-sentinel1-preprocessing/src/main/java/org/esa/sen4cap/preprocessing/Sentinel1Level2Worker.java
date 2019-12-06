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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.esa.sen2agri.commons.Config;
import org.esa.sen2agri.commons.ProcessingTopic;
import org.esa.sen2agri.entities.*;
import org.esa.sen2agri.entities.enums.ActivityStatus;
import org.esa.sen2agri.entities.enums.OrbitType;
import org.esa.sen2agri.entities.enums.Satellite;
import org.esa.sen2agri.services.JobHelper;
import org.esa.sen4cap.entities.enums.ProductType;
import org.esa.sen4cap.preprocessing.scheduling.ConfigurationKeys;
import org.locationtech.jts.geom.Geometry;
import ro.cs.tao.EnumUtils;
import ro.cs.tao.eodata.enums.PixelType;
import ro.cs.tao.eodata.metadata.DecodeStatus;
import ro.cs.tao.eodata.metadata.MetadataInspector;
import ro.cs.tao.execution.OutputDataHandlerManager;
import ro.cs.tao.messaging.Message;
import ro.cs.tao.messaging.Messaging;
import ro.cs.tao.products.sentinels.Sentinel1ProductHelper;
import ro.cs.tao.products.sentinels.SentinelProductHelper;
import ro.cs.tao.security.SystemPrincipal;
import ro.cs.tao.serialization.GeometryAdapter;
import ro.cs.tao.spi.ServiceRegistryManager;
import ro.cs.tao.utils.DateUtils;
import ro.cs.tao.utils.FileUtilities;
import ro.cs.tao.utils.FixedQueue;
import ro.cs.tao.utils.executors.*;
import ro.cs.tao.utils.executors.monitoring.ActivityListener;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.logging.Formatter;
import java.util.logging.*;
import java.util.stream.Collectors;

public class Sentinel1Level2Worker {
    private static final DateTimeFormatter productDateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");

    private static final Set<String> excludedExtensions;
    private static final Set<MetadataInspector> metadataServices;
    private static final String[] replaceTokens;
    private static final String[] cropReplaceTokens;
    private static final String[] otherReplacements;
    private static final String[] burstTokens;
    private static final Map<String, Boolean> stepDeletionStatus;
    private final ProcessorRuntimeConfiguration configuration;
    private final Path targetPath;
    private final Path temporaryPath;
    private BiConsumer<HighLevelProduct, Boolean> completionCallback;
    private final List<String> errors;
    private final Logger logger = Logger.getLogger(Sentinel1Level2Worker.class.getName());
    private final Map<ExecutionUnit, Step> dbJobSteps;

    static {
        excludedExtensions = new HashSet<String>() {{
            add(".mtd"); add(".nc"); add(".NC"); add(".tif"); add(".TIF"); add(".tiff"); add(".TIFF"); add(".log");
        }};
        StepCommandBuilder.initialize();
        metadataServices = ServiceRegistryManager.getInstance().getServiceRegistry(MetadataInspector.class).getServices();
        replaceTokens = new String[] {"$LOCATION", "$PRODUCT", "$EXTENSION", "$FORMAT", "$POLARISATION", "$PROJECTION", "$BAND_NAME", "$PS_METERS", "$PS_DEGREES" };
        burstTokens = new String[] { "$FP_IW1_LAST", "$FP_IW2_LAST", "$FP_IW3_LAST", "$SP_IW1_LAST", "$SP_IW2_LAST", "$SP_IW3_LAST" };
        cropReplaceTokens = new String[] { "$SUBSET_PARENT", "$WRITE_PARENT" };
        otherReplacements = new String[] { "$TMPFOLDER", "$masterProduct" };
        stepDeletionStatus = StepCommandBuilder.getStepDeletionStatus();
    }

    public static void init() { }

    Sentinel1Level2Worker(ProcessorRuntimeConfiguration configuration, Path path) {
        this.targetPath = path;
        this.configuration = configuration;
        logger.fine(this.configuration.toString());
        String configuredTmpPath = Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_WORK_DIR);
        this.temporaryPath = configuredTmpPath != null && !configuredTmpPath.isEmpty() ? Paths.get(configuredTmpPath) : targetPath;
        logger.fine(String.format("Sentinel-1 pre-processor will use '%s' as working directory", this.temporaryPath));
        this.errors = new ArrayList<>();
        this.dbJobSteps = new HashMap<>();
        ProductLog.initialize(logger);
    }

    void setCompletionCallback(BiConsumer<HighLevelProduct, Boolean> completionCallback) {
        this.completionCallback = completionCallback;
    }

    String createProducts(Job job, int downloadProductId, int siteId, Polarisation polarisation, OrbitType orbitType,
                          Path masterProduct, Path slaveProduct,
                          boolean useMasterBand, int flag, long timeoutMinutes) {
        this.errors.clear();
        Instant start = Instant.now();
        final MetadataBuilder metadataBuilder = new MetadataBuilder().withStartTime(start);
        final ProductFolderBuilder nameBuilder = new ProductFolderBuilder();
        Sentinel1ProductHelper helper = (Sentinel1ProductHelper) SentinelProductHelper.create(masterProduct.getFileName().toString());
        final String relativeOrbit = helper.getOrbit();
        String masterSensingDate = helper.getSensingDate();
        String acquisition = helper.getName();
        nameBuilder.withSiteId(siteId)
                    .withMasterDate(masterSensingDate)
                    .withPolarisation(polarisation)
                    .withRelativeOrbit(relativeOrbit);
        LocalDateTime hlpDate = LocalDateTime.parse(masterSensingDate, productDateFormatter);
        if (slaveProduct != null) {
            helper = (Sentinel1ProductHelper) SentinelProductHelper.create(slaveProduct.getFileName().toString());
            String slaveSensingDate = helper.getSensingDate();
            nameBuilder.withSlaveDate(slaveSensingDate);
            LocalDateTime slaveDate = LocalDateTime.parse(slaveSensingDate, productDateFormatter);
            if (hlpDate.compareTo(slaveDate) > 0) {
                metadataBuilder.withIntersectionOffset((int) Duration.between(slaveDate, hlpDate).toDays())
                        .withAcquisition(acquisition);
            } else {
                metadataBuilder.withIntersectionOffset((int) Duration.between(hlpDate, slaveDate).toDays())
                        .withAcquisition(helper.getName());
                hlpDate = slaveDate;
            }
        }
        String name = nameBuilder.build();
        metadataBuilder.withProductName(name)
                    .withProductType(ProductType.L2A_AMP)
                    .withPolarisation(polarisation)
                    .withPixelType(PixelType.FLOAT32)
                    .withProductFormat(configuration.outputFormat())
                    .withRelativeOrbit(Integer.parseInt(relativeOrbit))
                    .withOrbitType(orbitType)
                    .withMaster(masterProduct.getFileName().toString())
                    .withMasterDate(masterSensingDate)
                    .withAcquisitionDate(hlpDate);
        if (slaveProduct != null) {
            metadataBuilder.withSlave(slaveProduct.getFileName().toString())
                        .withSlaveDate(helper.getSensingDate());
        }
        boolean status = false;
        List<HighLevelProduct> products = new ArrayList<>();
        HighLevelProduct product;
        int processFlag = flag;
        Path path;
        if (configuration.amplitudeEnabled() & configuration.coherenceEnabled()) {
            if (ProcessFlag.isSet(processFlag, ProcessFlag.COHERENCE)) {
                product = new HighLevelProduct();
                product.setProductName(name + "_" + ProductType.L2A_COHE.shortName().replace("l2-", "").toUpperCase());
                product.setDownloadProductId(downloadProductId);
                product.setProductType(ProductType.L2A_COHE.value());
                path = this.targetPath.resolve(name).resolve(product.getProductName() + configuration.outputExtension());
                if (Files.exists(path) && ProcessFlag.isReset(processFlag, ProcessFlag.OVERWRITE)) {
                    processFlag = ProcessFlag.resetBit(processFlag, ProcessFlag.COHERENCE);
                    logger.warning(String.format("Product '%s' was found in '%s'. It will not be re-processed.",
                                                 product.getProductName(), path));
                } else {
                    if (Files.exists(path) && ProcessFlag.isSet(processFlag, ProcessFlag.OVERWRITE)) {
                        logger.fine(String.format("Product '%s' was found in '%s'. It will be overwritten.",
                                                  product.getProductName(), path));
                    }
                    products.add(product);
                }
            }
            if (ProcessFlag.isSet(processFlag, ProcessFlag.AMPLITUDE)) {
                product = new HighLevelProduct();
                product.setProductName(name + "_" + ProductType.L2A_AMP.shortName().replace("l2-", "").toUpperCase());
                product.setDownloadProductId(downloadProductId);
                product.setProductType(ProductType.L2A_AMP.value());
                path = this.targetPath.resolve(name).resolve(product.getProductName() + configuration.outputExtension());
                if (Files.exists(path) && ProcessFlag.isReset(processFlag, ProcessFlag.OVERWRITE)) {
                    processFlag = ProcessFlag.resetBit(processFlag, ProcessFlag.AMPLITUDE);
                    logger.warning(String.format("Product '%s' was found in '%s'. It will not be re-processed.",
                                                 product.getProductName(), path));
                } else {
                    if (Files.exists(path) && ProcessFlag.isSet(processFlag, ProcessFlag.OVERWRITE)) {
                        logger.fine(String.format("Product '%s' was found in '%s'. It will be overwritten.",
                                                  product.getProductName(), path));
                    }
                    products.add(product);
                }
            }
        } else {
            if (configuration.amplitudeEnabled() && ProcessFlag.isSet(processFlag, ProcessFlag.AMPLITUDE)) {
                product = new HighLevelProduct();
                product.setProductName(name);
                product.setDownloadProductId(downloadProductId);
                product.setProductType(ProductType.L2A_AMP.value());
                path = this.targetPath.resolve(name).resolve(product.getProductName() + configuration.outputExtension());
                if (Files.exists(path) && ProcessFlag.isReset(processFlag, ProcessFlag.OVERWRITE)) {
                    processFlag = ProcessFlag.resetBit(processFlag, ProcessFlag.AMPLITUDE);
                    logger.warning(String.format("Product '%s' was found in '%s'. It will not be re-processed.",
                                                 product.getProductName(), path));
                } else {
                    if (Files.exists(path) && ProcessFlag.isSet(processFlag, ProcessFlag.OVERWRITE)) {
                        logger.fine(String.format("Product '%s' was found in '%s'. It will be overwritten.",
                                                  product.getProductName(), path));
                    }
                    products.add(product);
                }
            }
            if (configuration.coherenceEnabled() && ProcessFlag.isSet(processFlag, ProcessFlag.COHERENCE)) {
                product = new HighLevelProduct();
                product.setProductName(name);
                product.setDownloadProductId(downloadProductId);
                product.setProductType(ProductType.L2A_COHE.value());
                path = this.targetPath.resolve(name).resolve(product.getProductName() + configuration.outputExtension());
                if (Files.exists(path) && ProcessFlag.isReset(processFlag, ProcessFlag.OVERWRITE)) {
                    processFlag = ProcessFlag.resetBit(processFlag, ProcessFlag.COHERENCE);
                    logger.warning(String.format("Product '%s' was found in '%s'. It will not be re-processed.",
                                                 product.getProductName(), path));
                } else {
                    if (Files.exists(path) && ProcessFlag.isSet(processFlag, ProcessFlag.OVERWRITE)) {
                        logger.fine(String.format("Product '%s' was found in '%s'. It will be overwritten.",
                                                  product.getProductName(), path));
                    }
                    products.add(product);
                }
            }
        }
        if (products.size() == 0) {
            logger.warning(String.format("Product pair (polarisation %s) was already processed in %s",
                                         polarisation.name(), this.targetPath.resolve(name)));
            return null;
        }
        logger.fine(String.format("Computed execution flags: AMPLITUDE=%d,COHERENCE=%d,OVERWRITE=%d",
                                  ProcessFlag.isSet(processFlag, ProcessFlag.AMPLITUDE) ? 1 : 0,
                                  ProcessFlag.isSet(processFlag, ProcessFlag.COHERENCE) ? 1 : 0,
                                  ProcessFlag.isSet(processFlag, ProcessFlag.OVERWRITE) ? 1 : 0));
        Path productFolder = this.temporaryPath.resolve(name);
        ActivityListener monitor = null;
        if (configuration.monitorDiskActivityInterval() > 0) {
            monitor = new ActivityListener();
            monitor.setSampleInterval(configuration.monitorDiskActivityInterval() * 1000);
            monitor.setLogFile(targetPath);
        }
        String message;
        final String productFolderName = productFolder.getFileName().toString();
        try {
            ProductLog.setupHandler(productFolderName, Files.createDirectories(productFolder));
            ProductLog.debug(productFolderName,
                             String.format("Master: %s, slave: %s",
                                    masterProduct.getName(masterProduct.getNameCount() - 1),
                                    slaveProduct != null ? slaveProduct.getName(slaveProduct.getNameCount() - 1) : "n/a"));
            LinkedHashMap<String, ExecutionUnit[]> jobs = createExecutionUnits(job, polarisation, masterProduct, slaveProduct,
                                                                               useMasterBand, name, metadataBuilder);
            int i = 1;
            String args;
            FixedQueue<String> queue = new FixedQueue<>(String.class, 1);
            final Map<String, Boolean> deletionStatus = new HashMap<>(stepDeletionStatus);
            final Map<String, Step> dbSteps = new HashMap<>();
            for (Map.Entry<String, ExecutionUnit[]> entry : jobs.entrySet()) {
                String stepName = entry.getKey();
                Task currentTask = Config.getPersistenceManager().getTask(job.getId(), stepName);
                JobHelper.update(currentTask, ActivityStatus.RUNNING);
                if (stepName.toLowerCase().startsWith("amplitude") && ProcessFlag.isReset(processFlag, ProcessFlag.AMPLITUDE)) {
                    ProductLog.debug(productFolderName, String.format("Step %s will be skipped (amplitude flag reset)", stepName));
                    i++;
                    continue;
                }
                if (stepName.toLowerCase().startsWith("coherence") && ProcessFlag.isReset(processFlag, ProcessFlag.COHERENCE)) {
                    ProductLog.debug(productFolderName, String.format("Step %s will be skipped (coherence flag reset)", stepName));
                    i++;
                    continue;
                }
                ExecutionUnit[] subSteps = entry.getValue();
                Instant stepStart = Instant.now();
                for (int j = 0; j < subSteps.length; j++) {
                    JobHelper.update(this.dbJobSteps.get(subSteps[j]), ActivityStatus.RUNNING, null);
                }
                try {
                    long availableDisk = productFolder.toFile().getUsableSpace() >> 20;
                    long requiredDisk = Arrays.stream(subSteps).mapToLong(ExecutionUnit::getMinDisk).sum();
                    if (availableDisk < requiredDisk) {
                        message = String.format("Not enough disk space for step %d (%s). Available: %dMB; required: %dMB",
                                                i, stepName, availableDisk, requiredDisk);
                        ProductLog.error(productFolderName, message);
                        notify(Level.SEVERE, message);
                        errors.add("Not enough disk space");
                        status = false;
                        break;
                    }
                    args = jobsToString(subSteps);
                    ProductLog.debug(productFolderName, String.format("Executing step %d (%s): %s", i, stepName, args));
                    OutputAccumulator outputConsumer = new OutputAccumulator();
                    int[] codes;
                    final Step firstStep = this.dbJobSteps.get(subSteps[0]);
                    Instant startTIme = Instant.now();
                    if (configuration.hasParallelSteps()) {
                        codes = Executor.execute(outputConsumer, timeoutMinutes * 60, monitor, subSteps);
                        for (int j = 0; j < subSteps.length; j++) {
                            JobHelper.update(this.dbJobSteps.get(subSteps[j]),
                                             codes[j] == 0 ? ActivityStatus.FINISHED : ActivityStatus.ERROR,
                                             codes[j]);
                        }
                        status = (Arrays.stream(codes).allMatch(c -> c == 0));
                    } else {
                        codes = new int[subSteps.length];
                        for (int si = 0; si < subSteps.length; si++) {
                            codes[si] = Executor.execute(outputConsumer, timeoutMinutes * 60, subSteps[si], monitor);
                            status = codes[si] == 0;
                            JobHelper.update(this.dbJobSteps.get(subSteps[si]),
                                             status ? ActivityStatus.FINISHED : ActivityStatus.ERROR,
                                             codes[si]);
                            if (!status) {
                                break;
                            }
                        }
                    }
                    i++;
                    final String output = outputConsumer.getOutput();
                    if (!status) {
                        // stop the steps execution
                        message = String.format("Error executing step %d (%s) [codes=%s]: %s",
                                                i - 1, stepName, Arrays.toString(codes), args);
                        ProductLog.error(productFolderName, message);
                        try {
                            Config.getPersistenceManager().saveLog(firstStep.getName(), currentTask.getId(), subSteps[0].getHost(),
                                                                   Duration.between(startTIme, Instant.now()).toMillis(), output, message);
                        } catch (Exception ex) {
                            logger.warning(String.format("Cannot save step output to database. Reason: %s", ex.getMessage()));
                        }
                        notify(Level.SEVERE, message);
                        JobHelper.update(currentTask, ActivityStatus.ERROR);
                        errors.add(output);
                        break;
                    } else {
                        try {
                            Config.getPersistenceManager().saveLog(firstStep.getName(), currentTask.getId(), subSteps[0].getHost(),
                                                                   Duration.between(startTIme, Instant.now()).toMillis(), output, "");
                        } catch (Exception ex) {
                            logger.warning(String.format("Cannot save step output to database. Reason: %s", ex.getMessage()));
                        }
                    }
                } finally {
                    ProductLog.debug(productFolderName,
                                     String.format("Step %d (%s) completed in %s", i - 1,
                                                   stepName,
                                                   DateUtils.formatDuration(Duration.between(stepStart, Instant.now()))));
                }
                JobHelper.update(currentTask, ActivityStatus.FINISHED);
                String previousStep = queue.enqueue(stepName);
                if (previousStep != null && !configuration.keepIntermediate() && deletionStatus.containsKey(previousStep)) {
                    if (deletionStatus.get(previousStep)) {
                        ProductType productType = stepName.contains("Coherence") ? ProductType.L2A_COHE : ProductType.L2A_AMP;
                        cleanupStepOutput(productFolder, productType, previousStep);
                    } else {
                        deletionStatus.put(previousStep, true);
                    }
                }
            }
            if (status) {
                for (HighLevelProduct hlp : products) {
                    if (createProductMetadata(productFolder, hlp, hlpDate, (short) siteId,
                                              relativeOrbit, orbitType, metadataBuilder) == null) {
                        ProductLog.warn(productFolderName,
                                        String.format("Metadata for product %s was not created",
                                                      hlp.getProductName()));
                    }
                    Path quickLookPath = null;
                    if (hlp.getFullPath() != null && (quickLookPath = OutputDataHandlerManager.getInstance().applyHandlers(Paths.get(hlp.getFullPath()))) == null) {
                        ProductLog.warn(productFolderName,
                                        String.format("Quicklook for product %s was not created",
                                                      hlp.getProductName()));
                    } else {
                        hlp.setQuickLookPath(quickLookPath != null ? quickLookPath.getFileName().toString() : null);
                    }
                }
            }
        } catch (Exception ex) {
            String exc = ExceptionUtils.getStackTrace(ex);
            logAndNotify(Level.SEVERE, exc);
            errors.add(exc);
            status = false;
        } finally {
            ProductLog.debug(productFolderName,
                             String.format("Product%s {%s} %s after %s",
                                           products.size() > 1 ? "s" : "",
                                           configuration.amplitudeEnabled() && configuration.coherenceEnabled() ?
                                                   products.stream().map(HighLevelProduct::getProductName).collect(Collectors.joining(",")) : name,
                                           status ? "completed" : "failed",
                                           DateUtils.formatDuration(Duration.between(start, Instant.now()))));
            ProductLog.cleanup(productFolderName);
            if (!configuration.keepIntermediate()) {
                // Remove the whole product dir on error or only the temporary (and keeping product files) on success
                cleanup(productFolder);
            }
            boolean moved = false;
            try {
                if (status && !Files.isSameFile(this.temporaryPath, this.targetPath)) {
                    logger.fine(String.format("Moving %s to %s", productFolder, this.targetPath));
                    FileUtilities.move(productFolder, this.targetPath);
                    moved = true;
                }
            } catch (IOException e) {
                logAndNotify(Level.SEVERE,
                             String.format("Cannot move folder %s to %s. Reason: %s",
                                           productFolder, this.targetPath, ExceptionUtils.getStackTrace(e)));
            }
            if (this.completionCallback != null) {
                for (HighLevelProduct p : products) {
                    if (p != null) {
                        try {
                            if (moved) {
                                p.setFullPath(p.getFullPath().replace(this.temporaryPath.toString(),
                                                                      this.targetPath.toString()));
                            }
                            this.completionCallback.accept(p, status);
                        } catch (Exception cex) {
                            logAndNotify(Level.WARNING,
                                         String.format("Callback for product %s failed. Reason: %s",
                                                       p.getProductName(), ExceptionUtils.getStackTrace(cex)));
                        }
                    }
                }
            }
            if (errors.size() > 0) {
                try {
                    Path logFile = targetPath.resolve(productFolder.getFileName()).resolve("output.log");
                    message = String.format(Configuration.getDefaultValue(Configuration.MESSAGE_TEMPLATE),
                                                   name,
                                                   masterProduct.getFileName(),
                                                   slaveProduct != null ? slaveProduct.getFileName() : "N/A",
                                                   String.join(";", errors),
                                                   Files.exists(logFile) ? logFile : "N/A");
                    sendNotification(ProcessingTopic.PROCESSING_ERROR.value(), name, message);
                } catch (Throwable e) {
                    logger.warning("Cannot send email notification. Reason: " + e.getMessage());
                }
            }
        }
        final boolean succeeded = errors.size() == 0;
        if (succeeded) {
            sendNotification(ProcessingTopic.PROCESSING_COMPLETED.value(), name, name);
        }
        return succeeded ? null : String.join(";", errors);
    }

    private LinkedHashMap<String, ExecutionUnit[]> createExecutionUnits(Job job, Polarisation polarisation,
                                                                        Path masterProduct, Path slaveProduct,
                                                                        boolean useMasterBand,
                                                                        String targetName,
                                                                        MetadataBuilder builder) throws IOException {
        Map<String, List<Map.Entry<String, List<String>>>> steps = new LinkedHashMap<>();
        if (configuration.amplitudeEnabled()) {
            steps.putAll(StepCommandBuilder.getCommands(ProductType.L2A_AMP));
        }
        if (configuration.coherenceEnabled()) {
            steps.putAll(StepCommandBuilder.getCommands(ProductType.L2A_COHE));
        }
        final Path productPath = this.temporaryPath.resolve(targetName);
        final String productPathString = productPath.toString();
        final LinkedHashMap<String, ExecutionUnit[]> units = new LinkedHashMap<>();
        int stepNumber = 1;
        final String productName = targetName + "_" + ProductType.L2A_AMP.shortName().replace("l2-", "").toUpperCase();
        final Sentinel1ProductHelper helper = (Sentinel1ProductHelper) SentinelProductHelper.create(
                useMasterBand ? masterProduct.getFileName().toString() : slaveProduct.getFileName().toString());
        final MetadataInspector masterInspector = getMetadataInspector(masterProduct);
        final MetadataInspector slaveInspector = getMetadataInspector(slaveProduct);
        if (masterInspector == null) {
            throw new IOException(String.format("Cannot read product metadata [%s]", masterProduct));
        }
        if (slaveInspector == null) {
            throw new IOException(String.format("Cannot read product metadata [%s]", slaveProduct));
        }
        MetadataInspector.Metadata masterMetadata = masterInspector.getMetadata(masterProduct);
        MetadataInspector.Metadata slaveMetadata = slaveInspector.getMetadata(slaveProduct);
        Task prevTask = null;
        for (Map.Entry<String, List<Map.Entry<String, List<String>>>> step : steps.entrySet()) {
            Task task = prevTask == null ?
                    JobHelper.createTask(job, step.getKey(), job.getParameters()) :
                    JobHelper.createTask(job, step.getKey(), job.getParameters(), prevTask.getId());
            final List<Map.Entry<String, List<String>>> entries = step.getValue();
            final int entriesSize = entries.size();
            final ExecutionUnit[] subSteps = new ExecutionUnit[entriesSize];
            for (int i = 0; i < entriesSize; i++) {
                String fileContents =
                        StringUtils.replaceEach(entries.get(i).getKey(),
                                                replaceTokens,
                                                new String[] { productPathString, productName, configuration.outputExtension(),
                                                               configuration.outputFormat(),
                                                               polarisation.name(), configuration.projectionCrs().toWKT(),
                                                               getBandName(useMasterBand, helper.getSensingDate(), polarisation),
                                                               configuration.resolutionInMeters(), configuration.resolutionInDegrees()
                                                              });
                final boolean isCoheStep = fileContents.contains("Coherence");
                final int insertionPoint = fileContents.indexOf("<node id=\"Write\">");
                if (fileContents.contains("Common_Steps")) {
                    Map<String, String> masterAttributes = masterMetadata.getAdditionalAttributes();
                    Map<String, String> slaveAttributes = slaveMetadata.getAdditionalAttributes();
                    fileContents = StringUtils.replaceEach(fileContents,
                                                           burstTokens,
                                                           new String[] {
                                                                   masterAttributes.get("iw1"),
                                                                   masterAttributes.get("iw2"),
                                                                   masterAttributes.get("iw3"),
                                                                   slaveAttributes.get("iw1"),
                                                                   slaveAttributes.get("iw2"),
                                                                   slaveAttributes.get("iw3")
                                                           });
                }
                if (fileContents.contains("$WRITE_PARENT") && i == entriesSize - 1 && insertionPoint > 0) {
                    try {
                        GeometryAdapter geometryAdapter = new GeometryAdapter();
                        Geometry masterFootprint = geometryAdapter.marshal(masterMetadata.getFootprint());
                        Geometry slaveFootprint = geometryAdapter.marshal(slaveMetadata.getFootprint());
                        builder.withMasterFootprint(masterFootprint)
                                .withSlaveFootprint(slaveFootprint);
                        if (isCoheStep) {
                            String intersectionWKT = masterFootprint.intersection(slaveFootprint).getEnvelope().toText();
                            fileContents = fileContents.substring(0, insertionPoint) +
                                    StepCommandBuilder.getOptionalSubsetNode().replace("$intersection", intersectionWKT) +
                                    fileContents.substring(insertionPoint);
                        }
                    } catch (Exception e) {
                        logger.warning(String.format("Cannot read product metadata. Reason: %s", e.getMessage()));
                    }
                    if (isCoheStep) {
                        fileContents = StringUtils.replaceEach(fileContents,
                                                               cropReplaceTokens,
                                                               new String[] { "Terrain-Correction", "Subset"});
                    } else {
                        fileContents = fileContents.replace("$WRITE_PARENT", "BandMaths");
                    }
                    if (isCoheStep) {
                        fileContents = fileContents.replace(productName + configuration.outputExtension(),
                                                            productName.replace("AMP", "COHE") + configuration.outputExtension());
                    }
                }
                Files.write(productPath.resolve(String.format("s1_step_%d_%d.xml", stepNumber, i + 1)), fileContents.getBytes());

                final List<String> subStepArguments = entries.get(i).getValue()
                        .stream()
                        .map(a -> {
                            a = StringUtils.replaceEach(a,
                                                        otherReplacements,
                                                        new String[] {
                                                                productPathString,
                                                                masterProduct.toString() });
                            if (slaveProduct != null) {
                                a = a.replace("$slaveProduct", slaveProduct.toString());
                            }
                            return a;
                        })
                        .collect(Collectors.toList());
                subSteps[i] = new ExecutionUnit(ExecutorType.PROCESS,
                                                InetAddress.getLocalHost().getHostName(), null, null,
                                                subStepArguments,
                                                false, SSHMode.EXEC);
                Step subStep = JobHelper.createStep(task,
                                                    task.getModuleShortName() + " " + stepNumber + "-" + (i + 1),
                                                    subStepArguments);
                this.dbJobSteps.put(subSteps[i], subStep);
                String minMemory = Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_MIN_MEMORY);
                if (minMemory != null && !minMemory.isEmpty()) {
                    subSteps[i].setMinMemory(Long.parseLong(minMemory));
                }
                String minDisk = Configuration.getSetting(ConfigurationKeys.DISK_SAMPLING_INTERVAL);
                subSteps[i].setMinDisk(minDisk != null && !minDisk.isEmpty() ? Long.parseLong(minDisk) : 0);
                logger.finest(String.format("Command for step %d-%d : %s {requires: memory %s, disk %s",
                                            stepNumber, i + 1, String.join(",", subSteps[i].getArguments()),
                                            minMemory != null ? minMemory + "MB" : "n/a",
                                            minDisk != null ? minDisk + "MB" : "n/a"));
            }
            stepNumber++;
            units.put(step.getKey(), subSteps);
            prevTask = task;
        }
        return units;
    }

    private static ExecutionUnit createCropNoDataUnit(Path productPath, Path outputPath) {
        Logger logger = Logger.getLogger(Sentinel1Level2Worker.class.getName());
        List<String> command = StepCommandBuilder.createCropNoDataCommand(productPath, outputPath);
        if (command == null) {
            logger.severe("Cannot create execution unit for cropping no data");
            return null;
        }
        try {
            return new ExecutionUnit(ExecutorType.PROCESS,
                                     InetAddress.getLocalHost().getHostName(), null, null,
                                     command, false, SSHMode.EXEC);
        } catch (UnknownHostException e) {
            logger.severe(e.getMessage());
            return null;
        }
    }

    private static void cropNoData(Path productPath) {
        Logger logger = Logger.getLogger(Sentinel1Level2Worker.class.getName());
        Path tmpPath = productPath.getParent()
                .resolve(FileUtilities.getFilenameWithoutExtension(productPath) +
                                 "_cropped" + FileUtilities.getExtension(productPath));
        try {
            ExecutionUnit unit = createCropNoDataUnit(productPath, tmpPath);
            if (unit == null) {
                logger.warning("Crop no data cannot be executed");
            } else {
                OutputAccumulator accumulator = new OutputAccumulator();
                int code;
                long initSize = -1;
                try {
                    initSize = Files.size(productPath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if ((code = Executor.execute(accumulator, 30, unit)) == 0) {
                    try {
                        if (Files.exists(tmpPath)) {
                            Files.delete(productPath);
                            Files.move(tmpPath, productPath);
                        }
                        logger.fine(String.format("Product size before crop: %d bytes. After crop: %d bytes.",
                                                  initSize, Files.size(productPath)));
                    } catch (IOException ex) {
                        logger.severe(String.format("Cannot rename cropped product %s. Reason: %s",
                                                    tmpPath, ex.getMessage()));
                    }
                } else if (code == 2) {
                    logger.warning("Product was not cropped (empty raster)");
                } else {
                    logger.severe(String.format("Cropping NoData for %s failed [code %d]. Output: %s",
                                                productPath, code, accumulator.getOutput()));
                }
            }
        } catch (Exception ex) {
            logger.severe(ex.getMessage());
        } finally {
            try {
                Files.deleteIfExists(tmpPath);
            } catch (IOException e) {
                logger.warning(String.format("Cannot remove file '%s'. Reason: %s", tmpPath, ExceptionUtils.getMessage(e)));
            }
        }
    }

    private void cleanupStepOutput(Path productFolder, ProductType productType, String stepName) {
        String[] files = StepCommandBuilder.getStepOutputFiles(productType, stepName);
        if (files != null) {
            for (String file : files) {
                try {
                    Path path = productFolder.resolve(file);
                    if (Files.isDirectory(path)) {
                        FileUtilities.deleteTree(path);
                    } else {
                        Files.delete(path);
                    }
                } catch (Exception ex) {
                    logger.warning(ex.getMessage());
                }
            }
        }
    }

    private void cleanup(Path productPath) {
        try {
            if (Files.isRegularFile(productPath)) {
                Files.deleteIfExists(productPath);
            }
            logger.fine(String.format("Performing cleanup in directory '%s'", productPath.toAbsolutePath()));
            Files.walkFileTree(productPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (!excludedExtensions.contains(FileUtilities.getExtension(file.toFile()))) {
                        Files.delete(file);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    // propagate the exception if not null
                    if (exc != null) {
                        throw exc;
                    }
                    // remove the directory if not empty
                    try(DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir)) {
                        if (!dirStream.iterator().hasNext()) {
                            Files.delete(dir);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ioe) {
            logger.severe(ioe.getMessage());
            ioe.printStackTrace();
        }
    }

    private Path createProductMetadata(Path productFolder, HighLevelProduct product, LocalDateTime productDate,
                                       short siteId, String relativeOrbit, OrbitType orbitType,
                                       MetadataBuilder templateMetadata) {
        final String name = product.getProductName();
        final Path productPath = productFolder.resolve(product.getProductName() + configuration.outputExtension());
        Path metadataPath = null;
        if (Files.exists(productPath)) {
            product.setFullPath(productPath.toString());
            product.setArchived(false);
            product.setInserted(LocalDateTime.now());
            product.setCreated(productDate);
            product.setProcessorId(configuration.processorId());
            product.setSatellite(Satellite.Sentinel1);
            product.setSiteId(siteId);
            product.setRelativeOrbit(Integer.parseInt(relativeOrbit));
            product.setTiles(new String[]{String.valueOf(product.getRelativeOrbit())});
            product.setOrbitType(orbitType);
            final MetadataInspector metadataInspector = getMetadataInspector(productPath);
            if (metadataInspector != null) {
                if (configuration.shouldCropNoData()) {
                    cropNoData(productPath);
                }
                try {
                    MetadataInspector.Metadata metadata = metadataInspector.getMetadata(productPath);
                    if (metadata != null) {
                        logger.finest(String.format("Inspection of product %s: %s",
                                                    product.getProductName(), metadata.toString()));
                        product.setFootprint(new GeometryAdapter().marshal(metadata.getFootprint()));
                        MetadataBuilder productMetadata = templateMetadata.clone()
                                .withProductName(product.getProductName())
                                .withCreatedOn(product.getInserted())
                                .withProductType(EnumUtils.getEnumConstantByValue(ProductType.class, product.getProductType()))
                                .withProductFormat(metadata.getProductType())
                                .withWidth(metadata.getWidth())
                                .withHeight(metadata.getHeight())
                                .withCrs(metadata.getCrs())
                                .withProductFootprint(product.getFootprint())
                                .withProductSize(productPath.toFile().length())
                                .withEndTime(Instant.now());
                        Map<String, Double> statistics = metadata.getStatistics();
                        if (statistics != null && statistics.size() == 4) {
                            productMetadata = productMetadata.withMinimum(statistics.get("min"))
                                                             .withMaximum(statistics.get("max"))
                                                             .withMean(statistics.get("mean"))
                                                             .withStandardDeviation(statistics.get("stdDev"));
                            if (statistics.values().stream().allMatch(v -> Double.compare(v, 0.0) == 0)) {
                                logAndNotify(Level.WARNING,
                                             String.format("Product %s seems to have only NODATA (statistics are 0)",
                                                           name));
                            }
                            int[] values = metadata.getHistogram();
                            Integer[] bins = null;
                            if (values != null) {
                                bins = Arrays.stream(values).boxed().toArray(Integer[]::new);
                            }
                            productMetadata = productMetadata.withHistogram(bins);
                            ProductDetails details = new ProductDetails();
                            details.setMinValue(statistics.get("min"));
                            details.setMaxValue(statistics.get("max"));
                            details.setMeanValue(statistics.get("mean"));
                            details.setStdDevValue(statistics.get("stdDev"));
                            details.setHistogram(bins);
                            product.setProductDetails(details);
                        } else {
                            logAndNotify(Level.WARNING, String.format("Cannot determine statistics for product %s", name));
                        }
                        metadataPath = Files.write(productFolder.resolve(product.getProductName() + ".mtd"),
                                                   productMetadata.toString().getBytes());
                    } else {
                        logAndNotify(Level.WARNING, String.format("Cannot determine footprint for product %s. Reason: %s",
                                                                  name, "null metadata"));
                    }
                } catch (Exception e) {
                    logAndNotify(Level.WARNING, String.format("Cannot determine footprint for product %s. Reason: %s",
                                                              name, ExceptionUtils.getStackTrace(e)));
                }
            } else {
                logAndNotify(Level.WARNING, String.format("Cannot determine footprint for product %s. Reason: %s",
                                                          name, "Metadata reader plugin not detected"));
            }
        } else {
            logAndNotify(Level.WARNING,
                         String.format("It appears that the product %s has been produced, but it does not exist in the file system",
                                       name));
        }
        return metadataPath;
    }

    private String jobsToString(ExecutionUnit[] jobs) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (ExecutionUnit job : jobs) {
            builder.append("{").append(String.join(",", job.getArguments())).append("},");
        }
        builder.setLength(builder.length() - 1);
        builder.append("]");
        return builder.toString();
    }

    private String getBandName(boolean isMaster, String acquisitionDate, Polarisation polarisation) {
        String friendlyDate = LocalDateTime.parse(acquisitionDate, productDateFormatter)
                                           .format(DateTimeFormatter.ofPattern("ddMMMyyyy", Locale.US));
        return "Intensity_" + polarisation.name () + "_" + (isMaster ? "mst" : "slv1") + "_" + friendlyDate;
    }

    private MetadataInspector getMetadataInspector(Path productPath) {
        return metadataServices.stream()
                .filter(s -> DecodeStatus.INTENDED == s.decodeQualification(productPath))
                .findFirst()
                .orElse(metadataServices.stream()
                                .filter(s -> DecodeStatus.SUITABLE == s.decodeQualification(productPath))
                                .findFirst()
                                .orElse(null));
    }

    private void notify(Level level, String message) {
        if (level == Level.WARNING || level == Level.SEVERE) {
            errors.add(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "[" + level.getName() + "] " + message);
            sendNotification(ProcessingTopic.PROCESSING_ATTENTION.value(), "Sentinel-1 L2 Processor", message);
        }
    }

    private void logAndNotify(Level level, String message) {
        logger.log(level, message);
        notify(level, message);
    }

    private void sendNotification(String topic, String key, String message) {
        Message msg = new Message();
        msg.setTopic(topic);
        msg.setTimestamp(System.currentTimeMillis());
        msg.setUser(SystemPrincipal.instance().getName());
        msg.addItem(Message.SOURCE_KEY, this.getClass().getSimpleName());
        msg.addItem(Message.PAYLOAD_KEY, key);
        msg.addItem(Message.MESSAGE_KEY, message);
        Messaging.send(SystemPrincipal.instance(), topic, msg);
    }

    private static class CustomFormatter extends Formatter {
        private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        @Override
        public String format(LogRecord record) {
            StringBuilder builder = new StringBuilder();
            builder.append(df.format(new Date(record.getMillis()))).append(" - ");
            builder.append(fixedWidth(record.getLevel().getName(), 7)).append(" - ");
            if (record.getSourceClassName() != null && record.getSourceMethodName() != null) {
                builder.append(fixedWidth(lastToken(record.getSourceClassName()) + "." +
                                                  lastToken(record.getSourceMethodName()), 40)).append(" - ");
            }
            builder.append(record.getMessage()).append("\n");
            return builder.toString();
        }

        private String fixedWidth(String value, int width) {
            if (value == null) value = "null";
            return value.length() >= width ? value.substring(value.length() - width) :
                    new String(new char[width - value.length()]).replace('\0', ' ') + value;
        }

        private String lastToken(String name) {
            return name.lastIndexOf('.') > 0 ? name.substring(name.lastIndexOf('.') + 1) : name;
        }
    }

    private static class ProductLog {
        private static final Map<String, FileHandler> loggers = new ConcurrentHashMap<>();
        private static Logger parentLogger;

        static void initialize(Logger logger) { parentLogger = logger; }

        static void setupHandler(String name, Path folder) throws IOException {
            FileHandler handler = new FileHandler(folder.resolve("output.log").toString());
            Formatter formatter = new CustomFormatter();
            handler.setFormatter(formatter);
            handler.setLevel(Level.FINEST);
            loggers.put(name, handler);
        }

        static void trace(String name, String message) {
            log(name, Level.FINEST, message);
        }

        static void debug(String name, String message) {
            log(name, Level.FINE, message);
        }

        static void info(String name, String message) {
            log(name, Level.INFO, message);
        }

        static void warn(String name, String message) {
            log(name, Level.WARNING, message);
        }

        static void error(String name, String message) {
            log(name, Level.SEVERE, message);
        }

        private static void log(String name, Level level, String message) {
            if (parentLogger != null) {
                parentLogger.log(level, message);
            }
            FileHandler handler = loggers.get(name);
            if (handler != null) {
                handler.publish(new LogRecord(level, message));
            }
        }

        static void cleanup(String name) {
            FileHandler handler = loggers.remove(name);
            if (handler != null) {
                handler.close();
            }
        }
    }

}
