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
import org.esa.sen2agri.entities.Processor;
import org.esa.sen4cap.preprocessing.scheduling.ConfigurationKeys;
import org.geotools.referencing.CRS;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import ro.cs.tao.configuration.ConfigurationManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ProcessorRuntimeConfiguration {
    private static Map<String, String> supportedTCResolutions;
    private final boolean parallelSteps;
    private final boolean shouldCrop;
    private final boolean keepIntermediate;
    private final boolean amplitudeEnabled;
    private final boolean coherenceEnabled;
    private final boolean cropNoData;
    private final int monitorDiskActivity;
    private final boolean computeHistogram;
    private final String outputFormat;
    private final String outputExtension;
    private final int gptTileCache;
    private final int gptParallelism;
    private final String projectionWKT;
    private final CoordinateReferenceSystem projectionCrs;
    private final String tcResolution;
    private final boolean bringLocally;
    private final boolean overwriteExisting;
    private final Processor processor;
    private final boolean replaceLinks;
    private final int stepTimeout;
    private final int daysBack;
    private final double intersectionThreshold;

    static {
        supportedTCResolutions = new HashMap<>();
        supportedTCResolutions.put("10.0", "8.983152841195215E-5");
        supportedTCResolutions.put("15.0", "1.3474729261792824E-4");
        supportedTCResolutions.put("20.0", "1.796630568239043E-4");
        supportedTCResolutions.put("30.0", "2.6949458523585647E-4");
    }

    public static ProcessorRuntimeConfiguration get() {
        final boolean computeHistogram = Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_EXTRACT_HISTOGRAM));
        if (computeHistogram) {
            ConfigurationManager.getInstance().setValue("extract.histogram", "true");
        }
        final String projectionWKT = Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_PROJECTION);
        final CoordinateReferenceSystem projectionCrs;
        try {
            if (projectionWKT.toUpperCase().startsWith("EPSG")) {
                projectionCrs = CRS.decode(projectionWKT.toUpperCase());
            } else {
                projectionCrs = CRS.parseWKT(projectionWKT);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Invalid projection code or wkt [Error: %s; projection: %s]",
                                                             e.getMessage(), projectionWKT));
        }
        String resolution = Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_PIXEL_SIZE);
        if (!supportedTCResolutions.containsKey(resolution)) {
            resolution = Configuration.getDefaultValue(ConfigurationKeys.S1_PROCESSOR_PIXEL_SIZE);
        }
        Processor dbProcessor;
        if ((dbProcessor = Config.getPersistenceManager().getProcessor(org.esa.sen4cap.entities.enums.Processor.L2S1.shortName())) == null) {
            dbProcessor = new Processor();
            dbProcessor.setName("L2-S1 Pre-Processor");
            dbProcessor.setDescription("Pre-processor for Sentinel-1 Amplitude and Coherence extraction");
            dbProcessor.setShortName(org.esa.sen4cap.entities.enums.Processor.L2S1.shortName());
            dbProcessor.setLabel("L2 S1 &mdash; SAR Pre-Processor");
            dbProcessor = Config.getPersistenceManager().save(dbProcessor);
        }
        return new ProcessorRuntimeConfiguration(Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_PARALLEL_STEPS)),
                                                 Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_CROP_OUTPUT)),
                                                 Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_INTERMEDIATE)),
                                                 Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_AMPLITUDE_ENABLED)),
                                                 Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_COHERENCE_ENABLED)),
                                                 Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_CROP_NODATA)),
                                                 Integer.parseInt(Configuration.getSetting(ConfigurationKeys.DISK_SAMPLING_INTERVAL)),
                                                 computeHistogram,
                                                 Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_OUTPUT_FORMAT),
                                                 Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_OUTPUT_EXTENSION),
                                                 Integer.parseInt(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_GPT_CACHE)),
                                                 Integer.parseInt(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_GPT_PARALLELISM)),
                                                 projectionWKT, projectionCrs, resolution,
                                                 Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_BRING_LOCALLY)),
                                                 Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_OVERWRITE_EXISTING)),
                                                 dbProcessor,
                                                 Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_RESOLVE_LINKS)),
                                                 Integer.parseInt(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_TIMEOUT)),
                                                 Integer.parseInt(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_DAYS_BACK)),
                                                 Double.parseDouble(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_MIN_INTERSECTION)));

    }

    private ProcessorRuntimeConfiguration(boolean parallelSteps, boolean shouldCrop, boolean keepIntermediate,
                                          boolean amplitudeEnabled, boolean coherenceEnabled, boolean cropNoData,
                                          int monitorDiskActivity, boolean computeHistogram, String outputFormat,
                                          String outputExtension, int gptTileCache, int gptParallelism,
                                          String projectionWKT, CoordinateReferenceSystem projectionCrs, String tcResolution,
                                          boolean bringLocally, boolean overwriteExisting, Processor processor,
                                          boolean replaceLinks, int stepTimeout, int daysBack, double intersection) {
        this.parallelSteps = parallelSteps;
        this.shouldCrop = shouldCrop;
        this.keepIntermediate = keepIntermediate;
        this.amplitudeEnabled = amplitudeEnabled;
        this.coherenceEnabled = coherenceEnabled;
        this.cropNoData = cropNoData;
        this.monitorDiskActivity = monitorDiskActivity;
        this.computeHistogram = computeHistogram;
        this.outputFormat = outputFormat;
        this.outputExtension = outputExtension;
        this.gptTileCache = gptTileCache;
        this.gptParallelism = gptParallelism;
        this.projectionWKT = projectionWKT;
        this.projectionCrs = projectionCrs;
        this.tcResolution = tcResolution;
        this.bringLocally = bringLocally;
        this.overwriteExisting = overwriteExisting;
        this.processor = processor;
        this.replaceLinks = replaceLinks;
        this.stepTimeout = stepTimeout;
        this.daysBack = daysBack;
        this.intersectionThreshold = intersection;
    }

    public boolean hasParallelSteps() {
        return parallelSteps;
    }

    public boolean shouldCrop() {
        return shouldCrop;
    }

    public boolean keepIntermediate() {
        return keepIntermediate;
    }

    public boolean amplitudeEnabled() {
        return amplitudeEnabled;
    }

    public boolean coherenceEnabled() {
        return coherenceEnabled;
    }

    public boolean shouldCropNoData() {
        return cropNoData;
    }

    public int monitorDiskActivityInterval() {
        return monitorDiskActivity;
    }

    public boolean shouldComputeHistogram() {
        return computeHistogram;
    }

    public String outputFormat() {
        return outputFormat;
    }

    public String outputExtension() {
        return outputExtension;
    }

    public int gptTileCache() {
        return gptTileCache;
    }

    public int gptParallelism() {
        return gptParallelism;
    }

    public String projectionWKT() {
        return projectionWKT;
    }

    public CoordinateReferenceSystem projectionCrs() {
        return projectionCrs;
    }

    public String projectionCode() {
        final Optional<ReferenceIdentifier> identifier = projectionCrs.getIdentifiers().stream().findFirst();
        return identifier.map(Object::toString).orElse(projectionCrs.getName().getCode());
    }

    public String resolutionInMeters() {
        return tcResolution;
    }

    public String resolutionInDegrees() {
        return supportedTCResolutions.get(tcResolution);
    }

    public boolean copyProductsLocally() {
        return bringLocally;
    }

    public boolean overwriteExistingProducts() {
        return overwriteExisting;
    }

    public int processorId() {
        return processor.getId();
    }

    public Processor processor() {
        return processor;
    }

    public boolean shouldeplaceLinks() {
        return replaceLinks;
    }

    public int stepTimeout() {
        return stepTimeout;
    }

    public int daysBack() {
        return daysBack;
    }

    public double intersectionThreshold() {
        return intersectionThreshold;
    }

    @Override
    public String toString() {
        return "Sentinel-1 pre-processor configuration { " +
                "parallelSteps=" + parallelSteps +
                ", shouldCrop=" + shouldCrop +
                ", keepIntermediate=" + keepIntermediate +
                ", amplitudeEnabled=" + amplitudeEnabled +
                ", coherenceEnabled=" + coherenceEnabled +
                ", cropNoData=" + cropNoData +
                ", monitorDiskActivityInterval=" + monitorDiskActivity +
                ", computeHistogram=" + computeHistogram +
                ", outputFormat='" + outputFormat + '\'' +
                ", outputExtension='" + outputExtension + '\'' +
                ", gptTileCache=" + gptTileCache +
                ", gptParallelism=" + gptParallelism +
                ", projectionWKT='" + projectionWKT + '\'' +
                ", tcResolution='" + tcResolution + '\'' +
                ", bringLocally=" + bringLocally +
                ", overwriteExisting=" + overwriteExisting +
                " }";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProcessorRuntimeConfiguration that = (ProcessorRuntimeConfiguration) o;
        return parallelSteps == that.parallelSteps &&
                shouldCrop == that.shouldCrop &&
                keepIntermediate == that.keepIntermediate &&
                amplitudeEnabled == that.amplitudeEnabled &&
                coherenceEnabled == that.coherenceEnabled &&
                cropNoData == that.cropNoData &&
                monitorDiskActivity == that.monitorDiskActivity &&
                computeHistogram == that.computeHistogram &&
                gptTileCache == that.gptTileCache &&
                gptParallelism == that.gptParallelism &&
                bringLocally == that.bringLocally &&
                overwriteExisting == that.overwriteExisting &&
                Objects.equals(outputFormat, that.outputFormat) &&
                Objects.equals(outputExtension, that.outputExtension) &&
                Objects.equals(projectionWKT, that.projectionWKT) &&
                Objects.equals(projectionCrs, that.projectionCrs) &&
                Objects.equals(tcResolution, that.tcResolution);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parallelSteps, shouldCrop, keepIntermediate, amplitudeEnabled, coherenceEnabled, cropNoData,
                            monitorDiskActivity, computeHistogram, outputFormat, outputExtension, gptTileCache, gptParallelism,
                            projectionWKT, projectionCrs, tcResolution, bringLocally, overwriteExisting);
    }
}
