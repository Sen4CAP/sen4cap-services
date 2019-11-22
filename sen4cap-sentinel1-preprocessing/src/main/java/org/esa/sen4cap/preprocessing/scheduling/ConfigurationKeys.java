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

/**
 * @author Cosmin Cara
 */
public class ConfigurationKeys {
    public static final String S1_PROCESSOR_REVERSE_ACQUISITION_DATE = "processor.l2s1.process.newest";
    public static final String S1_PROCESSOR_ENABLED = "processor.l2s1.enabled";
    public static final String S1_PROCESSOR_AMPLITUDE_ENABLED = "processor.l2s1.compute.amplitude";
    public static final String S1_PROCESSOR_COHERENCE_ENABLED = "processor.l2s1.compute.coherence";
    public static final String S1_PROCESSOR_INTERVAL = "processor.l2s1.interval";
    public static final String S1_PROCESSOR_BRING_LOCALLY = "processor.l2s1.copy.locally";
    public static final String S1_PROCESSOR_OUTPUT_PATH = "processor.l2s1.path";
    public static final String S1_PROCESSOR_POLARISATIONS = "processor.l2s1.polarisations";
    public static final String S1_PROCESSOR_PROJECTION = "processor.l2s1.projection";
    public static final String S1_PROCESSOR_INTERMEDIATE = "processor.l2s1.keep.intermediate";
    public static final String S1_PROCESSOR_CROP_OUTPUT = "processor.l2s1.crop.output";
    public static final String S1_PROCESSOR_MASTER = "processor.l2s1.master";
    public static final String S1_PROCESSOR_MIN_MEMORY = "processor.l2s1.min.memory";
    public static final String S1_PROCESSOR_MIN_DISK = "processor.l2s1.min.disk";
    public static final String S1_PROCESSOR_PARALLELISM = "processor.l2s1.parallelism";
    public static final String S1_PROCESSOR_OUTPUT_EXTENSION = "processor.l2s1.output.extension";
    public static final String S1_PROCESSOR_OUTPUT_FORMAT = "processor.l2s1.output.format";
    public static final String S1_PROCESSOR_PIXEL_SIZE = "processor.l2s1.pixel.spacing";
    public static final String S1_PROCESSOR_MIN_INTERSECTION = "processor.l2s1.min.intersection";
    public static final String S1_PROCESSOR_WORK_DIR = "processor.l2s1.work.dir";
    public static final String S1_PROCESSOR_CROP_NODATA = "processor.l2s1.crop.nodata";
    public static final String S1_PROCESSOR_GPT_CACHE = "processor.l2s1.gpt.tile.cache.size";
    public static final String S1_PROCESSOR_GPT_PARALLELISM = "processor.l2s1.gpt.parallelism";
    public static final String S1_PROCESSOR_RESOLVE_LINKS = "processor.l2s1.resolve.links";
    public static final String S1_PROCESSOR_USE_DOCKER = "processor.l2s1.plugins.use.docker";
    public static final String S1_PROCESSOR_DOCKER_IMAGE = "processor.l2s1.docker.gdal.image";
    public static final String S1_PROCESSOR_PARALLEL_STEPS = "processor.l2s1.parallel.steps.enabled";
    public static final String S1_PROCESSOR_OVERWRITE_EXISTING = "processor.l2s1.overwrite.existing";
    public static final String S1_PROCESSOR_DAYS_BACK = "processor.l2s1.temporal.offset";
    public static final String S1_PROCESSOR_WAIT_FOR_ORBIT_FILES = "processor.l2s1.acquisition.delay";
    public static final String S1_PROCESSOR_EXTRACT_HISTOGRAM = "processor.l2s1.extract.histogram";
    public static final String S1_PROCESSOR_TIMEOUT = "processor.l2s1.step.timeout";
    public static final String S1_PROCESSOR_JOIN_AMPLITUDE_STEPS = "processor.l2s1.join.amplitude.steps";
    public static final String S1_PROCESSOR_JOIN_COHERENCE_STEPS = "processor.l2s1.join.coherence.steps";

    public static final String DISK_SAMPLING_INTERVAL = "disk.monitor.interval";
    public static final String REPORTS_ENABLED = "scheduled.reports.enabled";
    public static final String REPORTS_INTERVAL_HOURS = "scheduled.reports.interval";
    public static final String SENTINEL1_DOWNLOAD_DIRECTORY = "downloader.s1.write-dir";

}
