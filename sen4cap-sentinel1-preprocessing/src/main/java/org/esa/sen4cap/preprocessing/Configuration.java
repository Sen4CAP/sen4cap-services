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
import org.esa.sen2agri.db.Constants;
import org.esa.sen4cap.preprocessing.scheduling.ConfigurationKeys;

import java.util.HashMap;
import java.util.Map;

public final class Configuration {
    static final String COMMON_CHAIN = "common.chain";
    static final String AMPLITUDE_CHAIN = "amplitude.chain";
    static final String AMPLITUDE_JOINED_CHAIN = "amplitude.joined.chain";
    static final String COHERENCE_CHAIN = "coherence.chain";
    static final String COHERENCE_JOINED_CHAIN = "coherence.joined.chain";
    static final String MESSAGE_TEMPLATE = "error.message.template";
    private static final Map<String, String> defaults;

    static {
        defaults = new HashMap<>();
        defaults.put(ConfigurationKeys.S1_PROCESSOR_REVERSE_ACQUISITION_DATE, "false");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_ENABLED, "true");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_AMPLITUDE_ENABLED, "true");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_COHERENCE_ENABLED, "true");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_INTERVAL, "60");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_BRING_LOCALLY, "false");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_POLARISATIONS, "VV;VH");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_CROP_OUTPUT, "true");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_CROP_NODATA, "true");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_INTERMEDIATE, "false");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_OUTPUT_FORMAT, "GeoTIFF");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_OUTPUT_EXTENSION, ".tif");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_WORK_DIR, "");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_MIN_MEMORY, "8192");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_MIN_DISK, "10240");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_PIXEL_SIZE, "20.0");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_MIN_INTERSECTION, "0.05");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_GPT_CACHE, "256");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_GPT_PARALLELISM, "8");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_PARALLEL_STEPS, "true");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_RESOLVE_LINKS, "false");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_PARALLELISM, "1");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_MASTER, "S1B");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_OVERWRITE_EXISTING, "false");
        defaults.put(ConfigurationKeys.DISK_SAMPLING_INTERVAL, "0");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_EXTRACT_HISTOGRAM, "true");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_DAYS_BACK, "6");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_WAIT_FOR_ORBIT_FILES, "2");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_TIMEOUT, "60");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_OUTPUT_PATH, Constants.DEFAULT_TARGET_PATH + "/{site}/l2a-s1");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_JOIN_AMPLITUDE_STEPS, "false");
        defaults.put(ConfigurationKeys.S1_PROCESSOR_JOIN_COHERENCE_STEPS, "false");
        // default projection is LAEA (EPSG:3035)
        defaults.put(ConfigurationKeys.S1_PROCESSOR_PROJECTION, "PROJCS[\"ETRS89 / LAEA Europe\", GEOGCS[\"ETRS89\", DATUM[\"European Terrestrial Reference System 1989\", SPHEROID[\"GRS 1980\", 6378137.0, 298.257222101, AUTHORITY[\"EPSG\",\"7019\"]], TOWGS84[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], AUTHORITY[\"EPSG\",\"6258\"]], PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]], UNIT[\"degree\", 0.017453292519943295], AXIS[\"Geodetic longitude\", EAST], AXIS[\"Geodetic latitude\", NORTH], AUTHORITY[\"EPSG\",\"4258\"]], PROJECTION[\"Lambert_Azimuthal_Equal_Area\", AUTHORITY[\"EPSG\",\"9820\"]], PARAMETER[\"latitude_of_center\", 52.0], PARAMETER[\"longitude_of_center\", 10.0], PARAMETER[\"false_easting\", 4321000.0], PARAMETER[\"false_northing\", 3210000.0], UNIT[\"m\", 1.0], AXIS[\"Easting\", EAST], AXIS[\"Northing\", NORTH], AUTHORITY[\"EPSG\",\"3035\"]]");
        defaults.put(COMMON_CHAIN, "{\"Calibration\":[\"0-calibration.xml\"],\"Coregistration\":[\"1-1-geocoding.xml\",\"1-2-geocoding.xml\",\"1-3-geocoding.xml\"]}");
        defaults.put(AMPLITUDE_CHAIN, "{\"Amplitude Deburst\":[\"2-1-amplitude.xml\",\"2-2-amplitude.xml\",\"2-3-amplitude.xml\"],\"Amplitude Merge\":[\"3-amplitude.xml\"],\"Amplitude Multilook\":[\"4-amplitude.xml\"],\"Amplitude Terrain Correction\":[\"5-amplitude.xml\"]}");
        defaults.put(AMPLITUDE_JOINED_CHAIN, "{\"Amplitude Deburst\":[\"2-1-amplitude.xml\",\"2-2-amplitude.xml\",\"2-3-amplitude.xml\"],\"Amplitude Merge, Multilook,Terrain Correction\":[\"3-4-5-amplitude.xml\"]}");
        defaults.put(COHERENCE_CHAIN, "{\"Coherence Deburst\":[\"2-1-coherence.xml\", \"2-2-coherence.xml\", \"2-3-coherence.xml\"],\"Coherence Merge\":[\"3-coherence.xml\"],\"Coherence Terrain Correction\":[\"4-coherence.xml\"]}");
        defaults.put(COHERENCE_JOINED_CHAIN, "{\"Coherence Deburst\":[\"2-1-coherence.xml\", \"2-2-coherence.xml\", \"2-3-coherence.xml\"],\"Coherence Merge, Terrain Correction\":[\"3-4-coherence.xml\"]}");
        defaults.put(MESSAGE_TEMPLATE, "Product:\t%s\nMaster:\t%s\nSlave:\t%s\n%s\nLog location:\t%s\n========================================\n");
    }

    public static String getSetting(String key) {
        String value = Config.getProperty(key, null);
        return value == null ? Config.getSetting(key, defaults.get(key)) : value;
    }

    public static String getDefaultValue(String key) { return defaults.get(key); }

    public static Map<String, String> getActualSettings() {
        Map<String, String> actualSettings = new HashMap<>();
        for (Map.Entry<String, String> entry : defaults.entrySet()) {
            actualSettings.put(entry.getKey(), getSetting(entry.getKey()));
        }
        return actualSettings;
    }
}
