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

import org.esa.sen4cap.entities.enums.ProductType;
import org.esa.sen4cap.preprocessing.scheduling.ConfigurationKeys;
import ro.cs.tao.configuration.ConfigurationManager;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.*;
import java.nio.file.Path;
import java.util.*;

class StepCommandBuilder {
    private static final String optionalSubsetNode = "<node id=\"Subset\">\n" +
            "        <operator>Subset</operator>\n" +
            "        <sources>\n" +
            "            <sourceProduct refid=\"$SUBSET_PARENT\"/>\n" +
            "        </sources>\n" +
            "        <parameters>\n" +
            "            <geoRegion>$intersection</geoRegion>\n" +
            "            <copyMetadata>true</copyMetadata>\n" +
            "        </parameters>\n" +
            "    </node>\n";
    private static Map<ProductType, LinkedHashMap<String, List<Map.Entry<String, List<String>>>>> commands;
    private static Map<ProductType, Map<String, String[]>> outputFiles;
    private static Map<String, Boolean> canDeleteMap;
    private static int gptCache;
    private static int gptProcs;
    private static final char[] buffer = new char[1024];

    static void initialize() {
        gptCache = Integer.parseInt(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_GPT_CACHE));
        gptProcs = Integer.parseInt(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_GPT_PARALLELISM));
        final boolean joinAmpSteps = Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_JOIN_AMPLITUDE_STEPS));
        final boolean joinCoheSteps = Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_JOIN_COHERENCE_STEPS));
        final LinkedHashMap<String, String[]> commonStepFiles = new LinkedHashMap<>(parseJson(Configuration.getDefaultValue(Configuration.COMMON_CHAIN)));
        final LinkedHashMap<String, String[]> amplitudeStepFiles = new LinkedHashMap<>();
        final LinkedHashMap<String, String[]> coherenceStepFiles = new LinkedHashMap<>();
        canDeleteMap = new HashMap<>();
        Map<String, String[]> commonOutputFiles = new HashMap<>();
        commonOutputFiles.put("Calibration", new String[]{
                "split_1-1.data", "split_1-1.dim", "split_1-2.data", "split_1-2.dim", "split_1-3.data", "split_1-3.dim",
                "split_2-1.data", "split_2-1.dim", "split_2-2.data", "split_2-2.dim", "split_2-3.data", "split_2-3.dim"});
        canDeleteMap.put("Calibration", true);
        commonOutputFiles.put("Coregistration", new String[]{
                    "geocoded_1.data", "geocoded_1.dim", "geocoded_2.data", "geocoded_2.dim", "geocoded_3.data", "geocoded_3.dim"});

        outputFiles = new HashMap<>();
        final boolean amplitudeEnabled = Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_AMPLITUDE_ENABLED));
        final boolean coherenceEnabled = Boolean.parseBoolean(Configuration.getSetting(ConfigurationKeys.S1_PROCESSOR_COHERENCE_ENABLED));
        canDeleteMap.put("Coregistration", amplitudeEnabled ^ coherenceEnabled);
        if (amplitudeEnabled) {
            Map<String, String[]> amplitudeOutputFiles = new HashMap<>(commonOutputFiles);
            amplitudeOutputFiles.put("Amplitude Deburst", new String[] {
                    "amplitude_deburst_1.data", "amplitude_deburst_1.dim",
                    "amplitude_deburst_2.data", "amplitude_deburst_2.dim",
                    "amplitude_deburst_3.data", "amplitude_deburst_3.dim" });
            canDeleteMap.put("Amplitude Deburst", true);
            if (!joinAmpSteps) {
                amplitudeStepFiles.putAll(parseJson(Configuration.getDefaultValue(Configuration.AMPLITUDE_CHAIN)));
                amplitudeOutputFiles.put("Amplitude Merge", new String[]{"amplitude_merge.data", "amplitude_merge.dim" });
                amplitudeOutputFiles.put("Amplitude Multilook", new String[]{"amplitude_multilook.data", "amplitude_multilook.dim" });
                canDeleteMap.put("Amplitude Merge", true);
                canDeleteMap.put("Amplitude Multilook", true);
            } else {
                amplitudeStepFiles.putAll(parseJson(Configuration.getDefaultValue(Configuration.AMPLITUDE_JOINED_CHAIN)));
            }
            outputFiles.put(ProductType.L2A_AMP, amplitudeOutputFiles);
        }
        if (coherenceEnabled) {
            Map<String, String[]> coherenceOutputFiles = new HashMap<>();
            if (!amplitudeEnabled) {
                coherenceOutputFiles.putAll(commonOutputFiles);
            }
            coherenceOutputFiles.put("Coherence Deburst", new String[] {
                    "coherence_deburst_sub1.data", "coherence_deburst_sub1.dim",
                    "coherence_deburst_sub2.data", "coherence_deburst_sub2.dim",
                    "coherence_deburst_sub3.data", "coherence_deburst_sub3.dim" });
            canDeleteMap.put("Coherence Deburst", true);
            if (!joinCoheSteps) {
                coherenceStepFiles.putAll(parseJson(Configuration.getDefaultValue(Configuration.COHERENCE_CHAIN)));
                coherenceOutputFiles.put("Coherence Merge", new String[]{"coherence_merge.data", "coherence_merge.dim" });
                canDeleteMap.put("Coherence Merge", true);
            } else {
                coherenceStepFiles.putAll(parseJson(Configuration.getDefaultValue(Configuration.COHERENCE_JOINED_CHAIN)));
            }
            outputFiles.put(ProductType.L2A_COHE, coherenceOutputFiles);
        }

        commands = new HashMap<>();
        LinkedHashMap<String, List<Map.Entry<String, List<String>>>> amplitudeTemplates = new LinkedHashMap<>();
        LinkedHashMap<String, List<Map.Entry<String, List<String>>>> coherenceTemplates = new LinkedHashMap<>();
        int step = 1;
        boolean inputProducts = true;
        if (amplitudeEnabled) {
            for (Map.Entry<String, String[]> sequentialStep : commonStepFiles.entrySet()) {
                amplitudeTemplates.put(sequentialStep.getKey(),
                                       processSequentialStep(sequentialStep, step++, inputProducts));
                inputProducts = false;
            }
            for (Map.Entry<String, String[]> sequentialStep : amplitudeStepFiles.entrySet()) {
                amplitudeTemplates.put(sequentialStep.getKey(),
                                       processSequentialStep(sequentialStep, step++, inputProducts));
            }
            commands.put(ProductType.L2A_AMP, amplitudeTemplates);
        }
        if (coherenceEnabled) {
            if (!amplitudeEnabled) {
                for (Map.Entry<String, String[]> sequentialStep : commonStepFiles.entrySet()) {
                    coherenceTemplates.put(sequentialStep.getKey(),
                                           processSequentialStep(sequentialStep, step++, inputProducts));
                    inputProducts = false;
                }
            }
            for (Map.Entry<String, String[]> sequentialStep : coherenceStepFiles.entrySet()) {
                coherenceTemplates.put(sequentialStep.getKey(),
                                       processSequentialStep(sequentialStep, step++, inputProducts));
            }
            commands.put(ProductType.L2A_COHE, coherenceTemplates);
        }
    }

    static String getOptionalSubsetNode() { return optionalSubsetNode; }

    static Map<String, List<Map.Entry<String, List<String>>>> getCommands(ProductType productType) {
        return commands.get(productType);
    }

    static Map<String, Boolean> getStepDeletionStatus() { return canDeleteMap; }

    static String[] getStepOutputFiles(ProductType productType, String stepName) {
        final Map<String, String[]> map = outputFiles.get(productType);
        // if the productType is disabled, check to avoid NPE
        return map != null ? map.get(stepName) : null;
    }

    static List<String> createCropNoDataCommand(Path productPath, Path outputPath) {
        Path scriptFolder = ConfigurationManager.getInstance().getScriptsFolder();
        if (scriptFolder == null) {
            return null;
        }
        final List<String> cmd = new ArrayList<>();
        cmd.add("python");
        cmd.add(scriptFolder.resolve("autocrop-raster.py").toString());
        cmd.add(productPath.toString());
        cmd.add(outputPath.toString());
        return cmd;
    }

    private static List<Map.Entry<String, List<String>>> processSequentialStep(Map.Entry<String, String[]> sequentialStep,
                                                                               int stepIndex, boolean inputProducts) {
        List<Map.Entry<String, List<String>>> stepTemplates = new ArrayList<>();
        int read;
        int idx = 1;
        for (String parallelStep : sequentialStep.getValue()) {
            try (Reader reader = new InputStreamReader(Sentinel1Level2Worker.class.getResourceAsStream(parallelStep))) {
                final StringBuilder builder = new StringBuilder();
                while ((read = reader.read(buffer, 0, buffer.length)) > 0) {
                    builder.append(buffer, 0, read);
                }
                final List<String> cmd = new ArrayList<>();
                cmd.add("gpt");
                cmd.add("-c");
                cmd.add(gptCache + "M");
                cmd.add("-q");
                cmd.add(String.valueOf(gptProcs));
                cmd.add("$TMPFOLDER" + File.separator + String.format("s1_step_%d_%d.xml", stepIndex, idx));
                if (inputProducts && idx == 1) {
                    cmd.add("$masterProduct");
                    cmd.add("$slaveProduct");
                }
                stepTemplates.add(new AbstractMap.SimpleEntry<>(builder.toString(), cmd));
                idx++;
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return stepTemplates;
    }

    private static LinkedHashMap<String, String[]> parseJson(String value) {
        LinkedHashMap<String, String[]> files = new LinkedHashMap<>();
        List<String> list = new ArrayList<>();
        try (JsonReader jsonReader = Json.createReader(new StringReader(value))) {
            JsonObject root = jsonReader.readObject();
            for (String key : root.keySet()) {
                JsonArray array = root.getJsonArray(key);
                for (int i = 0; i < array.size(); i++) {
                    list.add(array.getString(i));
                }
                files.put(key, list.toArray(new String[0]));
                list.clear();
            }
        }
        return files;
    }
}
