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

package org.esa.sen4cap.shapefile.services.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.shapefile.db.Database;
import org.esa.sen4cap.shapefile.entities.ParcelInfo;
import org.esa.sen4cap.shapefile.entities.Practice;
import org.esa.sen4cap.shapefile.entities.xml.ParcelXml;
import org.springframework.stereotype.Service;
import ro.cs.tao.serialization.BaseSerializer;
import ro.cs.tao.serialization.MediaType;
import ro.cs.tao.serialization.SerializationException;
import ro.cs.tao.serialization.SerializerFactory;
import ro.cs.tao.utils.Tuple;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service("parcelQueryService")
public class ParcelQueryService {
    private static final Map<Integer, Map<String, Tuple<Long, Long>>> offsets;
    private static final ObjectMapper jsonSerializer;
    private static final BaseSerializer<ParcelXml> xmlDeserializer;
    private Logger logger = Logger.getLogger(ParcelQueryService.class.getName());

    static {
        jsonSerializer = new ObjectMapper();
        jsonSerializer.registerModule(new JavaTimeModule());
        jsonSerializer.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        try {
            xmlDeserializer = SerializerFactory.create(ParcelXml.class, MediaType.XML);
        } catch (SerializationException e) {
            throw new RuntimeException(e);
        }
        offsets = new WeakHashMap<>();
    }

//    public static void main(String[] args) throws IOException, SerializationException {
//        ParcelQueryService parcelQueryService = new ParcelQueryService();
//        String[] ids = new String[] { "742732-5-a" , "342276-6-b", "81477-8-a" };
//        for (String id : ids) {
//            long start = System.currentTimeMillis();
//            System.out.println(Runtime.getRuntime().freeMemory());
//            List<ParcelInfo> parcelInfo = parcelQueryService.getParcelInfo(1, 2018, Practice.CatchCrop, id);
//            //System.out.println(jsonSerializer.writeValueAsString(parcelInfo));
//            System.out.println(String.format("Completed in %d ms", System.currentTimeMillis() - start));
//            System.out.println(Runtime.getRuntime().freeMemory());
//            System.gc();
//        }
//    }

    public List<ParcelInfo> getParcelInfo(Site site, int year, Practice practice, String... ids) throws IOException, SerializationException {
        int key = Objects.hash(site, year, practice);
        String dataFileName = getDataFile(site.getId(), year, practice);
        if (dataFileName == null || !Files.exists(Paths.get(dataFileName))) {
            throw new IOException(String.format("Data [%s] file not found", dataFileName));
        }
        String indexFileName = dataFileName + ".idx";
        logger.finest(String.format("Using index file %s", indexFileName));
        if (!offsets.containsKey(key)) {
            logger.finest(String.format("Index for site %s not loaded, reading from file %s",
                                        site.getShortName(), indexFileName));
            readIndexFile(key, indexFileName);
        }
        Map<String, Tuple<Long, Long>> parcelsOffsets = new LinkedHashMap<>();
        for (String id : ids) {
            Tuple<Long, Long> offsets = ParcelQueryService.offsets.get(key).get(id);
            if (offsets != null) {
                parcelsOffsets.put(id, offsets);
            } else {
                logger.warning(String.format("Parcel '%s' not found in the data file '%s'",
                                             id, dataFileName));
            }
        }
        return readParcels(dataFileName, parcelsOffsets);
    }

    private String getDataFile(int siteId, int year, Practice practice) throws IOException {
        String productPath = Database.getL4CProduct(siteId, year, practice);
        logger.finest(String.format("Using product %s", productPath));
        if (productPath == null || !Files.exists(Paths.get(productPath))) {
            throw new IOException(String.format("L4C Product [%s] not found", productPath));
        }
        Path vectDataPath = Paths.get(productPath).resolve("VECTOR_DATA");
        if (!Files.exists(vectDataPath)) {
            throw new IOException(String.format("L4C Product [%s] VECTOR_DATA cannot be accessed", productPath));
        }
        String vectDataPathStr = vectDataPath.toString();
        logger.finest(String.format("Checking for PLOT file in %s", vectDataPathStr));
        File dir = new File(vectDataPathStr);
        File [] files = dir.listFiles((d, s) ->
            s.contains(practice.toString()) && s.contains("_PLOT") && s.toLowerCase().endsWith(".xml")
        );
        if (files.length == 0) {
            throw new IOException(String.format("No plot found in product [%s]", vectDataPathStr));
        }

        logger.finest(String.format("Using PLOT file %s", files[0].getAbsolutePath()));
        return files[0].getAbsolutePath();
    }

    private void readIndexFile(int key, String indexFileName) throws IOException {
        Map<String, Tuple<Long, Long>> map = new HashMap<>();
        try (Stream<String> stream = Files.lines(Paths.get(indexFileName))) {
            stream.forEach(line -> {
                int idx1 = line.indexOf(";");
                int idx2 = line.indexOf(";", idx1 + 1);
                if (idx1 == -1 || idx2 == -1) {
                    logger.warning(String.format("Invalid record [%s]", line));
                    return;
                }
                try {
                    map.put(line.substring(0, idx1),
                            new Tuple<>(Long.parseLong(line.substring(idx1 + 1, idx2)),
                                        Long.parseLong(line.substring(idx2 + 1))));
                } catch (NumberFormatException nfe) {
                    logger.warning(String.format("Invalid record [%s]", line));
                }
            });
        }
        offsets.put(key, map);
    }

    private List<ParcelInfo> readParcels(String dataFileName,
                                         Map<String, Tuple<Long, Long>> parcelOffsets) throws IOException {
        List<ParcelInfo> parcels;
        try (RandomAccessFile raf = new RandomAccessFile(dataFileName, "r")) {
            parcels = new ArrayList<>();
            List<Map.Entry<String, Tuple<Long, Long>>> sortedOffsets =
                    parcelOffsets.entrySet().stream()
                            .sorted(Comparator.comparingLong(o -> o.getValue().getKeyOne())).collect(Collectors.toList());
            int idx;
            byte[] buffer;
            for (Map.Entry<String, Tuple<Long, Long>> entry : sortedOffsets) {
                Tuple<Long, Long> entryValue = entry.getValue();
                raf.seek(entryValue.getKeyOne());
                Long keyTwo = entryValue.getKeyTwo();
                buffer = new byte[keyTwo.intValue()];
                idx = 0;
                while (idx < buffer.length) {
                    buffer[idx++] = raf.readByte();
                }
                String value = new String(buffer);
                try {
                    parcels.add(bufferToEntity(value));
                } catch (SerializationException ex) {
                    logger.warning(String.format("Invalid record format [%s]", value));
                }
            }
        }
        return parcels;
    }

    private ParcelInfo bufferToEntity(String contents) throws SerializationException {
        return xmlDeserializer.deserialize(contents).toInfo();
    }
}
