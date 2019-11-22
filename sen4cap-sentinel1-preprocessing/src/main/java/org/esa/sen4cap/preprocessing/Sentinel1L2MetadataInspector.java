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

import ro.cs.tao.eodata.enums.OrbitDirection;
import ro.cs.tao.eodata.enums.PixelType;
import ro.cs.tao.eodata.metadata.DecodeStatus;
import ro.cs.tao.eodata.metadata.MetadataInspector;
import ro.cs.tao.eodata.metadata.XmlMetadataInspector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Sentinel1L2MetadataInspector extends XmlMetadataInspector {
    @Override
    public DecodeStatus decodeQualification(Path productPath) {
        String fileName = productPath != null ? productPath.getFileName().toString() : null;
        return fileName != null ?
                Files.exists(productPath.getParent().resolve(fileName.replace(".tif", ".mtd"))) ?
                        DecodeStatus.INTENDED : DecodeStatus.UNABLE
                : DecodeStatus.UNABLE;
    }

    @Override
    public Metadata getMetadata(Path productPath) throws IOException {
        if (productPath == null) {
            return null;
        }
        Path metadataFile = productPath.getParent().resolve(productPath.getFileName().toString().replace(".tif", ".mtd"));
        if (!Files.exists(metadataFile)) {
            return null;
        }
        MetadataInspector.Metadata metadata = new MetadataInspector.Metadata();
        metadata.setEntryPoint(productPath.getFileName().toString());
        metadata.setPixelType(PixelType.FLOAT32);
        metadata.setSize(Files.size(productPath));
        try {
            readDocument(metadataFile);
        } catch (Exception e) {
            logger.warning(String.format("Cannot read metadata %s. Reason: %s", metadataFile, e.getMessage()));
            throw new IOException(e);
        }
        metadata.setProductType(getValue("/product/@type"));
        metadata.setOrbitDirection(OrbitDirection.valueOf(getValue("/product/orbitType/text()")));
        metadata.setProductId(getValue("/product/name/text()"));
        metadata.setFootprint(getValue("/product/footprint/text()"));
        return metadata;
    }
}
