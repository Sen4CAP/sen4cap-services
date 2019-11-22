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

package org.esa.sen4cap.objectstorage.services.internal;

import org.esa.sen2agri.entities.HighLevelProduct;
import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.entities.enums.ProductType;
import org.esa.sen4cap.objectstorage.scheduling.ObjectStorageAccount;
import org.esa.sen4cap.objectstorage.services.ObjectStorageService;
import org.openstack4j.api.OSClient;
import org.openstack4j.model.common.ActionResponse;
import org.openstack4j.model.common.Payloads;
import org.openstack4j.model.storage.object.SwiftObject;
import org.openstack4j.model.storage.object.options.ObjectListOptions;
import org.openstack4j.model.storage.object.options.ObjectPutOptions;
import org.springframework.stereotype.Service;
import ro.cs.tao.EnumUtils;
import ro.cs.tao.utils.FileUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * @author Cosmin Cara
 */
@Service("objectStorageService")
public class ObjectStorageServiceImpl implements ObjectStorageService {

    private Logger logger = Logger.getLogger(ObjectStorageService.class.getSimpleName());

    @Override
    public int delete(String containerName, String keyFilter) throws IOException {
        final ObjectStorageAccount account = ObjectStorageAccount.instance();
        if (!account.isValid()) {
            throw new IOException("The object storage account does not exist or is not valid");
        }
        final OSClient.OSClientV3 containerService = account.getContainerService();
        int deleted = 0;
        try {
            ObjectListOptions options = ObjectListOptions.create().startsWith(keyFilter);
            final List<? extends SwiftObject> objects =
                    containerService.objectStorage().objects().list(containerName, options);
            /*List<String> toDelete = objects.stream()
                                            .filter(o -> o.getName().startsWith(keyFilter))
                                            .map(SwiftObject::getName)
                                            .collect(Collectors.toList());*/
            for (SwiftObject object : objects) {
                try {
                    containerService.objectStorage().objects().delete(containerName, object.getName());
                    deleted++;
                } catch (Exception ex) {
                    logger.severe(String.format("Deleting '%s' failed: %s", object.getName(), ex.getMessage()));
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        return deleted;
    }

    @Override
    public void copyProduct(Site site, HighLevelProduct product, Consumer<HighLevelProduct> callback) throws IOException {
        final ObjectStorageAccount account = ObjectStorageAccount.instance();
        if (!account.isValid()) {
            throw new IOException("The object storage account does not exist or is not valid");
        }
        Path source;
        Path productPath = FileUtilities.resolveSymLinks(Paths.get(product.getFullPath()));
        if (Files.isDirectory(productPath)) {
            source = productPath;
        } else {
            source = productPath.getParent();
        }
        if (Files.exists(source)) {
            final OSClient.OSClientV3 containerService = account.getContainerService();
            final List<Path> paths = list(source);
            final String containerName = site.getShortName() + "_" + product.getCreated().getYear() + "_" + EnumUtils.getEnumConstantByValue(ProductType.class, product.getProductType()).shortName();
            String relativePath;
            ActionResponse actionResponse = null;
            try {
                actionResponse = containerService.objectStorage().containers().create(containerName);
                if (actionResponse != null && !actionResponse.isSuccess()) {
                    throw new IOException(String.format("Failed to connect to container %s. Response code: %d; fault: %s",
                                                        containerName, actionResponse.getCode(), actionResponse.getFault()));
                } else {
                    logger.finest(String.format("Connected to container %s", containerName));
                }
            } catch (Exception ex) {
                throw new IOException(String.format("Container %s does not exist", containerName));
            }
            final List<String> movedPaths = new LinkedList<>();
            String relativeTargetRoot = source.getName(source.getNameCount() - 1).toString();
            for (Path path : paths) {
                try {
                    if (!Files.isDirectory(path)) {
                        relativePath = relativeTargetRoot + "/" + source.relativize(path.getParent()).toString();
                        if (relativePath.endsWith("/")) {
                            relativePath = relativePath.substring(0, relativePath.length() - 1);
                        }
                        relativePath = relativePath.replace("\\", "/");
                        logger.fine(String.format("Copying '%s' to '%s'",
                                                  path.toString(),
                                                  relativePath + path.getFileName().toString()));
                        containerService.objectStorage()
                                .objects().put(containerName,
                                               path.getFileName().toString(),
                                               Files.size(path) > 0 ? Payloads.create(Files.newInputStream(path)) : null,
                                               ObjectPutOptions.create().path(relativePath));
                        movedPaths.add(relativePath);
                    }
                } catch (Exception inner) {
                    logger.severe(String.format("Uploading '%s' failed: %s", path, inner.getMessage()));
                    for (String relPath : movedPaths) {
                        try {
                            containerService.objectStorage()
                                    .objects().delete(containerName, relPath);
                        } catch (Exception ex) {
                            logger.warning(String.format("Removing '%s' failed: %s", relPath, ex.getMessage()));
                        }
                    }
                    throw new IOException(inner);
                }
            }
        } else {
            logger.warning(String.format("Path '%s' not found", source.toString()));
        }
        if (callback != null) {
            callback.accept(product);
        }
    }

    @Override
    public void moveProduct(Site site, HighLevelProduct product, Consumer<HighLevelProduct> productConsumer) throws IOException {
        copyProduct(site, product, null);
        Files.walk(Paths.get(product.getFullPath()), FileVisitOption.FOLLOW_LINKS)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        if (productConsumer != null) {
            productConsumer.accept(product);
        }
    }

    private List<Path> list(Path source) throws IOException {
        final List<Path> files = new LinkedList<>();
        Files.walkFileTree(source, EnumSet.noneOf(FileVisitOption.class), 16,
                           new SimpleFileVisitor<Path>() {
                               @Override
                               public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                                   files.add(dir);
                                   return super.preVisitDirectory(dir, attrs);
                               }
                               @Override
                               public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                   files.add(file);
                                   return super.visitFile(file, attrs);
                               }
                           });
        return files;
    }
}
