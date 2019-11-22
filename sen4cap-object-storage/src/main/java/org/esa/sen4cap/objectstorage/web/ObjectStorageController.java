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

package org.esa.sen4cap.objectstorage.web;

import org.esa.sen2agri.commons.Config;
import org.esa.sen2agri.db.PersistenceManager;
import org.esa.sen2agri.entities.HighLevelProduct;
import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.entities.enums.ProductType;
import org.esa.sen4cap.objectstorage.services.ObjectStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import ro.cs.tao.services.commons.ControllerBase;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

/**
 * Controller class for Object Storage operations
 *
 * @author Cosmin Cara
 */
@Controller
@RequestMapping("/objectstorage")
public class ObjectStorageController extends ControllerBase {

    @Autowired
    private ObjectStorageService objectStorageService;

    @Autowired
    private PersistenceManager persistenceManager;

    /**
     * Returns the product type ids that should be moved to Object Storage buckets
     */
    @RequestMapping(value = "/productTypes/", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<String[]> getProductTypesForObjectStorage() {
        return new ResponseEntity<>(Config.getSettingValues("scheduled.object.storage.move.product.types",
                                                            new String[0]),
                                    HttpStatus.OK);
    }

    /**
     * Sets the product type ids that should be moved to Object Storage buckets
     * @param productTypeIds    The product type ids
     */
    @RequestMapping(value = "/productTypes/", method = RequestMethod.POST, produces = "application/json")
    public ResponseEntity<?> setProductTypesForObjectStorage(@RequestParam(name = "productTypeIds") String[] productTypeIds) {
        if (productTypeIds != null && productTypeIds.length > 0) {
            Config.setSetting("scheduled.object.storage.move.product.types", String.join(";", productTypeIds));
        }
        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * Deletes from an Object Storage bucket the entries that match the given filter
     * @param container The Object Storage bucket
     * @param filter    The filter to be applied
     * @return  The number of objects deleted
     */
    @RequestMapping(value = "/delete", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> delete(@RequestParam("container") String container,
                                    @RequestParam("filter") String filter) {
        int deleted = 0;
        if (filter != null) {
            try {
                deleted = objectStorageService.delete(URLDecoder.decode(container, StandardCharsets.UTF_8.toString()),
                                                      URLDecoder.decode(filter, StandardCharsets.UTF_8.toString()));
            } catch (Exception e) {
                error("Request failed: %s", e.getMessage());
                return new ResponseEntity<>("Request failed: " + e.getMessage(), HttpStatus.BAD_REQUEST);
            }
        }
        return new ResponseEntity<>((deleted == 0 ?
                                        "No object was " : deleted + " objects were ") + "deleted",
                                    HttpStatus.OK);
    }

    /**
     * Copies the given file, associated to the given site and representing the given product type, to Object Storage
     * @param siteId        The site identifier
     * @param productType   The product type identifier
     * @param year          The year of the product
     * @param file          The file representing the product
     */
    @RequestMapping(value = "/copy", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> copy(@RequestParam("siteId") short siteId,
                                  @RequestParam("productType") ProductType productType,
                                  @RequestParam("year") int year,
                                  @RequestParam("file") String file) {
        if (file != null) {
            try {
                final Site site = persistenceManager.getSiteById(siteId);
                if (site == null) {
                    throw new Exception(String.format("No such site [id=%d]", siteId));
                }
                HighLevelProduct fakeProduct = new HighLevelProduct();
                fakeProduct.setProductType(productType.value());
                fakeProduct.setCreated(LocalDateTime.of(year, 1, 1, 0, 0));
                fakeProduct.setSiteId(siteId);
                fakeProduct.setFullPath(URLDecoder.decode(file.endsWith("_SAFE") ?
                                                                  file.replace("_SAFE", ".SAFE") : file,
                                                          StandardCharsets.UTF_8.toString()));
                info("Received request for copying '%s' to object storage", fakeProduct.getFullPath());
                asyncExecute(() -> {
                    try {
                        objectStorageService.copyProduct(site, fakeProduct, this::operationCompleted);
                    } catch (Exception e) {
                        error("Request failed: %s", e.getMessage());
                    }
                });
            } catch (Exception e) {
                error("Request failed: %s", e.getMessage());
                return new ResponseEntity<>("Request failed: " + e.getMessage(), HttpStatus.BAD_REQUEST);
            }
        }
        return new ResponseEntity<>("Request has been submitted. Please consult the service log files for the status",
                                    HttpStatus.OK);
    }

    /**
     * Moves the given file, associated to the given site and representing the given product type, to Object Storage
     * @param siteId        The site identifier
     * @param productType   The product type identifier
     * @param year          The year of the product
     * @param file          The file representing the product
     */
    @RequestMapping(value = "/move", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> move(@RequestParam("siteId") short siteId,
                                  @RequestParam("productType") ProductType productType,
                                  @RequestParam("year") int year,
                                  @RequestParam("file") String file) {
        if (file != null) {
            try {
                final Site site = persistenceManager.getSiteById(siteId);
                if (site == null) {
                    throw new Exception(String.format("No such site [id=%d]", siteId));
                }
                HighLevelProduct fakeProduct = new HighLevelProduct();
                fakeProduct.setProductType(productType.value());
                fakeProduct.setCreated(LocalDateTime.of(year, 1, 1, 0, 0));
                fakeProduct.setSiteId(siteId);
                fakeProduct.setFullPath(URLDecoder.decode(file.endsWith("_SAFE") ?
                                                                  file.replace("_SAFE", ".SAFE") : file,
                                                          StandardCharsets.UTF_8.toString()));
                info("Received request for moving '%s' to object storage", fakeProduct.getFullPath());
                asyncExecute(() -> {
                    try {
                        objectStorageService.copyProduct(site, fakeProduct, this::operationCompleted);
                    } catch (Exception e) {
                        error("Request failed: %s", e.getMessage());
                    }
                });
            } catch (Exception e) {
                error("Request failed: %s", e.getMessage());
                return new ResponseEntity<>("Request failed: " + e.getMessage(), HttpStatus.BAD_REQUEST);
            }
        }
        return new ResponseEntity<>("Request has been submitted. Please consult the service log files for the status",
                                    HttpStatus.OK);
    }

    private void operationCompleted(HighLevelProduct product) {
        info("Request for '%s' completed", product.getFullPath());
    }
}
