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
package org.esa.sen4cap.objectstorage.scheduling;

import org.esa.sen2agri.commons.Config;
import org.esa.sen4cap.objectstorage.services.ObjectStorageService;
import org.openstack4j.api.OSClient;
import org.openstack4j.model.common.Identifier;
import org.openstack4j.openstack.OSFactory;

import java.util.logging.Logger;

/**
 * @author Cosmin Cara
 */
public class ObjectStorageAccount {
    private static final Logger logger = Logger.getLogger(ObjectStorageService.class.getSimpleName());
    private static ObjectStorageAccount instance;

    private final String domain;
    private final String user;
    private final String password;
    private final String authenticationURL;
    private final String projectId;
    private String defaultContainerName;

    public static ObjectStorageAccount instance() {
        if (instance == null) {
            boolean complete = true;
            final String domain = Config.getSetting(ConfigurationKeys.OBJECT_STORAGE_DOMAIN, null);
            if (domain == null || domain.isEmpty()) {
                createDefault(ConfigurationKeys.OBJECT_STORAGE_DOMAIN);
                complete = false;
            }
            final String user = Config.getSetting(ConfigurationKeys.OBJECT_STORAGE_USER, null);
            if (user == null || user.isEmpty()) {
                createDefault(ConfigurationKeys.OBJECT_STORAGE_USER);
                complete = false;
            }
            final String password = Config.getSetting(ConfigurationKeys.OBJECT_STORAGE_PASSWORD, null);
            if (password == null || password.isEmpty()) {
                createDefault(ConfigurationKeys.OBJECT_STORAGE_PASSWORD);
                complete = false;
            }
            final String authUrl = Config.getSetting(ConfigurationKeys.OBJECT_STORAGE_URL, null);
            if (authUrl == null || authUrl.isEmpty()) {
                createDefault(ConfigurationKeys.OBJECT_STORAGE_URL);
                complete = false;
            }
            final String projectId = Config.getSetting(ConfigurationKeys.OBJECT_STORAGE_PROJECT_ID, null);
            if (projectId == null || projectId.isEmpty()) {
                createDefault(ConfigurationKeys.OBJECT_STORAGE_PROJECT_ID);
                complete = false;
            }
            final String containerName = Config.getSetting(ConfigurationKeys.OBJECT_STORAGE_CONTAINER, null);
            if (containerName == null || containerName.isEmpty()) {
                createDefault(ConfigurationKeys.OBJECT_STORAGE_CONTAINER);
                complete = false;
            }
            if (!complete) {
                throw new RuntimeException("Object storage account settings are incomplete");
            }
            instance = new ObjectStorageAccount(domain, user, password, authUrl, projectId);
        }
        return instance;
    }

    private static void createDefault(String configKey) {
        logger.warning(String.format("%s is missing from configuration. A default entry was created, please update it",
                                     configKey));
        Config.setSetting(configKey, "");
    }

    private ObjectStorageAccount(String domain, String user, String password, String authenticationURL, String projectId) {
        this.domain = domain;
        this.user = user;
        this.password = password;
        this.authenticationURL = authenticationURL;
        this.projectId = projectId;
    }

    public OSClient.OSClientV3 getContainerService() {
        OSClient.OSClientV3 client = null;
        if (isValid()) {
            client = OSFactory.builderV3()
                    .endpoint(authenticationURL)
                    .credentials(user, password, Identifier.byName(domain))
                    .scopeToProject(Identifier.byId(projectId))
                    .authenticate();
            defaultContainerName = Config.getSetting(ConfigurationKeys.OBJECT_STORAGE_CONTAINER,
                                                     ConfigurationKeys.OBJECT_STORAGE_CONTAINER_DEFAULT);
        }
        return client;
    }

    public String getDefaultContainerName() { return defaultContainerName; }

    public boolean isValid() {
        return domain != null && user != null && password != null &&
                authenticationURL != null && projectId != null;

    }
}
