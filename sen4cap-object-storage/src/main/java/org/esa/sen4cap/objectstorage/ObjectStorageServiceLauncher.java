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
package org.esa.sen4cap.objectstorage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;
import org.springframework.scheduling.annotation.EnableScheduling;
import ro.cs.tao.services.commons.ServiceLauncher;

/**
 * @author Cosmin Cara
 */
@SpringBootApplication
@EnableScheduling
@ImportResource("classpath:object-storage-service-context.xml")
public class ObjectStorageServiceLauncher implements ServiceLauncher {

    public static void main(String[] args) {
        SpringApplication.run(ObjectStorageServiceLauncher.class, args);
    }

    @Override
    public String serviceName() { return "Object Storage Service"; }
}