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

package org.esa.sen4cap;

import org.esa.sen4cap.preprocessing.Sentinel1Level2Worker;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.scheduling.annotation.EnableScheduling;
import ro.cs.tao.services.commons.StartupBase;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * @author Cosmin Cara
 */
@SpringBootApplication
@EnableScheduling
@ImportResource("classpath:sen4cap-services-context.xml")
public class ServicesStartup extends StartupBase {

    private Logger logger = Logger.getLogger(ServicesStartup.class.getName());

    public static void main(String[] args) throws IOException {
        run(ServicesStartup.class, args);
    }

    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        if (applicationEvent instanceof ContextRefreshedEvent) {
            logger.fine("Spring initialization completed");
            // force static constructor
            Sentinel1Level2Worker.init();
        }
    }
}
