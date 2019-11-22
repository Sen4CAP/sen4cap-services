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

import org.springframework.context.annotation.Configuration;
import ro.cs.tao.services.commons.ServletConfiguration;

import java.util.Map;

@Configuration
public class ServletConfig extends ServletConfiguration {

    @Override
    protected Map<String, String> versionInfo() {
        final Map<String, String> parentEntries = super.versionInfo();
        Map<String, String> entries = this.readManifest();
        if (entries.size() == 0) {
            entries.put("Sen4CAP Services", "running from IDE");
        }
        parentEntries.putAll(entries);
        return parentEntries;
    }
}
