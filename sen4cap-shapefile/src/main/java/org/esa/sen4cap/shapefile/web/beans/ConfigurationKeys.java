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

package org.esa.sen4cap.shapefile.web.beans;

public class ConfigurationKeys {
    public static final String LPIS_IMPORT_SCRIPT_PATH_CFG_KEY = "executor.module.path.lpis_import";
    public static final String L4B_IMPORT_SCRIPT_PATH_CFG_KEY = "executor.module.path.l4b_cfg_import";
    public static final String L4C_IMPORT_SCRIPT_PATH_CFG_KEY = "executor.module.path.l4c_cfg_import";
    public static final String L4C_PRACTICES_IMPORT_SCRIPT_PATH_CFG_KEY = "executor.module.path.l4c_practices_import";
    public static final String L4C_PRACTICES_EXPORT_SCRIPT_PATH_CFG_KEY = "executor.module.path.l4c_practices_export";

    public static final String LPIS_UPLOAD_DIR_CFG_KEY = "processor.lpis.upload_path";
    public static final String LUT_UPLOAD_DIR_CFG_KEY = "processor.lpis.lut_upload_path";
    public static final String L4B_UPLOAD_DIR_CFG_KEY = "processor.s4c_l4b.cfg_upload_dir";
    public static final String L4C_CFG_UPLOAD_DIR_CFG_KEY = "processor.s4c_l4c.cfg_upload_dir";
    public static final String L4C_PRACT_UPLOAD_DIR_CFG_KEY = "processor.s4c_l4c.ts_input_tables_upload_root_dir";
}
