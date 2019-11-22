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

package org.esa.sen4cap.shapefile.web;

import org.esa.sen2agri.commons.Config;
import org.esa.sen2agri.db.PersistenceManager;
import org.esa.sen2agri.entities.Job;
import org.esa.sen2agri.entities.Site;
import org.esa.sen2agri.entities.enums.ActivityStatus;
import org.esa.sen2agri.entities.enums.JobStartType;
import org.esa.sen4cap.entities.enums.Processor;
import org.esa.sen4cap.shapefile.web.beans.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import ro.cs.tao.messaging.ProgressNotifier;
import ro.cs.tao.messaging.Topic;
import ro.cs.tao.security.SystemPrincipal;
import ro.cs.tao.services.commons.ControllerBase;
import ro.cs.tao.utils.executors.*;

import java.io.File;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Controller class for various LPIS-related files import
 *
 * @author Cosmin Cara
 * @author Cosmin Udroiu
 */
@Controller
@RequestMapping("/auxdata")
public class ImportController extends ControllerBase {

    private final Set<Integer> tasks = new HashSet<>();

    @Autowired
    private PersistenceManager persistenceManager;

    @RequestMapping(value = "/import/lpis", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> importShapeFile(@RequestParam("siteId") short siteId,
                                             @RequestParam(name = "parcelColumns", required = false) String[] parcelColumns,
                                             @RequestParam(name = "holdingColumns", required = false) String[] holdingColumns,
                                             @RequestParam(name = "cropCodeColumn", required = false) String cropCodeColumn,
                                             @RequestParam(name = "year", required = false) Integer year,
                                             @RequestParam(name = "lpisFile", required = false) String lpisFile,
                                             @RequestParam(name = "lutFile", required = false) String lutFile,
                                             @RequestParam(name = "mode", required = false, defaultValue = "UPDATE") String mode) {
        try {
            if (lpisFile == null && lutFile == null) {
                throw new IllegalArgumentException("At least one of the LPIS shape file or the LUT file have to be present");
            }
            Site site = persistenceManager.getSiteById(siteId);
            if (site == null) {
                throw new IllegalArgumentException(String.format("Site with id %d does not exist", siteId));
            }
            if (lpisFile != null) {
                if (parcelColumns == null || parcelColumns.length == 0) {
                    throw new IllegalArgumentException("Parameter [parcelColumns] is empty");
                } else if (holdingColumns == null || holdingColumns.length == 0) {
                    throw new IllegalArgumentException("Parameter [holdingColumns] is empty");
                } else if (cropCodeColumn == null || cropCodeColumn.isEmpty()) {
                    throw new IllegalArgumentException("Parameter [cropCodeColumn] is empty");
                }
            }
            final String files = (lpisFile != null ? lpisFile : "") + " " + (lutFile != null ? lutFile : "");
            final int taskHashCode = taskStarted(siteId, files);
            if (taskHashCode != 0) {
                final String lpisUploadFilePath = getConfigUploadPath(site, ConfigurationKeys.LPIS_UPLOAD_DIR_CFG_KEY, lpisFile);
                final String lutUploadFilePath = getConfigUploadPath(site, ConfigurationKeys.LUT_UPLOAD_DIR_CFG_KEY, lutFile);
                final LpisImportParams.Mode importMode = LpisImportParams.Mode.valueOf(mode);
                final LpisImportParams arguments = new LpisImportParams(siteId,
                                                                        year != null ? year : LocalDate.now().getYear(),
                                                                        parcelColumns, holdingColumns, cropCodeColumn,
                                                                        lpisUploadFilePath, lutUploadFilePath, importMode);
                final List<String> args = new ArrayList<>();
                args.add(persistenceManager.getSetting((short) 0, ConfigurationKeys.LPIS_IMPORT_SCRIPT_PATH_CFG_KEY, "data-preparation.py"));
                args.addAll(arguments.toArguments());
                asyncExecute(new ImportJob(files, Processor.LPIS, site, year, args, arguments, taskHashCode, this::triggerL4CPracticesExport));
            } else {
                return new ResponseEntity<>(String.format("File(s) %s already submitted and the import is in progress", files),
                                            HttpStatus.BAD_REQUEST);
            }
            return prepareResult("LPIS/LUT import started", HttpStatus.OK);
        } catch (Exception ex) {
            return handleException(ex);
        }
    }

    @RequestMapping(value = "/import/l4ccfg", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> importL4CPracticesConfig(@RequestParam("siteId") short siteId,
                                                      @RequestParam("practices") String practices,
                                                      @RequestParam("country") String country,
                                                      @RequestParam(name = "year", required = false) Integer year,
                                                      @RequestParam(name = "l4cCfgFile") String l4cCfgFile) {
        try {
            Site site = persistenceManager.getSiteById(siteId);
            if (site == null) {
                throw new IllegalArgumentException(String.format("Site with id %d does not exist", siteId));
            }
            if (practices == null || practices.isEmpty()) {
                throw new IllegalArgumentException("Parameter [practices] is empty");
            } else if (country == null || country.isEmpty()) {
                throw new IllegalArgumentException("Parameter [country] is empty");
            }
            final int taskHashCode = taskStarted(siteId, l4cCfgFile);
            if (taskHashCode != 0) {
                final String uploadFilePath = getConfigUploadPath(site, ConfigurationKeys.L4C_CFG_UPLOAD_DIR_CFG_KEY, l4cCfgFile);
                final L4CCfgImportParams arguments = new L4CCfgImportParams(site.getShortName(),
                                                                            year != null ? year : LocalDate.now().getYear(),
                                                                            practices, country, uploadFilePath);
                final List<String> args = new ArrayList<>();
                args.add(persistenceManager.getSetting((short) 0, ConfigurationKeys.L4C_IMPORT_SCRIPT_PATH_CFG_KEY, "s4c_l4c_import_config.py"));
                args.addAll(arguments.toArguments());
                asyncExecute(new ImportJob(l4cCfgFile, Processor.L4C, site, year, args, arguments, taskHashCode, null));
            } else {
                return new ResponseEntity<>(String.format("File %s already submitted and the import is in progress", l4cCfgFile),
                                            HttpStatus.BAD_REQUEST);
            }
            return prepareResult("L4C Configuration import started", HttpStatus.OK);
        } catch (Exception ex) {
            return handleException(ex);
        }
    }

    @RequestMapping(value = "/import/l4cpractice", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> importL4CPractice(@RequestParam("siteId") short siteId,
                                               @RequestParam("practice") String practice,
                                               @RequestParam(name = "year", required = false) Integer year,
                                               @RequestParam(name = "practiceFile") String practiceFile) {
        try {
            Site site = persistenceManager.getSiteById(siteId);
            if (site == null) {
                throw new IllegalArgumentException(String.format("Site with id %d does not exist", siteId));
            }
            if (practice == null || practice.isEmpty()) {
                throw new IllegalArgumentException("Parameter [practices] is empty");
            }
            final int taskHashCode = taskStarted(siteId, practiceFile);
            if (taskHashCode != 0) {
                // for the practices the upload is performed in a subfolder with the name of the practice name
                final String uploadFilePath = getConfigUploadPath(site, ConfigurationKeys.L4C_PRACT_UPLOAD_DIR_CFG_KEY, practiceFile, practice);
                final int actualYear = year != null ? year : LocalDate.now().getYear();
                final L4CPracticeImportParams arguments = new L4CPracticeImportParams(site.getShortName(),
                                                                                      actualYear,
                                                                                      practice, uploadFilePath);
                final List<String> args = new ArrayList<>();
                args.add(persistenceManager.getSetting((short) 0, ConfigurationKeys.L4C_PRACTICES_IMPORT_SCRIPT_PATH_CFG_KEY, "s4c_l4c_import_practice.py"));
                args.addAll(arguments.toArguments());
                asyncExecute(new ImportJob(practiceFile, Processor.L4C, site, year, args, arguments, taskHashCode, this::triggerL4CPracticesExport));
            } else {
                return new ResponseEntity<>(String.format("File %s already submitted and the import is in progress", practiceFile),
                                            HttpStatus.BAD_REQUEST);
            }
            return prepareResult("L4C practice import started", HttpStatus.OK);
        } catch (Exception ex) {
            return handleException(ex);
        }
    }

    @RequestMapping(value = "/import/l4bcfg", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<?> importL4BPracticesConfig(@RequestParam("siteId") short siteId,
                                                      @RequestParam(name = "year", required = false) Integer year,
                                                      @RequestParam(name = "l4bCfgFile") String l4bCfgFile,
                                                      @RequestParam(name = "mowingStartDate", required = false) String mowingStartDate) {
            final Logger logger = Logger.getLogger(ImportController.class.getName());
            try {
                Site site =persistenceManager.getSiteById(siteId);
                if (site == null) {
                    throw new IllegalArgumentException(String.format("Site with id %d does not exist", siteId));
                }
                final int actualYear = year != null ? year: LocalDate.now().getYear();
                final int taskHashCode = taskStarted(siteId, l4bCfgFile);
                if (taskHashCode != 0) {
                    final String uploadFilePath = getConfigUploadPath(site, ConfigurationKeys.L4B_UPLOAD_DIR_CFG_KEY, l4bCfgFile);
                    final L4BCfgImportParams arguments = new L4BCfgImportParams(site.getShortName(), actualYear, uploadFilePath, mowingStartDate);
                    final List<String> args = new ArrayList<>();
                    args.add(persistenceManager.getSetting((short) 0, ConfigurationKeys.L4B_IMPORT_SCRIPT_PATH_CFG_KEY, "s4c_l4b_import_config.py"));
                    args.addAll(arguments.toArguments());
                    asyncExecute(new ImportJob(l4bCfgFile, Processor.L4B, site, year, args, arguments, taskHashCode, null));
                } else {
                    return new ResponseEntity<>(String.format("File %s already submitted and the import is in progress", l4bCfgFile),
                                                HttpStatus.BAD_REQUEST);
                }
                return prepareResult("L4B Configuration import started", HttpStatus.OK);
            } catch (Exception ex) {
                return handleException(ex);
            }
        }

        private int taskStarted(Object... args) {
            int retVal = 0;
            final int taskHashCode = Objects.hash(args);
            if (!tasks.contains(taskHashCode)) {
                tasks.add(taskHashCode);
                retVal = taskHashCode;
            }
            return retVal;
        }

        private void taskCompleted(int taskCode, String fileName) {
            tasks.remove(taskCode);
            info("Processing %s completed", fileName);
        }

        private void triggerL4CPracticesExport(String siteShortName, int year) {
            final L4CPracticesExportParams arguments = new L4CPracticesExportParams(siteShortName, year);
            final List<String> args = new ArrayList<>();
            args.add(persistenceManager.getSetting((short)0, ConfigurationKeys.L4C_PRACTICES_EXPORT_SCRIPT_PATH_CFG_KEY, "s4c_l4c_export_all_practices.py"));
            args.addAll(arguments.toArguments());
            asyncExecute(() -> {
                try {
                    final ExecutionUnit executionUnit = new ExecutionUnit(ExecutorType.PROCESS,
                                                                          InetAddress.getLocalHost().getHostName(), null, null,
                                                                          args, false, SSHMode.EXEC);
                    int code = Executor.execute(null, 3600 * 6, executionUnit);
                    if (code != 0) {
                        warn("L4C practices export ended with code %d", code);
                    } else {
                        debug("L4C practices export ended with code %d", code);
                    }
                } catch (Exception ex) {
                    error("L4C practice import failed. Reason: %s", ex.getMessage());
                }
            });
        }

        private String getConfigUploadPath(Site site, String key, String fileName) {
            return getConfigUploadPath(site, key, fileName, "");
        }
        private String getConfigUploadPath(Site site, String key, String fileName, String subDir) {
            if (fileName == null || fileName.length() == 0) {
                return null;
            }
            // no need to check anything in DB
            if (new File(fileName).isAbsolute()) {
                return fileName;
            }
            String cfgVal = Config.getPersistenceManager().getSetting((short)0, key, "")
                    .replace("{site}", site.getShortName());
            if ("".equals(cfgVal)) {
                final Logger logger = Logger.getLogger(ImportController.class.getName());
                logger.severe("Cannot find in the config table the key " + key);
                throw new IllegalArgumentException(String.format("Cannot find in the config table the key %s", key));
            }
            return Paths.get(cfgVal).resolve(subDir).resolve(fileName).toString();
        }

        private class ImportJob implements Runnable {
            private final Processor processor;
            private final String name;
            private final int taskHashCode;
            private final Site site;
            private final Integer year;
            private final List<String> args;
            private final ImportParams importParams;
            private final BiConsumer<String, Integer> additionalTrigger;

            ImportJob(String name, Processor processor, Site site, Integer year, List<String> args, ImportParams importParams, int hashCode,
                      BiConsumer<String, Integer> additionalCall) {
                this.name = name;
                this.processor = processor;
                this.site = site;
                this.year = year;
                this.args = args;
                this.importParams = importParams;
                this.taskHashCode = hashCode;
                this.additionalTrigger = additionalCall;
            }

            @Override
            public void run() {
                final Map<String, String> info = new HashMap<String, String>() {{
                    put("siteId", String.valueOf(site.getId()));
                    put("siteName", site.getName());
                }};
                final ProgressNotifier progressNotifier = new ProgressNotifier(SystemPrincipal.instance(),
                                                                               ImportController.this,
                                                                               Topic.create(Topic.PROGRESS, "lpis"),
                                                                               info);
                try {
                    Job job = new Job();
                    LocalDateTime timeStamp = LocalDateTime.now();
                    job.setProcessor(persistenceManager.getProcessor(processor.shortName()));
                    job.setSite(site);
                    job.setJobStartType(JobStartType.TRIGGERED);
                    job.setSubmitTimestamp(timeStamp);
                    job.setStartTimestamp(timeStamp);
                    job.setStatus(ActivityStatus.RUNNING);
                    job.setParameters(importParams.toJson());
                    job.setStatusTimestamp(timeStamp);
                    job = persistenceManager.save(job);
                    final ExecutionUnit executionUnit = new ExecutionUnit(ExecutorType.PROCESS,
                                                                          InetAddress.getLocalHost().getHostName(), null, null,
                                                                          args, false, SSHMode.EXEC);
                    final OutputConsumer consumer = new OutputConsumer() {
                        private final Pattern progressPattern = Pattern.compile("((?:\\w|\\s)+): (\\d{1,3}\\.\\d{2}|\\d{1,3})%");
                        private final Pattern errorPattern = Pattern.compile("(?:ERROR:)(.+)");

                        @Override
                        public void consume(String message) {
                            Matcher matcher;
                            if ((matcher = progressPattern.matcher(message)).find()) {
                                int worked;
                                String subTask = matcher.group(1);
                                try {
                                    worked = Integer.parseInt(matcher.group(2));
                                } catch (Exception e) {
                                    worked = (int) Float.parseFloat(matcher.group(2));
                                }
                                if (worked == 0) {
                                    progressNotifier.subActivityStarted(subTask);
                                } else {
                                    progressNotifier.notifyProgress(subTask, (double) worked / 100);
                                }
                            } else if (errorPattern.matcher(message).find()) {
                                debug("%s import failed. Reason: %s", name, message);
                            }
                        }
                    };
                    progressNotifier.started(name);
                    int code = Executor.execute(consumer, 3600 * 6, executionUnit);
                    if (code == 0) {
                        job.setStatus(ActivityStatus.FINISHED);
                    } else {
                        job.setStatus(ActivityStatus.ERROR);
                    }
                    timeStamp = LocalDateTime.now();
                    job.setStatusTimestamp(timeStamp);
                    job.setEndTimestamp(timeStamp);
                    persistenceManager.save(job);
                    if (code != 0) {
                        warn("%s import ended with code %d", name, code);
                    } else {
                        debug("%s import ended with code %d", name, code);
                        if (this.additionalTrigger != null) {
                            this.additionalTrigger.accept(site.getShortName(), year != null ? year : LocalDate.now().getYear());
                        }
                    }
                } catch (Exception ex) {
                    error("% import failed. Reason: %s", name, ex.getMessage());
                } finally {
                    taskCompleted(taskHashCode, name);
                    progressNotifier.ended();
                }
            }
        }
    }