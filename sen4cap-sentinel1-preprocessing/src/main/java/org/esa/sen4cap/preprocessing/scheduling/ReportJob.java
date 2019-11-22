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

package org.esa.sen4cap.preprocessing.scheduling;

import org.esa.sen2agri.commons.Config;
import org.esa.sen2agri.db.Constants;
import org.esa.sen2agri.entities.Site;
import org.esa.sen2agri.entities.enums.Satellite;
import org.esa.sen2agri.scheduling.AbstractJob;
import org.esa.sen2agri.scheduling.JobDescriptor;
import org.esa.sen2agri.services.SiteHelper;
import org.esa.sen4cap.preprocessing.db.DirectDb;
import org.esa.sen4cap.preprocessing.db.ReportRecord;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

public class ReportJob extends AbstractJob {

    private SiteHelper siteHelper;

    public ReportJob() {
        super();
        this.id = "Reports";
    }

    @Override
    public String configKey() { return ConfigurationKeys.REPORTS_ENABLED; }

    @Override
    public String groupName() { return "Reports"; }

    @Override
    public boolean isSingleInstance() { return true; }

    @Override
    public JobDescriptor createDescriptor(int rateInMinutes) {
        int hours = Integer.parseInt(Config.getSetting(ConfigurationKeys.REPORTS_INTERVAL_HOURS, "24"));
        return new JobDescriptor()
                .setName(getId())
                .setGroup(groupName())
                .setFireTime(LocalDateTime.now().plusMinutes(1))
                .setRate(hours * 60);
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        super.execute(jobExecutionContext);
        SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
    }

    @Override
    protected void executeImpl(JobDataMap dataMap) {
        Site site = (Site) dataMap.get("site");
        try {
            if (siteHelper == null) {
                siteHelper = new SiteHelper();
                siteHelper.setPersistenceManager(persistenceManager);
            }
            Path targetPath = Paths.get(Config.getSetting(ConfigurationKeys.S1_PROCESSOR_OUTPUT_PATH,
                                                          Constants.DEFAULT_TARGET_PATH + "/{site}/l2a-s1")
                                                .replace("{site}", site.getShortName()));
            targetPath = targetPath.getParent().resolve("reports");
            Files.createDirectories(targetPath);
            List<ReportRecord> processingReport;
            String fileName;
            Path reportFile;
            if (Config.isFeatureEnabled(site.getId(), String.format(org.esa.sen2agri.db.ConfigurationKeys.DOWNLOADER_SENSOR_ENABLED,
                                                                    Satellite.Sentinel1.friendlyName().toLowerCase()))) {
                fileName = String.format("%s_%s_%s.csv", Satellite.Sentinel1.friendlyName().toLowerCase(), site.getShortName(),
                                         LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")));
                reportFile = targetPath.resolve(fileName);
                try {
                    processingReport = DirectDb.getS1ProcessingReport(site.getId());
                    if (!Files.exists(reportFile)) {
                        Files.write(reportFile, ReportRecord.headers().getBytes(), StandardOpenOption.CREATE);
                    }
                    Files.write(reportFile,
                                processingReport.size() > 0 ?
                                        processingReport.stream().map(ReportRecord::toCSVString).collect(Collectors.joining("\n")).getBytes() :
                                        "No products".getBytes(),
                                StandardOpenOption.APPEND);
                    logger.fine(String.format("Report for S1 pre-processing created at %s", reportFile));
                } catch (IOException e) {
                    logger.severe(String.format("Error during S1 pre-processing report extraction: %s", e.getMessage()));
                }
            }
            if (Config.isFeatureEnabled(site.getId(), String.format(org.esa.sen2agri.db.ConfigurationKeys.DOWNLOADER_SENSOR_ENABLED,
                                                                    Satellite.Sentinel2.friendlyName().toLowerCase()))) {
                fileName = String.format("%s_%s_%s.csv", Satellite.Sentinel2.friendlyName().toLowerCase(), site.getShortName(),
                                         LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")));
                reportFile = targetPath.resolve(fileName);
                try {
                    processingReport = DirectDb.getS2ProcessingReport(site.getId());
                    if (!Files.exists(reportFile)) {
                        Files.write(reportFile, ReportRecord.headers().getBytes(), StandardOpenOption.CREATE);
                    }
                    Files.write(reportFile,
                                processingReport.size() > 0 ?
                                        processingReport.stream().map(ReportRecord::toCSVString).collect(Collectors.joining("\n")).getBytes() :
                                        "No products".getBytes(),
                                StandardOpenOption.APPEND);
                    logger.fine(String.format("Report for S2 pre-processing created at %s", reportFile));
                } catch (IOException e) {
                    logger.severe(String.format("Error during S2 pre-processing report extraction: %s", e.getMessage()));
                }
            }
            if (Config.isFeatureEnabled(site.getId(), String.format(org.esa.sen2agri.db.ConfigurationKeys.DOWNLOADER_SENSOR_ENABLED,
                                                                    Satellite.Landsat8.friendlyName().toLowerCase()))) {
                fileName = String.format("%s_%s_%s.csv", Satellite.Landsat8.friendlyName().toLowerCase(), site.getShortName(),
                                         LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")));
                reportFile = targetPath.resolve(fileName);
                try {
                    processingReport = DirectDb.getL8ProcessingReport(site.getId());
                    if (!Files.exists(reportFile)) {
                        Files.write(reportFile, ReportRecord.headers().getBytes(), StandardOpenOption.CREATE);
                    }
                    Files.write(reportFile,
                                processingReport.size() > 0 ?
                                        processingReport.stream().map(ReportRecord::toCSVString).collect(Collectors.joining("\n")).getBytes() :
                                        "No products".getBytes(),
                                StandardOpenOption.APPEND);
                    logger.fine(String.format("Report for L8 pre-processing created at %s", reportFile));
                } catch (IOException e) {
                    logger.severe(String.format("Error during L8 pre-processing report extraction: %s", e.getMessage()));
                }
            }
        } catch (IOException e) {
            logger.severe(e.getMessage());
        }
    }
}
