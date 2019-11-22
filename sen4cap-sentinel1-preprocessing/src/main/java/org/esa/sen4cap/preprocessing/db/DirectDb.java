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

package org.esa.sen4cap.preprocessing.db;

import org.esa.sen2agri.db.Database;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class DirectDb extends Database {

    public static List<ReportRecord> getS1ProcessingReport(int siteId) throws IOException {
        List<ReportRecord> results = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        try (InputStream is = DirectDb.class.getResourceAsStream("s1_report.sql");
             InputStreamReader streamReader = new InputStreamReader(is);
             BufferedReader reader = new BufferedReader(streamReader)) {
            for (String line; (line = reader.readLine()) != null; ) {
                builder.append(line);
            }
        }
        try (Connection connection = getConnection()) {
            PreparedStatement statement = connection.prepareStatement(builder.toString());
            statement.setInt(1, siteId);
            statement.setInt(2, siteId);
            statement.setInt(3, siteId);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                ReportRecord record = new ReportRecord();
                record.setOrbit(resultSet.getInt(1));
                record.setAcquisitionDate(resultSet.getString(2));
                record.setAcquisition(resultSet.getString(3));
                record.setAcquisitionStatus(resultSet.getString(4));
                record.setIntersectionDate(resultSet.getString(5));
                record.setIntersectionProduct(resultSet.getString(6));
                record.setIntersectionStatus(resultSet.getString(7));
                record.setIntersectionStatus(resultSet.getString(8));
                record.setPolarisation(resultSet.getString(9));
                record.setL2Product(resultSet.getString(10));
                record.setL2Coverage(resultSet.getString(11));
                record.setMinValue((Double) resultSet.getObject(12));
                record.setMaxValue((Double) resultSet.getObject(13));
                record.setMeanValue((Double) resultSet.getObject(14));
                record.setStdDev((Double) resultSet.getObject(15));
                record.setStatusReason(resultSet.getString(16));
                results.add(record);
            }
        } catch (SQLException e) {
            logger.severe(String.format("Cannot produce S1 pre-processing report. Reason: %s", e.getMessage()));
        }
        return results;
    }

    public static List<ReportRecord> getS2ProcessingReport(int siteId) throws IOException {
        List<ReportRecord> results = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        try (InputStream is = DirectDb.class.getResourceAsStream("s2_report.sql");
             InputStreamReader streamReader = new InputStreamReader(is);
             BufferedReader reader = new BufferedReader(streamReader)) {
            for (String line; (line = reader.readLine()) != null; ) {
                builder.append(line);
            }
        }
        try (Connection connection = getConnection()) {
            PreparedStatement statement = connection.prepareStatement(builder.toString());
            statement.setInt(1, siteId);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                ReportRecord record = new ReportRecord();
                record.setOrbit(resultSet.getInt(1));
                record.setAcquisitionDate(resultSet.getString(2));
                record.setAcquisition(resultSet.getString(3));
                record.setStatusReason(resultSet.getString(4));
                record.setAcquisitionStatus(resultSet.getString(5));
                record.setL2Product(resultSet.getString(6));
                record.setClouds(resultSet.getString(7));
                results.add(record);
            }
        } catch (SQLException e) {
            logger.severe(String.format("Cannot produce S2 pre-processing report. Reason: %s", e.getMessage()));
        }
        return results;
    }

    public static List<ReportRecord> getL8ProcessingReport(int siteId) throws IOException {
        List<ReportRecord> results = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        try (InputStream is = DirectDb.class.getResourceAsStream("l8_report.sql");
             InputStreamReader streamReader = new InputStreamReader(is);
             BufferedReader reader = new BufferedReader(streamReader)) {
            for (String line; (line = reader.readLine()) != null; ) {
                builder.append(line);
            }
        }
        try (Connection connection = getConnection()) {
            PreparedStatement statement = connection.prepareStatement(builder.toString());
            statement.setInt(1, siteId);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                ReportRecord record = new ReportRecord();
                record.setOrbit(resultSet.getInt(1));
                record.setAcquisitionDate(resultSet.getString(2));
                record.setAcquisition(resultSet.getString(3));
                record.setStatusReason(resultSet.getString(4));
                record.setAcquisitionStatus(resultSet.getString(5));
                record.setL2Product(resultSet.getString(6));
                record.setClouds(resultSet.getString(7));
                results.add(record);
            }
        } catch (SQLException e) {
            logger.severe(String.format("Cannot produce L8 pre-processing report. Reason: %s", e.getMessage()));
        }
        return results;
    }
}
