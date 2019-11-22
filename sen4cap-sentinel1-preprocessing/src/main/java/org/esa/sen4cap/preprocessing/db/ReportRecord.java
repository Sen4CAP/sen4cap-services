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

public class ReportRecord {
    private int orbit;
    private String acquisitionDate;
    private String acquisition;
    private String acquisitionStatus;
    private String intersectionDate;
    private String intersectionProduct;
    private String intersection;
    private String intersectionStatus;
    private String polarisation;
    private String l2Product;
    private String l2Coverage;
    private Double minValue;
    private Double maxValue;
    private Double meanValue;
    private Double stdDev;
    private String statusReason;
    private String clouds;

    public int getOrbit() {
        return orbit;
    }

    public void setOrbit(int orbit) {
        this.orbit = orbit;
    }

    public String getAcquisitionDate() {
        return acquisitionDate;
    }

    public void setAcquisitionDate(String acquisitionDate) {
        this.acquisitionDate = acquisitionDate;
    }

    public String getAcquisition() {
        return acquisition;
    }

    public void setAcquisition(String acquisition) {
        this.acquisition = acquisition;
    }

    public String getIntersectionDate() {
        return intersectionDate;
    }

    public void setIntersectionDate(String intersectionDate) {
        this.intersectionDate = intersectionDate;
    }

    public String getIntersectionProduct() {
        return intersectionProduct;
    }

    public void setIntersectionProduct(String intersectionProduct) {
        this.intersectionProduct = intersectionProduct;
    }

    public String getIntersection() {
        return intersection;
    }

    public void setIntersection(String intersection) {
        this.intersection = intersection;
    }

    public String getIntersectionStatus() { return intersectionStatus; }

    public void setIntersectionStatus(String intersectionStatus) { this.intersectionStatus = intersectionStatus; }

    public String getAcquisitionStatus() {
        return acquisitionStatus;
    }

    public void setAcquisitionStatus(String acquisitionStatus) {
        this.acquisitionStatus = acquisitionStatus;
    }

    public String getPolarisation() {
        return polarisation;
    }

    public void setPolarisation(String polarisation) {
        this.polarisation = polarisation;
    }

    public String getL2Product() {
        return l2Product;
    }

    public void setL2Product(String l2Product) {
        this.l2Product = l2Product;
    }

    public String getL2Coverage() {
        return l2Coverage;
    }

    public void setL2Coverage(String l2Coverage) {
        this.l2Coverage = l2Coverage;
    }

    public Double getMinValue() {
        return minValue;
    }

    public void setMinValue(Double minValue) {
        this.minValue = minValue;
    }

    public Double getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(Double maxValue) {
        this.maxValue = maxValue;
    }

    public Double getMeanValue() {
        return meanValue;
    }

    public void setMeanValue(Double meanValue) {
        this.meanValue = meanValue;
    }

    public Double getStdDev() {
        return stdDev;
    }

    public void setStdDev(Double stdDev) {
        this.stdDev = stdDev;
    }

    public String getStatusReason() {
        return statusReason;
    }

    public void setStatusReason(String statusReason) {
        this.statusReason = statusReason;
    }

    public String getClouds() {
        return clouds;
    }

    public void setClouds(String clouds) {
        this.clouds = clouds;
    }

    public String toCSVString() {
        return orbit + ","
                + acquisitionDate + ","
                + acquisition + ","
                + (acquisitionStatus != null ? acquisitionStatus : "") + ","
                + (intersectionDate != null ? intersectionDate : "") + ","
                + (intersectionProduct != null ? intersectionProduct : "") + ","
                + (intersection != null ? intersection : "") + ","
                + (intersectionStatus != null ? intersectionStatus : "") + ","
                + (polarisation != null ? polarisation : "") + ","
                + (l2Product != null ? l2Product : "") + ","
                + (l2Coverage != null ? l2Coverage : "") + ","
                + (minValue != null ? minValue.toString() : "") + ","
                + (maxValue != null ? maxValue.toString() : "") + ","
                + (meanValue != null ? meanValue.toString() : "") + ","
                + (stdDev != null ? stdDev.toString() : "") + ","
                + (statusReason != null ? statusReason : "") + ","
                + (clouds != null ? clouds : "");
    }

    public static String headers() {
        return "orbit,acquisitionDate,acquisition,acquisitionStatus,intersectionDate,intersectionProduct," +
                "intersection,intersectionStatus,polarisation,l2Product,l2Coverage,minValue,maxValue,meanValue,stdDev," +
                "statusReason";
    }
}
