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

import ro.cs.tao.datasource.ProductPathBuilder;
import ro.cs.tao.eodata.EOProduct;
import ro.cs.tao.serialization.BaseSerializer;
import ro.cs.tao.serialization.MediaType;
import ro.cs.tao.serialization.SerializationException;
import ro.cs.tao.serialization.SerializerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class PathTest {
    private static EOProduct l8Product;
    private static EOProduct s1Product;
    private static EOProduct s2Product;

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 4) {
            System.out.println("Usage: \njava -cp '../modules/*:../lib/*:../services/*:../plugins/*' org.esa.sen4cap.PathTest <s1_path> <s2_path> <s3_path>");
            System.exit(-1);
        }
        Properties s1Props = readProperties(args[0], "SciHubDataSource.Sentinel1");
        Properties s2Props = readProperties(args[0], "SciHubDataSource.Sentinel2");
        Properties l8Props = readProperties(args[0], "USGSDataSource.Landsat8");
        readEOProducts();

        /*Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(Level.FINEST);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINEST);
        rootLogger.addHandler(handler);*/

        String localPathFormat = s1Props.getProperty(ProductPathBuilder.LOCAL_ARCHIVE_PATH_FORMAT);
        String className = s1Props.getProperty(ProductPathBuilder.PATH_BUILDER_CLASS);
        ProductPathBuilder pathBuilder = createBuilder(className, Paths.get(args[1]), localPathFormat, s1Props);
        Path productPath = pathBuilder.getProductPath(Paths.get(args[1]), s1Product);
        System.out.println((Files.exists(productPath) ? "OK" : "NOK") + ": " + productPath);

        className = s2Props.getProperty(ProductPathBuilder.PATH_BUILDER_CLASS);
        localPathFormat = s2Props.getProperty(ProductPathBuilder.LOCAL_ARCHIVE_PATH_FORMAT);
        pathBuilder = createBuilder(className, Paths.get(args[2]), localPathFormat, s2Props);
        productPath = pathBuilder.getProductPath(Paths.get(args[2]), s2Product);
        System.out.println((Files.exists(productPath) ? "OK" : "NOK") + ": " + productPath);

        localPathFormat = l8Props.getProperty(ProductPathBuilder.LOCAL_ARCHIVE_PATH_FORMAT);
        className = l8Props.getProperty(ProductPathBuilder.PATH_BUILDER_CLASS);
        pathBuilder = createBuilder(className, Paths.get(args[3]), localPathFormat, l8Props);
        productPath = pathBuilder.getProductPath(Paths.get(args[3]), l8Product);
        System.out.println((Files.exists(productPath) ? "OK" : "NOK") + ": " + productPath);
    }

    private static ProductPathBuilder createBuilder(String className, Path path, String format, Properties props) throws Exception {
        Constructor<?> constructor = Class.forName(className).getDeclaredConstructor(Path.class, String.class, Properties.class, Boolean.TYPE);
        return (ProductPathBuilder) constructor.newInstance(path, format, props, true);
    }

    private static void readEOProducts() throws SerializationException {
        BaseSerializer<EOProduct> serializer = SerializerFactory.create(EOProduct.class, MediaType.JSON);
        /*l8Product = serializer.deserialize(new StreamSource(PathTest.class.getResourceAsStream("/testFiles/LC08_L1TP_189031_20170708_20170716_01_T1.json")));
        s1Product = serializer.deserialize(new StreamSource(PathTest.class.getResourceAsStream("/testFiles/S1A_IW_SLC__1SDV_20170712T165713_20170712T165740_017441_01D251_D248.json")));
        s2Product = serializer.deserialize(new StreamSource(PathTest.class.getResourceAsStream("/testFiles/S2A_MSIL1C_20170707T095031_N0205_R079_T33TVF_20170707T095257.json")));*/
    }

    private static Properties readProperties(String configFile, String prefix) throws IOException {
        Properties properties = new Properties();
        Path configPath = Paths.get(configFile).toAbsolutePath();
        properties.load(Files.newInputStream(configPath));
        Properties toReturn = new Properties();
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                toReturn.setProperty(key.replace(prefix + ".", ""), properties.getProperty(key));
            }
        }
        return toReturn;
    }

}
