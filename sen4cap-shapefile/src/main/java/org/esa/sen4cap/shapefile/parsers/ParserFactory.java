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

package org.esa.sen4cap.shapefile.parsers;

import org.esa.sen2agri.commons.Config;
import org.esa.sen2agri.entities.Site;
import org.esa.sen4cap.shapefile.db.DataType;
import org.esa.sen4cap.shapefile.declaration.DECLARATION;
import org.esa.sen4cap.shapefile.lpis.LPIS;
import org.reflections.Reflections;

import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * @author Cosmin Cara
 */
public abstract class ParserFactory {
    private static final Map<DataType, ParserFactory> instances;

    static {
        instances = new HashMap<>();
        instances.put(DataType.LPIS, new LPISParserFactory());
        //noinspection StaticInitializerReferencesSubClass
        instances.put(DataType.DECLARATION, new GSAAParserFactory());
    }

    private final Map<Short, Class<?>> parsers;

    private ParserFactory() {
        parsers = initialize();
    }

    public static Parser create(DataType type, Site site, Path shapeFilePath) {
        final Class<?> parserClass = instances.get(type).parsers.get(site.getId());
        if (parserClass == null) {
            throw new NoClassDefFoundError(String.format("No parser class found for site '%s'", site.getName()));
        }
        try {
            final Constructor constructor = parserClass.getConstructors()[0];//(Site.class, Path.class);
            return (Parser) constructor.newInstance(site, shapeFilePath);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Cannot instantiate parser for site '%s'", site.getName()));
        }
    }

    protected abstract Map<Short, Class<?>> initialize();

    private static class LPISParserFactory extends ParserFactory {

        private LPISParserFactory() { super(); }

        @Override
        protected Map<Short, Class<?>> initialize() {
            Map<Short, Class<?>> parsers = new HashMap<>();
            final List<Site> sites = Config.getPersistenceManager().getEnabledSites();
            Reflections reflections = new Reflections("org.esa.sen2agri.shapefile.lpis");
            final Set<Class<?>> annotatedTypes = reflections.getTypesAnnotatedWith(LPIS.class);
            annotatedTypes.forEach(type -> {
                String countryCode = type.getAnnotation(LPIS.class).countryCode();
                Site site = sites.stream().filter(s -> countryCode.equals(s.getShortName())).findFirst().orElse(null);
                if (site != null) {
                    parsers.put(site.getId(), type);
                } else {
                    Logger.getLogger(LPISParserFactory.class.getSimpleName())
                            .warning(String.format("No parser found for site '%s'", countryCode));
                }
            });
            return parsers;
        }
    }

    private static class GSAAParserFactory extends ParserFactory {

        private GSAAParserFactory() { super(); }

        @Override
        protected Map<Short, Class<?>> initialize() {
            Map<Short, Class<?>> parsers = new HashMap<>();
            final List<Site> sites = Config.getPersistenceManager().getEnabledSites();
            Reflections reflections = new Reflections("org.esa.sen2agri.shapefile.declaration");
            final Set<Class<?>> annotatedTypes = reflections.getTypesAnnotatedWith(DECLARATION.class);
            annotatedTypes.forEach(type -> {
                String countryCode = type.getAnnotation(DECLARATION.class).countryCode();
                Site site = sites.stream().filter(s -> countryCode.equals(s.getShortName())).findFirst().orElse(null);
                if (site != null) {
                    parsers.put(site.getId(), type);
                } else {
                    Logger.getLogger(GSAAParserFactory.class.getSimpleName())
                            .warning(String.format("No parser found for site '%s'", countryCode));
                }
            });
            return parsers;
        }
    }

}
