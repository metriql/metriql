package com.metriql.bootstrap;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;

import java.lang.reflect.Field;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ProxyBootstrap
        extends Bootstrap {
    private final static Logger LOGGER = Logger.getLogger(ProxyBootstrap.class.getName());

    public ProxyBootstrap(Set<Module> modules) {
        super(modules);
    }

    @Override
    public Injector initialize() {
        List<Module> installedModules;
        Field modules;
        try {
            modules = Bootstrap.class.getDeclaredField("modules");
            modules.setAccessible(true);
            installedModules = (List<Module>) modules.get(this);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        SystemRegistry systemRegistry = new SystemRegistry(null, new HashSet<>(installedModules));

        List<Module> list = new ArrayList<>();
        list.addAll(installedModules);
        list.add(binder -> binder.bind(SystemRegistry.class).toInstance(systemRegistry));
        try {
            modules.set(this, list);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        String env = System.getProperty("env");
        ArrayList<String> objects = new ArrayList<>();
        if (env != null) {
            LOGGER.info(String.format("Reading environment variables starting with `%s`", env));

            System.getenv().entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(env)).forEach(entry -> {
                String configName = entry.getKey().substring(env.length() + 1)
                        .toLowerCase(Locale.ENGLISH).replaceAll("__", "-").replaceAll("_", ".");
                objects.add(configName);
                this.setOptionalConfigurationProperty(configName, entry.getValue());
            });

            LOGGER.info(String.format("Set the configurations using environment variables (%s)", objects.stream().collect(Collectors.joining(". "))));
        }

        Injector initialize = super.initialize();
        return initialize;
    }
}
