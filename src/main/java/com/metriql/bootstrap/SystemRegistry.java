package com.metriql.bootstrap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationInspector;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SystemRegistry
{
    private final Set<Module> installedModules;
    private final Set<Module> allModules;

    private List<ModuleDescriptor> moduleDescriptors;

    public SystemRegistry(Set<Module> allModules, Set<Module> installedModules)
    {
        this.allModules = allModules;
        this.installedModules = installedModules;
    }

    private List<ModuleDescriptor> createModuleDescriptor()
    {
        return (allModules == null ? installedModules : allModules).stream()
                .filter(module -> module instanceof AppModule)
                .map(module -> {
                    AppModule appModule = (AppModule) module;
                    Optional<ModuleDescriptor.Condition> condition = Optional.empty();

                    ConfigurationFactory otherConfigurationFactory = new ConfigurationFactory(ImmutableMap.of());

                    // process modules and add used properties to ConfigurationFactory

                    otherConfigurationFactory.validateRegisteredConfigurationProvider();

                    ImmutableList.Builder<ConfigItem> attributesBuilder = ImmutableList.builder();

                    ConfigurationInspector configurationInspector = new ConfigurationInspector();
                    for (ConfigurationInspector.ConfigRecord<?> record : configurationInspector.inspect(otherConfigurationFactory)) {
                        for (ConfigurationInspector.ConfigAttribute attribute : record.getAttributes()) {
                            attributesBuilder.add(new ConfigItem(attribute.getPropertyName(), attribute.getDefaultValue(), attribute.getDescription()));
                        }
                    }

                    return new SystemRegistry.ModuleDescriptor(
                            appModule.name(),
                            appModule.description(),
                            appModule.getClass().getName(),
                            installedModules.contains(module),
                            condition,
                            attributesBuilder.build());
                }).collect(Collectors.toList());
    }

    public synchronized List<ModuleDescriptor> getModules()
    {
        if (moduleDescriptors == null) {
            this.moduleDescriptors = createModuleDescriptor();
        }

        return moduleDescriptors;
    }

    public static class ModuleDescriptor
    {
        public final String name;
        public final String description;
        public final String className;
        public final boolean isActive;
        public final Optional<Condition> condition;
        public final List<ConfigItem> properties;

        @JsonCreator
        public ModuleDescriptor(String name,
                String description,
                String className,
                boolean isActive,
                Optional<Condition> condition,
                List<ConfigItem> properties)
        {
            this.name = name;
            this.description = description;
            this.className = className;
            this.isActive = isActive;
            this.condition = condition;
            this.properties = properties;
        }

        public static class Condition
        {
            public final String property;
            public final String expectedValue;

            public Condition(String property,
                    String expectedValue)
            {
                this.property = property;
                this.expectedValue = expectedValue;
            }
        }
    }

    public static class ConfigItem
    {
        public final String property;
        public final String defaultValue;
        public final String description;

        @JsonCreator
        public ConfigItem(String property,
                String defaultValue,
                String description)
        {
            this.property = property;
            this.defaultValue = defaultValue;
            this.description = description;
        }
    }
}
