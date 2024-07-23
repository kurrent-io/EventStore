// ReSharper disable CheckNamespace

using EventStore.Connect.Connectors;
using EventStore.Connect.Consumers;
using EventStore.Connect.Consumers.Configuration;
using EventStore.Connect.Producers;
using EventStore.Connect.Producers.Configuration;
using EventStore.Connect.Readers;
using EventStore.Connect.Readers.Configuration;
using EventStore.Core.Bus;
using EventStore.Streaming.Persistence.State;
using EventStore.Streaming.Schema;
using EventStore.Streaming.Schema.Serializers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventStore.Connect;

public static class ConnectExtensions {
    public static IServiceCollection AddConnectSystemComponents(this IServiceCollection services) {
        services.AddConnectSchemaRegistry(SchemaRegistry.Global);

        services.AddSingleton<Func<SystemReaderBuilder>>(ctx => {
            var publisher      = ctx.GetRequiredService<IPublisher>();
            var loggerFactory  = ctx.GetRequiredService<ILoggerFactory>();
            var schemaRegistry = ctx.GetRequiredService<SchemaRegistry>();

            return () => SystemReader.Builder
                .Publisher(publisher)
                .SchemaRegistry(schemaRegistry)
                .LoggerFactory(loggerFactory);
        });

        services.AddSingleton<Func<SystemConsumerBuilder>>(ctx => {
            var publisher      = ctx.GetRequiredService<IPublisher>();
            var loggerFactory  = ctx.GetRequiredService<ILoggerFactory>();
            var schemaRegistry = ctx.GetRequiredService<SchemaRegistry>();

            return () => SystemConsumer.Builder
                .Publisher(publisher)
                .SchemaRegistry(schemaRegistry)
                .LoggerFactory(loggerFactory);
        });

        services.AddSingleton<Func<SystemProducerBuilder>>(ctx => {
            var publisher      = ctx.GetRequiredService<IPublisher>();
            var loggerFactory  = ctx.GetRequiredService<ILoggerFactory>();
            var schemaRegistry = ctx.GetRequiredService<SchemaRegistry>();

            return () => SystemProducer.Builder
                .Publisher(publisher)
                .SchemaRegistry(schemaRegistry)
                .LoggerFactory(loggerFactory);
        });

        services.AddSingleton<IStateStore, InMemoryStateStore>();

        services.AddSingleton<IConnectorValidator, SystemConnectorsValidation>();
        services.AddSingleton<IConnectorFactory, SystemConnectorsFactory>();

        return services;
    }

    public static IServiceCollection AddConnectSchemaRegistry(this IServiceCollection services, SchemaRegistry? schemaRegistry = null) {
        schemaRegistry ??= SchemaRegistry.Global;

        services.AddSingleton(schemaRegistry);
        services.AddSingleton<ISchemaRegistry>(schemaRegistry);
        services.AddSingleton<ISchemaSerializer>(schemaRegistry);

        return services;
    }
}