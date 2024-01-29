#nullable enable
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Memory;
using static System.StringComparer;

namespace EventStore.Common.Configuration.Sources;

public class EventStoreDefaultValuesConfigurationSource(IEnumerable<KeyValuePair<string, string?>>? initialData = null) : IConfigurationSource {
	IEnumerable<KeyValuePair<string, string?>> InitialData { get; } = initialData ?? new Dictionary<string, string?>();

	public IConfigurationProvider Build(IConfigurationBuilder builder) => 
		new EventStoreDefaultValuesConfigurationProvider(InitialData);
}

public class EventStoreDefaultValuesConfigurationProvider(IEnumerable<KeyValuePair<string, string?>> initialData)
	: MemoryConfigurationProvider(new() { InitialData = initialData }) {
	const string Prefix = "EventStore";

	public override void Load() {
		Data = Data.Keys.ToDictionary(
			key => $"{Prefix}:{key}", 
			x => Data[x], 
			OrdinalIgnoreCase
		);
	}
}

public static class EventStoreDefaultValuesConfigurationExtensions {
	public static IConfigurationBuilder AddEventStoreDefaultValues(this IConfigurationBuilder configurationBuilder) => 
		configurationBuilder.Add(new EventStoreDefaultValuesConfigurationSource());

	public static IConfigurationBuilder AddEventStoreDefaultValues(this IConfigurationBuilder configurationBuilder, IEnumerable<KeyValuePair<string, string?>> initialData) =>
		configurationBuilder.Add(new EventStoreDefaultValuesConfigurationSource(initialData));

	public static IConfigurationBuilder AddEventStoreDefaultValues(this IConfigurationBuilder builder, IEnumerable<KeyValuePair<string, object?>>? initialData) {
		return initialData is not null
			? builder.AddEventStoreDefaultValues(initialData.Select(x => new KeyValuePair<string, string?>(x.Key, x.Value?.ToString())).ToArray())
			: builder;
	}
}
