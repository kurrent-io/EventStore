// ReSharper disable CheckNamespace

using EventStore.Core.Bus;
using EventStore.Streaming.Resilience;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventStore.Streaming.Consumers.Configuration;

[PublicAPI]
public record SystemConsumerBuilder : ConsumerBuilder<SystemConsumerBuilder, SystemConsumerOptions, SystemConsumer> {
	public SystemConsumerBuilder Publisher(IPublisher publisher) {
		Ensure.NotNull(publisher);
		return new() {
			Options = Options with {
				Publisher = publisher
			}
		};
	}

	public override SystemConsumer Create() {
		Ensure.NotNullOrWhiteSpace(Options.ConsumerName);
		Ensure.NotNullOrWhiteSpace(Options.SubscriptionName);
		Ensure.NotNull(Options.Publisher);

		var options = Options with {
			Filter = Options.Streams.Length != 0 ? ConsumeFilter.Streams(Options.Streams) : Options.Filter,
			ResiliencePipelineBuilder = Options.ResiliencePipelineBuilder.ConfigureTelemetry(
				Options.EnableLogging
					? Options.LoggerFactory
					: NullLoggerFactory.Instance,
				"ConsumerResilienceTelemetryLogger"
			)
		};

		return new(options);
	}
}