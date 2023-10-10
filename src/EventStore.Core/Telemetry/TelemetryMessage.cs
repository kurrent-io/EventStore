using System.Text.Json.Nodes;
using EventStore.Common.Utils;
using EventStore.Core.Messaging;

namespace EventStore.Core.Telemetry;

[DerivedMessage]
public abstract partial class TelemetryMessage : Message {
	[DerivedMessage(CoreMessage.Telemetry)]
	public partial class Request : TelemetryMessage {
		public readonly IEnvelope<Response> Envelope;

		public Request(IEnvelope<Response> envelope) {
			Ensure.NotNull(envelope, "envelope");

			Envelope = envelope;
		}
	}

	[DerivedMessage(CoreMessage.Telemetry)]
	public partial class Response : TelemetryMessage {
		public readonly string Key;
		public readonly JsonNode Value;

		public Response(string key, JsonNode value) {
			Ensure.NotNullOrEmpty(key, "key");
			Ensure.NotNull(value, "value");

			Key = key;
			Value = value;
		}
	}

	[DerivedMessage(CoreMessage.Telemetry)]
	public partial class Collect : TelemetryMessage { }

	[DerivedMessage(CoreMessage.Telemetry)]
	public partial class Flush : TelemetryMessage { }
}
