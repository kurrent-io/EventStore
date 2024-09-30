// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using EventStore.ClientAPI;
using EventStore.Common.Utils;

namespace EventStore.Core.Tests.ClientAPI.Helpers {
	internal static class EventDataComparer {
		public static bool Equal(EventData expected, RecordedEvent actual) {
			if (expected.EventId != actual.EventId)
				return false;

			if (expected.Type != actual.EventType)
				return false;

			var expectedDataString = Helper.UTF8NoBom.GetString(expected.Data ?? new byte[0]);
			var expectedMetadataString = Helper.UTF8NoBom.GetString(expected.Metadata ?? new byte[0]);

			var actualDataString = Helper.UTF8NoBom.GetString(actual.Data ?? new byte[0]);
			var actualMetadataDataString = Helper.UTF8NoBom.GetString(actual.Metadata ?? new byte[0]);

			return expectedDataString == actualDataString && expectedMetadataString == actualMetadataDataString;
		}

		public static bool Equal(EventData[] expected, RecordedEvent[] actual) {
			if (expected.Length != actual.Length)
				return false;

			for (var i = 0; i < expected.Length; i++) {
				if (!Equal(expected[i], actual[i]))
					return false;
			}

			return true;
		}
	}
}
