// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System;

namespace EventStore.TestClient.Commands.DvuBasic;

internal static class StreamNamesGenerator {
	public static string GenerateName(string original, int index) {
		return string.Format("{0}-{1}", original, index);
	}

	public static string GetOriginalName(string autogenerated) {
		return autogenerated.Substring(0, autogenerated.LastIndexOf('-'));
	}
}
