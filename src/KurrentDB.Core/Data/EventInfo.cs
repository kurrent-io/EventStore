// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

namespace KurrentDB.Core.Data;

public readonly struct EventInfo {
	public readonly long LogPosition;
	public readonly long EventNumber;

	public EventInfo(long logPosition, long eventNumber) {
		LogPosition = logPosition;
		EventNumber = eventNumber;
	}
}
