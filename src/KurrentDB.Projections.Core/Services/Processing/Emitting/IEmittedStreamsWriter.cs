// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System;
using System.Security.Claims;
using KurrentDB.Core.Data;
using KurrentDB.Core.Messages;

namespace KurrentDB.Projections.Core.Services.Processing.Emitting;

public interface IEmittedStreamsWriter {
	void WriteEvents(string streamId, long expectedVersion, Event[] events, ClaimsPrincipal writeAs,
		Action<ClientMessage.WriteEventsCompleted> complete);
}
