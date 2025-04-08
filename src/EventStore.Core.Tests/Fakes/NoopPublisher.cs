// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using KurrentDB.Core.Messaging;
using KurrentDB.Core.Bus;

namespace EventStore.Core.Tests.Fakes;

public class NoopPublisher : IPublisher {
	public void Publish(Message message) {
		// do nothing
	}
}
