// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

namespace KurrentDB.Core.Tests.Services.Replication.LogReplication;

internal record ReplicationAck {
	public long ReplicationPosition { get; init; }
	public long WriterPosition { get; init; }
}
