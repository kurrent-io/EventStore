// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace EventStore.Core.Duck;

public class DuckDbPooledConnection(DuckDb db) : DuckDBConnection(db.ConnectionString) {
	protected override void Dispose(bool disposing) {
		db.ReturnConnection(this);
	}

#pragma warning disable CA1816
	public override ValueTask DisposeAsync() {
#pragma warning restore CA1816
		db.ReturnConnection(this);
		return ValueTask.CompletedTask;
	}
}
