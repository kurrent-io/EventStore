// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.IO;
using Dapper;
using DuckDB.NET.Data;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.Duck;

public class DuckDb(TFChunkDbConfig dbConfig) {
	public readonly string ConnectionString = $"Data Source={Path.Combine(dbConfig.Path, "index.db")};";
	readonly ConcurrentBag<DuckDbPooledConnection> _connectionPool = [];

	public void InitDb() {
		using var connection = OpenConnection();
		DuckDbSchema.CreateSchema(connection);
	}

	public DuckDBConnection OpenConnection() {
		var connection = new DuckDBConnection(ConnectionString);
		connection.Open();
		return connection;
	}

	public DuckDbPooledConnection GetOrOpenConnection() {
		if (!_connectionPool.TryTake(out var connection)) {
			connection = new(this);
			connection.Open();

			Debug.Assert(connection.State is ConnectionState.Open);
		}

		return connection;
	}

	public void ReturnConnection(DuckDbPooledConnection connection) {
		Debug.Assert(connection.State is ConnectionState.Open);

		_connectionPool.Add(connection);
	}

	public void Close() {
		using (var connection = OpenConnection()) {
			connection.Execute("checkpoint");
		}

		while (_connectionPool.TryTake(out var connection)) {
			connection.Dispose();
		}
	}
}
