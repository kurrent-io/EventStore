// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System.Threading;
using System.Threading.Tasks;
using KurrentDB.Core.TransactionLog.Chunks;
using KurrentDB.Core.TransactionLog.Scavenging.Data;
using KurrentDB.Core.TransactionLog.Scavenging.Interfaces;

namespace KurrentDB.Core.XUnit.Tests.Scavenge.Infrastructure;

public class TracingChunkExecutor<TStreamId> : IChunkExecutor<TStreamId> {
	private readonly IChunkExecutor<TStreamId> _wrapped;
	private readonly Tracer _tracer;

	public TracingChunkExecutor(IChunkExecutor<TStreamId> wrapped, Tracer tracer) {
		_wrapped = wrapped;
		_tracer = tracer;
	}

	public async ValueTask Execute(
		ScavengePoint scavengePoint,
		IScavengeStateForChunkExecutor<TStreamId> state,
		ITFChunkScavengerLog scavengerLogger,
		CancellationToken cancellationToken) {

		_tracer.TraceIn($"Executing chunks for {scavengePoint.GetName()}");
		try {
			await _wrapped.Execute(scavengePoint, state, scavengerLogger, cancellationToken);
			_tracer.TraceOut("Done");
		} catch {
			_tracer.TraceOut("Exception executing chunks");
			throw;
		}
	}

	public async ValueTask Execute(
		ScavengeCheckpoint.ExecutingChunks checkpoint,
		IScavengeStateForChunkExecutor<TStreamId> state,
		ITFChunkScavengerLog scavengerLogger,
		CancellationToken cancellationToken) {

		_tracer.TraceIn($"Executing chunks from checkpoint: {checkpoint}");
		try {
			await _wrapped.Execute(checkpoint, state, scavengerLogger, cancellationToken);
			_tracer.TraceOut("Done");
		} catch {
			_tracer.TraceOut("Exception executing chunks");
			throw;
		}
	}
}
