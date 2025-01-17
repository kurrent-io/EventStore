// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DotNext.Runtime.CompilerServices;
using DotNext.Threading;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Services.Archive.Storage;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Chunks.TFChunk;
using Serilog;

namespace EventStore.Core.Services.Archive.Archiver;

public sealed class ArchiverService :
	IHandle<SystemMessage.ChunkCompleted>,
	IHandle<SystemMessage.ChunkSwitched>,
	IHandle<ReplicationTrackingMessage.ReplicatedTo>,
	IHandle<SystemMessage.BecomeShuttingDown>,
	IAsyncDisposable
{
	private static readonly ILogger Log = Serilog.Log.ForContext<ArchiverService>();

	private readonly IArchiveStorage _archive;
	private readonly CancellationToken _lifetimeToken;
	private readonly AsyncAutoResetEvent _archivingSignal;
	private readonly Task _archivingTask;

	private long _replicationPosition; // volatile
	private volatile CancellationTokenSource _cts;

	public ArchiverService(
		ISubscriber mainBus,
		IArchiveStorage archiveStorage,
		TFChunkManager chunkManager) {
		_archive = archiveStorage;
		_cts = new();
		_lifetimeToken = _cts.Token;
		_archivingSignal = new(initialState: false);
		_archivingTask = ArchiveAsync(chunkManager, _lifetimeToken);

		mainBus.Subscribe<SystemMessage.ChunkSwitched>(this);
		mainBus.Subscribe<SystemMessage.ChunkCompleted>(this);
		mainBus.Subscribe<ReplicationTrackingMessage.ReplicatedTo>(this);
		mainBus.Subscribe<SystemMessage.BecomeShuttingDown>(this);
	}

	public void Handle(SystemMessage.ChunkCompleted message) {
		_archivingSignal.Set();
	}

	public void Handle(SystemMessage.ChunkSwitched message) {
		// TBD
	}

	public void Handle(ReplicationTrackingMessage.ReplicatedTo message) {
		Atomic.AccumulateAndGet(ref _replicationPosition, message.LogPosition, long.Max);
		_archivingSignal.Set();
	}

	[AsyncMethodBuilder(typeof(SpawningAsyncTaskMethodBuilder))]
	private async Task ArchiveAsync(TFChunkManager manager, CancellationToken token) {
		var checkpoint = await _archive.GetCheckpoint(token);
		while (!token.IsCancellationRequested) {
			var chunk = manager.GetChunkFor(checkpoint);
			if (chunk.ChunkFooter.IsCompleted && chunk.ChunkHeader.ChunkEndPosition <= Volatile.Read(in _replicationPosition)) {
				await ArchiveChunkAsync(chunk, token);
				await _archive.SetCheckpoint(checkpoint = chunk.ChunkHeader.ChunkEndPosition, token);
			} else {
				await _archivingSignal.WaitAsync(token);
			}
		}
	}

	private async ValueTask ArchiveChunkAsync(TFChunk chunk, CancellationToken token) {
		await _archive.StoreChunk(chunk.ChunkLocator, chunk.ChunkHeader.ChunkEndNumber, token);
	}

	public void Handle(SystemMessage.BecomeShuttingDown message) {
		if (Interlocked.Exchange(ref _cts, null) is { } cts) {
			using (cts) {
				cts.Cancel();
			}
		}
	}

	public async ValueTask DisposeAsync() {
		try {
			await _archivingTask.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext |
			                                    ConfigureAwaitOptions.SuppressThrowing);
		} finally {
			_archivingSignal.Dispose();
		}
	}
}
