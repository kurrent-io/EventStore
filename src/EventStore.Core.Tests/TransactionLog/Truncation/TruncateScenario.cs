// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System;
using System.Threading.Tasks;
using EventStore.Core.Tests.Services.Storage;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.Transforms.Identity;

namespace EventStore.Core.Tests.TransactionLog.Truncation;

public abstract class TruncateScenario<TLogFormat, TStreamId> : ReadIndexTestScenario<TLogFormat, TStreamId> {
	protected TFChunkDbTruncator Truncator;
	protected long TruncateCheckpoint = long.MinValue;

	protected TruncateScenario(int maxEntriesInMemTable = 100, int metastreamMaxCount = 1)
		: base(maxEntriesInMemTable, metastreamMaxCount) {
	}

	public override async Task TestFixtureSetUp() {
		await base.TestFixtureSetUp();

		if (TruncateCheckpoint == long.MinValue)
			throw new InvalidOperationException("AckCheckpoint must be set in WriteTestScenario.");

		OnBeforeTruncating();

		// need to close db before truncator can delete files

		await ReadIndex.DisposeAsync();

		await TableIndex.Close(removeFiles: false);

		await Db.DisposeAsync();

		var truncator = new TFChunkDbTruncator(Db.Config, _ => new IdentityChunkTransformFactory());
		truncator.TruncateDb(TruncateCheckpoint);
	}

	protected virtual void OnBeforeTruncating() {
	}
}
