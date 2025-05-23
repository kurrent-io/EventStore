// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KurrentDB.Core.Data;
using KurrentDB.Core.Tests.TransactionLog.Scavenging.Helpers;
using KurrentDB.Core.TransactionLog.LogRecords;
using NUnit.Framework;

namespace KurrentDB.Core.Tests.TransactionLog.Scavenging;

[TestFixture(typeof(LogFormat.V2), typeof(string))]
[TestFixture(typeof(LogFormat.V3), typeof(uint))]
public class when_deleted_stream_with_metadata_is_scavenged<TLogFormat, TStreamId> : ScavengeTestScenario<TLogFormat, TStreamId> {
	protected override ValueTask<DbResult> CreateDb(TFChunkDbCreationHelper<TLogFormat, TStreamId> dbCreator, CancellationToken token) {
		return dbCreator
			.Chunk(Rec.Prepare(0, "$$bla", metadata: new StreamMetadata(10, null, null, null, null)),
				Rec.Prepare(0, "$$bla", metadata: new StreamMetadata(2, null, null, null, null)),
				Rec.Commit(0, "$$bla"),
				Rec.Delete(1, "bla"),
				Rec.Commit(1, "bla"))
			.CompleteLastChunk()
			.CreateDb(token: token);
	}

	protected override ILogRecord[][] KeptRecords(DbResult dbResult) {
		return LogFormatHelper<TLogFormat, TStreamId>.IsV2
			? new[] { dbResult.Recs[0].Where((x, i) => i >= 3).ToArray() }
			: new[] { dbResult.Recs[0].Where((x, i) => i == 0 || i >= 4).ToArray() };
	}

	[Test]
	public async Task metastream_is_scavenged_as_well() {
		await CheckRecords();
	}
}
