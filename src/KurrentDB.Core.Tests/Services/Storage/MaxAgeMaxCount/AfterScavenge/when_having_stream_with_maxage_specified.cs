// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System;
using System.Threading;
using System.Threading.Tasks;
using KurrentDB.Core.Data;
using NUnit.Framework;
using ReadStreamResult = KurrentDB.Core.Services.Storage.ReaderIndex.ReadStreamResult;

namespace KurrentDB.Core.Tests.Services.Storage.MaxAgeMaxCount.AfterScavenge;

[TestFixture(typeof(LogFormat.V2), typeof(string))]
[TestFixture(typeof(LogFormat.V3), typeof(uint))]
public class when_having_stream_with_maxage_specified<TLogFormat, TStreamId> : ReadIndexTestScenario<TLogFormat, TStreamId> {
	private EventRecord _r1;
	private EventRecord _r5;
	private EventRecord _r6;

	protected override async ValueTask WriteTestScenario(CancellationToken token) {
		var now = DateTime.UtcNow;

		var metadata = string.Format(@"{{""$maxAge"":{0}}}", (int)TimeSpan.FromMinutes(10).TotalSeconds);

		_r1 = await WriteStreamMetadata("ES", 0, metadata, token: token);
		await WriteSingleEvent("ES", 0, "bla1", now.AddMinutes(-50), token: token);
		await WriteSingleEvent("ES", 1, "bla1", now.AddMinutes(-20), token: token);
		await WriteSingleEvent("ES", 2, "bla1", now.AddMinutes(-11), token: token);
		_r5 = await WriteSingleEvent("ES", 3, "bla1", now.AddMinutes(-5), token: token);
		_r6 = await WriteSingleEvent("ES", 4, "bla1", now.AddMinutes(-1), token: token);

		Scavenge(completeLast: true, mergeChunks: false);
	}

	[Test]
	public async Task single_event_read_doesnt_return_expired_events_and_returns_all_actual_ones() {
		var result = await ReadIndex.ReadEvent("ES", 0, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.NotFound, result.Result);
		Assert.IsNull(result.Record);

		result = await ReadIndex.ReadEvent("ES", 1, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.NotFound, result.Result);
		Assert.IsNull(result.Record);

		result = await ReadIndex.ReadEvent("ES", 2, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.NotFound, result.Result);
		Assert.IsNull(result.Record);

		result = await ReadIndex.ReadEvent("ES", 3, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.Success, result.Result);
		Assert.AreEqual(_r5, result.Record);

		result = await ReadIndex.ReadEvent("ES", 4, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.Success, result.Result);
		Assert.AreEqual(_r6, result.Record);
	}

	[Test]
	public async Task forward_range_read_doesnt_return_expired_records() {
		var result = await ReadIndex.ReadStreamEventsForward("ES", 0, 100, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.Success, result.Result);
		Assert.AreEqual(2, result.Records.Length);
		Assert.AreEqual(_r5, result.Records[0]);
		Assert.AreEqual(_r6, result.Records[1]);
	}

	[Test]
	public async Task backward_range_read_doesnt_return_expired_records() {
		var result = await ReadIndex.ReadStreamEventsBackward("ES", -1, 100, CancellationToken.None);
		Assert.AreEqual(ReadStreamResult.Success, result.Result);
		Assert.AreEqual(2, result.Records.Length);
		Assert.AreEqual(_r6, result.Records[0]);
		Assert.AreEqual(_r5, result.Records[1]);
	}

	[Test]
	public async Task read_all_forward_doesnt_return_expired_records() {
		var records = (await ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100, CancellationToken.None))
			.EventRecords();
		Assert.AreEqual(3, records.Count);
		Assert.AreEqual(_r1, records[0].Event);
		Assert.AreEqual(_r5, records[1].Event);
		Assert.AreEqual(_r6, records[2].Event);
	}

	[Test]
	public async Task read_all_backward_doesnt_return_expired_records() {
		var records = (await ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100, CancellationToken.None)).EventRecords();
		Assert.AreEqual(3, records.Count);
		Assert.AreEqual(_r6, records[0].Event);
		Assert.AreEqual(_r5, records[1].Event);
		Assert.AreEqual(_r1, records[2].Event);
	}
}
