// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KurrentDB.Core.Data;
using NUnit.Framework;

namespace KurrentDB.Core.Tests.TransactionLog.Truncation;

[TestFixture(typeof(LogFormat.V2), typeof(string))]
[TestFixture(typeof(LogFormat.V3), typeof(uint))]
public class
	when_truncating_single_uncompleted_chunk_with_index_in_memory_and_then_reopening_db<TLogFormat, TStreamId> :
		TruncateAndReOpenDbScenario<TLogFormat, TStreamId> {
	private EventRecord _event1;
	private EventRecord _event2;
	private EventRecord _event3;

	public when_truncating_single_uncompleted_chunk_with_index_in_memory_and_then_reopening_db()
		: base(20000) {
	}

	protected override async ValueTask WriteTestScenario(CancellationToken token) {
		_event1 = await WriteSingleEvent("ES", 0, new string('.', 500), token: token);
		_event2 = await WriteSingleEvent("ES", 1, new string('.', 500), token: token); // truncated
		_event3 = await WriteSingleEvent("ES", 2, new string('.', 500), token: token); // truncated

		TruncateCheckpoint = _event2.LogPosition;
	}

	[Test]
	public void checksums_should_be_equal_to_ack_checksum() {
		Assert.AreEqual(TruncateCheckpoint, WriterCheckpoint.Read());
		Assert.AreEqual(TruncateCheckpoint, ChaserCheckpoint.Read());
	}

	[Test]
	public async Task read_one_by_one_doesnt_return_truncated_records() {
		var res = await ReadIndex.ReadEvent("ES", 0, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.Success, res.Result);
		Assert.AreEqual(_event1, res.Record);

		res = await ReadIndex.ReadEvent("ES", 1, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.NotFound, res.Result);
		Assert.IsNull(res.Record);

		res = await ReadIndex.ReadEvent("ES", 2, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.NotFound, res.Result);
		Assert.IsNull(res.Record);

		res = await ReadIndex.ReadEvent("ES", 3, CancellationToken.None);
		Assert.AreEqual(ReadEventResult.NotFound, res.Result);
		Assert.IsNull(res.Record);
	}

	[Test]
	public async Task read_stream_forward_doesnt_return_truncated_records() {
		var res = await ReadIndex.ReadStreamEventsForward("ES", 0, 100, CancellationToken.None);
		var records = res.Records;
		Assert.AreEqual(1, records.Length);
		Assert.AreEqual(_event1, records[0]);
	}

	[Test]
	public async Task read_stream_backward_doesnt_return_truncated_records() {
		var res = await ReadIndex.ReadStreamEventsBackward("ES", -1, 100, CancellationToken.None);
		var records = res.Records;
		Assert.AreEqual(1, records.Length);
		Assert.AreEqual(_event1, records[0]);
	}

	[Test]
	public async Task read_all_forward_doesnt_return_truncated_records() {
		var res = await ReadIndex.ReadAllEventsForward(new TFPos(0, 0), 100, CancellationToken.None);
		var records = res.EventRecords()
			.Select(r => r.Event)
			.ToArray();
		Assert.AreEqual(1, records.Length);
		Assert.AreEqual(_event1, records[0]);
	}

	[Test]
	public async Task read_all_backward_doesnt_return_truncated_records() {
		var res = await ReadIndex.ReadAllEventsBackward(GetBackwardReadPos(), 100, CancellationToken.None);
		var records = res.EventRecords()
			.Select(r => r.Event)
			.ToArray();
		Assert.AreEqual(1, records.Length);
		Assert.AreEqual(_event1, records[0]);
	}

	[Test]
	public async Task read_all_backward_from_last_truncated_record_returns_no_records() {
		var pos = new TFPos(_event3.LogPosition, _event3.LogPosition);
		var res = await ReadIndex.ReadAllEventsForward(pos, 100, CancellationToken.None);
		var records = res.EventRecords()
			.Select(r => r.Event)
			.ToArray();
		Assert.AreEqual(0, records.Length);
	}
}
