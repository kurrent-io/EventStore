// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System;
using System.Collections.Generic;
using System.Linq;
using KurrentDB.Common.Utils;
using KurrentDB.Core.Data;
using KurrentDB.Core.Messages;
using KurrentDB.Core.Tests;
using KurrentDB.Core.Tests.Services.TimeService;
using KurrentDB.Core.TransactionLog.LogRecords;
using KurrentDB.Projections.Core.Messages;
using KurrentDB.Projections.Core.Services.Processing.EventByType;
using NUnit.Framework;
using ResolvedEvent = KurrentDB.Core.Data.ResolvedEvent;

namespace KurrentDB.Projections.Core.Tests.Services.event_reader.event_by_type_index_event_reader;

[TestFixture(typeof(LogFormat.V2), typeof(string))]
[TestFixture(typeof(LogFormat.V3), typeof(uint))]
public class when_index_based_read_timeout_occurs<TLogFormat, TStreamId> : EventByTypeIndexEventReaderTestFixture<TLogFormat, TStreamId> {
	private EventByTypeIndexEventReader _eventReader;
	private Guid _distributionCorrelationId;
	private Guid _eventTypeOneStreamReadCorrelationId;
	private Guid _eventTypeTwoStreamReadCorrelationId;

	protected override void Given() {
		TicksAreHandledImmediately();
	}

	private FakeTimeProvider _fakeTimeProvider;

	[SetUp]
	public new void When() {
		_distributionCorrelationId = Guid.NewGuid();
		_fakeTimeProvider = new FakeTimeProvider();
		var fromPositions = new Dictionary<string, long>();
		fromPositions.Add("$et-eventTypeOne", 0);
		fromPositions.Add("$et-eventTypeTwo", 0);
		_eventReader = new EventByTypeIndexEventReader(_bus, _distributionCorrelationId,
			null, new string[] { "eventTypeOne", "eventTypeTwo" },
			false, new TFPos(0, 0),
			fromPositions, true,
			_fakeTimeProvider,
			stopOnEof: true);

		_eventReader.Resume();

		_eventTypeOneStreamReadCorrelationId = TimeoutRead("$et-eventTypeOne", Guid.Empty);

		CompleteForwardStreamRead("$et-eventTypeOne", _eventTypeOneStreamReadCorrelationId, new[] {
			ResolvedEvent.ForUnresolvedEvent(
				new EventRecord(
					1, 50, Guid.NewGuid(), Guid.NewGuid(), 50, 0, "$et-eventTypeOne", ExpectedVersion.Any,
					DateTime.UtcNow,
					PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd |
					PrepareFlags.IsJson,
					"$>", Helper.UTF8NoBom.GetBytes("0@test-stream"),
					Helper.UTF8NoBom.GetBytes(TFPosToMetadata(new TFPos(50, 50))),
					properties: [])),
			ResolvedEvent.ForUnresolvedEvent(
				new EventRecord(
					2, 150, Guid.NewGuid(), Guid.NewGuid(), 150, 0, "$et-eventTypeOne", ExpectedVersion.Any,
					DateTime.UtcNow,
					PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
					"$>", Helper.UTF8NoBom.GetBytes("1@test-stream"),
					Helper.UTF8NoBom.GetBytes(TFPosToMetadata(new TFPos(150, 150))),
					properties: []))
		});

		_eventTypeTwoStreamReadCorrelationId = TimeoutRead("$et-eventTypeTwo", Guid.Empty);

		CompleteForwardStreamRead("$et-eventTypeTwo", _eventTypeTwoStreamReadCorrelationId, new[] {
			ResolvedEvent.ForUnresolvedEvent(
				new EventRecord(
					1, 100, Guid.NewGuid(), Guid.NewGuid(), 100, 0, "$et-eventTypeTwo", ExpectedVersion.Any,
					DateTime.UtcNow,
					PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd |
					PrepareFlags.IsJson,
					"$>", Helper.UTF8NoBom.GetBytes("2@test-stream"),
					Helper.UTF8NoBom.GetBytes(TFPosToMetadata(new TFPos(100, 100))),
					properties: [])),
			ResolvedEvent.ForUnresolvedEvent(
				new EventRecord(
					2, 200, Guid.NewGuid(), Guid.NewGuid(), 200, 0, "$et-eventTypeTwo", ExpectedVersion.Any,
					DateTime.UtcNow,
					PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
					"$>", Helper.UTF8NoBom.GetBytes("3@test-stream"),
					Helper.UTF8NoBom.GetBytes(TFPosToMetadata(new TFPos(200, 200))),
					properties: []))
		});
	}

	[Test]
	public void should_not_deliver_events() {
		Assert.AreEqual(0,
			_consumer.HandledMessages.OfType<ReaderSubscriptionMessage.CommittedEventDistributed>().Count());
	}

	[Test]
	public void should_attempt_another_read_for_the_timed_out_reads() {
		var eventTypeOneStreamReads = _consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
			.Where(x => x.EventStreamId == "$et-eventTypeOne");

		Assert.AreEqual(eventTypeOneStreamReads.First().CorrelationId, _eventTypeOneStreamReadCorrelationId);
		Assert.AreEqual(1, eventTypeOneStreamReads.Skip(1).Count());

		var eventTypeTwoStreamReads = _consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>()
			.Where(x => x.EventStreamId == "$et-eventTypeTwo");

		Assert.AreEqual(eventTypeTwoStreamReads.First().CorrelationId, _eventTypeTwoStreamReadCorrelationId);
		Assert.AreEqual(1, eventTypeTwoStreamReads.Skip(1).Count());
	}
}
