// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System;
using System.Linq;
using KurrentDB.Core.Data;
using KurrentDB.Core.Services.TimerService;
using KurrentDB.Core.Tests.Helpers;
using KurrentDB.Projections.Core.Messages;
using KurrentDB.Projections.Core.Services.Processing.TransactionFile;
using NUnit.Framework;
using HeadingEventReader = KurrentDB.Projections.Core.Services.Processing.TransactionFile.HeadingEventReader;

namespace KurrentDB.Projections.Core.Tests.Services.event_reader.heading_event_reader;

[TestFixture]
public class when_the_heading_event_reader_with_a_subscribed_projection_handles_an_event :
	TestFixtureWithReadWriteDispatchers {
	private HeadingEventReader _point;
	private Exception _exception;
	private Guid _distibutionPointCorrelationId;
	private FakeReaderSubscription _subscription;
	private Guid _projectionSubscriptionId;

	[SetUp]
	public void setup() {
		_exception = null;
		try {
			_point = new HeadingEventReader(10, _bus);
		} catch (Exception ex) {
			_exception = ex;
		}

		Assume.That(_exception == null);

		_distibutionPointCorrelationId = Guid.NewGuid();
		_point.Start(
			_distibutionPointCorrelationId,
			new TransactionFileEventReader(_bus, _distibutionPointCorrelationId, null, new TFPos(0, -1),
				new RealTimeProvider()));
		_point.Handle(
			ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				_distibutionPointCorrelationId, new TFPos(20, 10), "stream", 10, false, Guid.NewGuid(),
				"type", false, new byte[0], new byte[0]));
		_point.Handle(
			ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				_distibutionPointCorrelationId, new TFPos(40, 30), "stream", 11, false, Guid.NewGuid(),
				"type", false, new byte[0], new byte[0]));
		_subscription = new FakeReaderSubscription();
		_projectionSubscriptionId = Guid.NewGuid();
		_point.TrySubscribe(_projectionSubscriptionId, _subscription, 30);
		_point.Handle(
			ReaderSubscriptionMessage.CommittedEventDistributed.Sample(
				_distibutionPointCorrelationId, new TFPos(60, 50), "stream", 12, false, Guid.NewGuid(),
				"type", false, new byte[0], new byte[0]));
	}


	[Test]
	public void projection_receives_events_after_the_subscription_point() {
		Assert.AreEqual(50, _subscription.ReceivedEvents.Last().Data.Position.PreparePosition);
	}

	[Test]
	public void it_can_be_unsubscribed() {
		_point.Unsubscribe(_projectionSubscriptionId);
	}

	[Test]
	public void no_other_projection_can_subscribe_with_the_same_projection_id() {
		Assert.Throws<InvalidOperationException>(() => {
			_point.TrySubscribe(_projectionSubscriptionId, _subscription, 30);
		});
	}
}
