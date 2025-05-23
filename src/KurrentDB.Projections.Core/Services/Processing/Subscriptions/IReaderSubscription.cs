// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System;
using KurrentDB.Core.Bus;
using KurrentDB.Core.Helpers;
using KurrentDB.Projections.Core.Messages;

namespace KurrentDB.Projections.Core.Services.Processing.Subscriptions;

public interface IReaderSubscription : IHandle<ReaderSubscriptionMessage.CommittedEventDistributed>,
	IHandle<ReaderSubscriptionMessage.EventReaderIdle>,
	IHandle<ReaderSubscriptionMessage.EventReaderStarting>,
	IHandle<ReaderSubscriptionMessage.EventReaderEof>,
	IHandle<ReaderSubscriptionMessage.EventReaderPartitionEof>,
	IHandle<ReaderSubscriptionMessage.EventReaderPartitionDeleted>,
	IHandle<ReaderSubscriptionMessage.EventReaderNotAuthorized>,
	IHandle<ReaderSubscriptionMessage.ReportProgress> {
	string Tag { get; }
	Guid SubscriptionId { get; }
	IEventReader CreatePausedEventReader(IPublisher publisher, IODispatcher ioDispatcher, Guid forkedEventReaderId);
}
