// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System;
using KurrentDB.Common.Utils;
using KurrentDB.Core.Bus;
using KurrentDB.Core.Messages;
using KurrentDB.Core.Services.TimerService;
using KurrentDB.Core.Services.VNode;

namespace KurrentDB.Core.Services;

public class RequestForwardingService : IHandle<SystemMessage.SystemStart>,
	IHandle<SystemMessage.RequestForwardingTimerTick>,
	IHandle<ClientMessage.NotHandled>,
	IHandle<ClientMessage.WriteEventsCompleted>,
	IHandle<ClientMessage.TransactionStartCompleted>,
	IHandle<ClientMessage.TransactionWriteCompleted>,
	IHandle<ClientMessage.TransactionCommitCompleted>,
	IHandle<ClientMessage.DeleteStreamCompleted> {
	private readonly IPublisher _bus;
	private readonly MessageForwardingProxy _forwardingProxy;

	private readonly TimerMessage.Schedule _tickScheduleMessage;

	public RequestForwardingService(IPublisher bus, MessageForwardingProxy forwardingProxy, TimeSpan tickInterval) {
		Ensure.Nonnegative(tickInterval.Milliseconds, "tickInterval");

		_bus = Ensure.NotNull(bus);
		_forwardingProxy = Ensure.NotNull(forwardingProxy);
		_tickScheduleMessage = TimerMessage.Schedule.Create(tickInterval, bus, new SystemMessage.RequestForwardingTimerTick());
	}

	public void Handle(SystemMessage.SystemStart message) {
		_bus.Publish(_tickScheduleMessage);
	}

	public void Handle(SystemMessage.RequestForwardingTimerTick message) {
		_forwardingProxy.TimeoutForwardings();
		_bus.Publish(_tickScheduleMessage);
	}

	public void Handle(ClientMessage.NotHandled message) {
		_forwardingProxy.TryForwardReply(message.CorrelationId, message, (clientCorrId, m) => new(clientCorrId, m.Reason, m.LeaderInfo));
	}

	public void Handle(ClientMessage.WriteEventsCompleted message) {
		_forwardingProxy.TryForwardReply(message.CorrelationId, message, (clientCorrId, m) => m.WithCorrelationId(clientCorrId));
	}

	public void Handle(ClientMessage.TransactionStartCompleted message) {
		_forwardingProxy.TryForwardReply(message.CorrelationId, message, (clientCorrId, m) => m.WithCorrelationId(clientCorrId));
	}

	public void Handle(ClientMessage.TransactionWriteCompleted message) {
		_forwardingProxy.TryForwardReply(message.CorrelationId, message, (clientCorrId, m) => m.WithCorrelationId(clientCorrId));
	}

	public void Handle(ClientMessage.TransactionCommitCompleted message) {
		_forwardingProxy.TryForwardReply(message.CorrelationId, message, (clientCorrId, m) => m.WithCorrelationId(clientCorrId));
	}

	public void Handle(ClientMessage.DeleteStreamCompleted message) {
		_forwardingProxy.TryForwardReply(message.CorrelationId, message, (clientCorrId, m) => m.WithCorrelationId(clientCorrId));
	}
}
