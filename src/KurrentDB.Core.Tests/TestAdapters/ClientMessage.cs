// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using KurrentDB.Core.Data;

namespace KurrentDB.Core.Tests.TestAdapters;

public static class ClientMessage {
	public class WriteEvents : KurrentDB.Core.Messages.ClientMessage.WriteRequestMessage {
		private readonly KurrentDB.Core.Messages.ClientMessage.WriteEvents _msg;
		public string EventStreamId => _msg.EventStreamIds.Span[0];
		public Event[] Events { get; }

		public WriteEvents(KurrentDB.Core.Messages.ClientMessage.WriteEvents msg) : base(
			msg.InternalCorrId, msg.CorrelationId, msg.Envelope, msg.RequireLeader, msg.User, msg.Tokens, msg.CancellationToken) {
			_msg = msg;
			Events = _msg.Events.ToArray();
		}
	}
}
