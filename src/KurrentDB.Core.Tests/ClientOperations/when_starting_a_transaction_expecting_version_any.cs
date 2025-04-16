// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System;
using System.Collections.Generic;
using System.Threading;
using KurrentDB.Core.Messages;
using KurrentDB.Core.Data;
using KurrentDB.Core.Messaging;
using KurrentDB.Core.Tests.Helpers;
using NUnit.Framework;

namespace KurrentDB.Core.Tests.ClientOperations;

[TestFixture(typeof(LogFormat.V2), typeof(string))]
[TestFixture(typeof(LogFormat.V3), typeof(uint), Ignore = "Explicit transactions are not supported yet by Log V3")]
public class when_starting_a_transaction_expecting_version_any<TLogFormat, TStreamId> : specification_with_request_manager_integration<TLogFormat, TStreamId> {
	readonly string _streamId = $"new_test_stream_{Guid.NewGuid()}";

	protected override IEnumerable<Message> WithInitialMessages() {
		yield break;
	}

	protected override Message When() {
		return new ClientMessage.TransactionStart(InternalCorrId, ClientCorrId, Envelope, true, _streamId, ExpectedVersion.Any, null);
	}

	[Test]
	public void successful_request_message_is_published() {
		AssertEx.IsOrBecomesTrue(() => Interlocked.Read(ref CompletionMessageCount) == 1);
		Assert.AreEqual(InternalCorrId, CompletionMessage.CorrelationId);
		Assert.True(CompletionMessage.Success);
	}

	[Test]
	public void the_envelope_is_replied_to_with_success() {
		AssertEx.IsOrBecomesTrue(() => Envelope.Replies.Count > 0);
		Assert.That(Envelope.Replies.ContainsSingle<ClientMessage.TransactionStartCompleted>(
			x => x.CorrelationId == ClientCorrId && x.Result == OperationResult.Success));
	}
}
