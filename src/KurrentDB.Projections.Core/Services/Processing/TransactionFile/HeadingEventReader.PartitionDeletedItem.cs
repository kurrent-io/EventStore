// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using KurrentDB.Projections.Core.Messages;
using KurrentDB.Projections.Core.Services.Processing.Subscriptions;

namespace KurrentDB.Projections.Core.Services.Processing.TransactionFile;

public partial class HeadingEventReader {
	private class PartitionDeletedItem : Item {
		public readonly ReaderSubscriptionMessage.EventReaderPartitionDeleted Message;

		public PartitionDeletedItem(ReaderSubscriptionMessage.EventReaderPartitionDeleted message)
			: base(message.DeleteLinkOrEventPosition.Value) {
			Message = message;
		}

		public override void Handle(IReaderSubscription subscription) {
			subscription.Handle(Message);
		}
	}
}
