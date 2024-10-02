// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.LogAbstraction {
	public interface IRecordFactory {
		ISystemLogRecord CreateEpoch(EpochRecord epoch);
	}

	public interface IRecordFactory<TStreamId> : IRecordFactory {
		bool ExplicitStreamCreation { get; }
		bool ExplicitEventTypeCreation { get; }

		IPrepareLogRecord<TStreamId> CreateStreamRecord(
			Guid streamId,
			long logPosition,
			DateTime timeStamp,
			TStreamId streamNumber,
			string streamName);

		IPrepareLogRecord<TStreamId> CreateEventTypeRecord(
			Guid eventTypeId,
			Guid parentEventTypeId,
			string eventType,
			TStreamId eventTypeNumber,
			ushort eventTypeVersion,
			long logPosition,
			DateTime timeStamp);

		IPrepareLogRecord<TStreamId> CreatePrepare(
			long logPosition,
			Guid correlationId,
			Guid eventId,
			long transactionPosition,
			int transactionOffset,
			TStreamId eventStreamId,
			long expectedVersion,
			DateTime timeStamp,
			PrepareFlags flags,
			TStreamId eventType,
			ReadOnlyMemory<byte> data,
			ReadOnlyMemory<byte> metadata);
	}
}
