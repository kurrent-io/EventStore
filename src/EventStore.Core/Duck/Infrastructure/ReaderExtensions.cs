// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.LogCommon;

namespace EventStore.Core.Duck.Infrastructure;

public static class ReaderExtensions {
	public static async ValueTask<IReadOnlyList<ResolvedEvent>> ReadRecords<TStreamId>(this IIndexReader<TStreamId> index, string virtualStreamId, IEnumerable<IndexedPrepare> indexPrepares,
		CancellationToken cancellationToken) {
		using var reader = index.BorrowReader();
		// ReSharper disable once AccessToDisposedClosure
		var readPrepares = indexPrepares.Select(async x => (Record: x, Prepare: await reader.ReadPrepare<TStreamId>(x.LogPosition, cancellationToken)));
		var prepared = await Task.WhenAll(readPrepares);
		var recordsQuery = prepared.Where(x => x.Prepare != null).OrderBy(x => x.Record.Version).ToList();
		var records = recordsQuery
			.Select(x => ResolvedEvent.ForResolvedLink(
				new(x.Record.EventNumber, x.Prepare, x.Prepare.EventStreamId.ToString(), x.Prepare.EventType.ToString()),
				new(
					x.Record.Version,
					x.Prepare.LogPosition,
					x.Prepare.CorrelationId,
					x.Prepare.EventId,
					x.Prepare.TransactionPosition,
					x.Prepare.TransactionOffset,
					virtualStreamId,
					x.Record.Version,
					x.Prepare.TimeStamp,
					x.Prepare.Flags,
					"$>",
					Encoding.UTF8.GetBytes($"{x.Record.EventNumber}@{x.Prepare.EventStreamId.ToString()}"),
					[]
				))
			);
		var result = records.ToList();
		return result;
	}

	internal static async ValueTask<IPrepareLogRecord<TStreamId>> ReadPrepare<TStreamId>(this TFReaderLease localReader, long logPosition, CancellationToken ct) {
		var r = await localReader.TryReadAt(logPosition, couldBeScavenged: true, ct);
		if (!r.Success)
			return null;

		if (r.LogRecord.RecordType is not LogRecordType.Prepare
		    and not LogRecordType.Stream
		    and not LogRecordType.EventType)
			throw new($"Incorrect type of log record {r.LogRecord.RecordType}, expected Prepare record.");
		return (IPrepareLogRecord<TStreamId>)r.LogRecord;
	}
}
