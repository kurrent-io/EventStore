// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System;
using System.Threading;
using System.Threading.Tasks;
using KurrentDB.Common.Utils;
using KurrentDB.Core.Bus;
using KurrentDB.Core.Data;
using KurrentDB.Core.Messaging;
using KurrentDB.Core.TransactionLog.LogRecords;

namespace KurrentDB.Core.Messages;

public static partial class StorageMessage {
	public interface IPreconditionedWriteMessage {
		Guid CorrelationId { get; }
		IEnvelope Envelope { get; }
	}

	public interface IFlushableMessage {
	}

	public interface ILeaderWriteMessage {
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class WritePrepares : Message, IPreconditionedWriteMessage, IFlushableMessage, ILeaderWriteMessage {
		public Guid CorrelationId { get; private set; }
		public IEnvelope Envelope { get; private set; }
		public ReadOnlyMemory<string> StreamIds { get; private set; }
		public ReadOnlyMemory<long> ExpectedVersions { get; private set; }
		public ReadOnlyMemory<Event> Events { get; private set; }
		public ReadOnlyMemory<int> EventStreamIndexes { get; private set; }

		public WritePrepares(
			Guid correlationId,
			IEnvelope envelope,
			ReadOnlyMemory<string> streamIds,
			ReadOnlyMemory<long> expectedVersions,
			ReadOnlyMemory<Event> events,
			ReadOnlyMemory<int> eventStreamIndexes,
			CancellationToken cancellationToken) : base(cancellationToken) {
			CorrelationId = correlationId;
			Envelope = envelope;
			StreamIds = streamIds;
			ExpectedVersions = expectedVersions;
			Events = events;
			EventStreamIndexes = eventStreamIndexes;
		}

		public override string ToString() {
			var sumDataBytes = 0L;
			var sumMetadataBytes = 0L;
			foreach (var @event in Events.Span) {
				sumDataBytes += @event.Data.Length;
				sumMetadataBytes += @event.Metadata.Length;
			}

			return $"{GetType().Name} " +
			$"CorrelationId: {CorrelationId}, " +
				$"EventStreamIds: {string.Join(", ", StreamIds.ToArray())}, " + // TODO: use .Span instead of .ToArray() when we move to .NET 10
				$"ExpectedVersions: {string.Join(", ", ExpectedVersions.ToArray())}, " +
			$"Envelope: {{ {Envelope} }}, " +
				$"NumEvents: {Events.Length}, " +
				$"DataBytes: {sumDataBytes}, " +
				$"MetadataBytes: {sumMetadataBytes}";
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class WriteDelete : Message, IPreconditionedWriteMessage, IFlushableMessage, ILeaderWriteMessage {
		public Guid CorrelationId { get; private set; }
		public IEnvelope Envelope { get; private set; }
		public string EventStreamId { get; private set; }
		public long ExpectedVersion { get; private set; }
		public readonly bool HardDelete;

		public WriteDelete(Guid correlationId, IEnvelope envelope, string eventStreamId, long expectedVersion,
			bool hardDelete, CancellationToken cancellationToken) : base(cancellationToken) {
			Ensure.NotEmptyGuid(correlationId, "correlationId");
			Ensure.NotNull(envelope, "envelope");
			Ensure.NotNull(eventStreamId, "eventStreamId");

			CorrelationId = correlationId;
			Envelope = envelope;
			EventStreamId = eventStreamId;
			ExpectedVersion = expectedVersion;
			HardDelete = hardDelete;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class WriteCommit : Message, IFlushableMessage, ILeaderWriteMessage {
		public readonly Guid CorrelationId;
		public readonly IEnvelope Envelope;
		public readonly long TransactionPosition;

		public WriteCommit(Guid correlationId, IEnvelope envelope, long transactionPosition) {
			CorrelationId = correlationId;
			Envelope = envelope;
			TransactionPosition = transactionPosition;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class WriteTransactionStart : Message, IPreconditionedWriteMessage, IFlushableMessage,
		ILeaderWriteMessage {

		public Guid CorrelationId { get; private set; }
		public IEnvelope Envelope { get; private set; }
		public string EventStreamId { get; private set; }
		public long ExpectedVersion { get; private set; }

		public readonly DateTime LiveUntil;

		public WriteTransactionStart(Guid correlationId, IEnvelope envelope, string eventStreamId,
			long expectedVersion, DateTime liveUntil) {
			Ensure.NotEmptyGuid(correlationId, "correlationId");
			Ensure.NotNull(envelope, "envelope");
			Ensure.NotNull(eventStreamId, "eventStreamId");

			CorrelationId = correlationId;
			Envelope = envelope;
			EventStreamId = eventStreamId;
			ExpectedVersion = expectedVersion;

			LiveUntil = liveUntil;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class WriteTransactionData : Message, IFlushableMessage, ILeaderWriteMessage {
		public readonly Guid CorrelationId;
		public readonly IEnvelope Envelope;
		public readonly long TransactionId;
		public readonly Event[] Events;

		public WriteTransactionData(Guid correlationId, IEnvelope envelope, long transactionId, Event[] events) {
			CorrelationId = correlationId;
			Envelope = envelope;
			TransactionId = transactionId;
			Events = events;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class WriteTransactionEnd : Message, IFlushableMessage, ILeaderWriteMessage {
		public readonly Guid CorrelationId;
		public readonly IEnvelope Envelope;
		public readonly long TransactionId;

		public readonly DateTime LiveUntil;

		public WriteTransactionEnd(Guid correlationId, IEnvelope envelope, long transactionId,
			DateTime liveUntil) {
			CorrelationId = correlationId;
			Envelope = envelope;
			TransactionId = transactionId;

			LiveUntil = liveUntil;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class PrepareAck : Message {
		public readonly Guid CorrelationId;
		public readonly long LogPosition;
		public readonly PrepareFlags Flags;

		public PrepareAck(Guid correlationId, long logPosition, PrepareFlags flags) {
			Ensure.NotEmptyGuid(correlationId, "correlationId");
			Ensure.Nonnegative(logPosition, "logPosition");

			CorrelationId = correlationId;
			LogPosition = logPosition;
			Flags = flags;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class CommitAck : Message {
		public readonly Guid CorrelationId;
		public readonly long LogPosition;
		public readonly long TransactionPosition;
		public readonly ReadOnlyMemory<long> FirstEventNumbers;
		public readonly ReadOnlyMemory<long> LastEventNumbers;
		public readonly ReadOnlyMemory<int> EventStreamIndexes;
		public int NumStreams => FirstEventNumbers.Length;

		public CommitAck(Guid correlationId, long logPosition, long transactionPosition,
			ReadOnlyMemory<long> firstEventNumbers, ReadOnlyMemory<long> lastEventNumbers,
			ReadOnlyMemory<int> eventStreamIndexes) {
			Ensure.NotEmptyGuid(correlationId, "correlationId");
			Ensure.Nonnegative(logPosition, "logPosition");
			Ensure.Nonnegative(transactionPosition, "transactionPosition");
			Ensure.Equal(firstEventNumbers.Length, lastEventNumbers.Length, nameof(lastEventNumbers));

			var numStreams = firstEventNumbers.Length;

			for (var i = 0; i < numStreams; i++) {
				var firstEventNumber = firstEventNumbers.Span[i];
				var lastEventNumber = lastEventNumbers.Span[i];
				if (firstEventNumber < -1)
					throw new ArgumentOutOfRangeException(nameof(firstEventNumbers),
						$"FirstEventNumber: {firstEventNumber}");
				if (lastEventNumber - firstEventNumber + 1 < 0)
					throw new ArgumentOutOfRangeException(nameof(lastEventNumbers),
						$"LastEventNumber {lastEventNumber}, FirstEventNumber {firstEventNumber}.");
			}

			foreach (var eventStreamIndex in eventStreamIndexes.Span) {
				if (eventStreamIndex < 0 || eventStreamIndex >= numStreams)
					throw new ArgumentOutOfRangeException(nameof(eventStreamIndexes));
			}

			CorrelationId = correlationId;
			LogPosition = logPosition;
			TransactionPosition = transactionPosition;
			FirstEventNumbers = firstEventNumbers;
			LastEventNumbers = lastEventNumbers;
			EventStreamIndexes = eventStreamIndexes;
		}

		// used in tests only
		public static CommitAck ForSingleStream(Guid correlationId, long logPosition, long transactionPosition, long firstEventNumber, long lastEventNumber) {
			return new CommitAck(
				correlationId,
				logPosition,
				transactionPosition,
				firstEventNumbers: new[] { firstEventNumber },
				lastEventNumbers: new[] { lastEventNumber },
				eventStreamIndexes: new int[lastEventNumber - firstEventNumber + 1]);
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class CommitIndexed : Message {
		public readonly Guid CorrelationId;
		public readonly long LogPosition;
		public readonly long TransactionPosition;
		public readonly ReadOnlyMemory<long> FirstEventNumbers;
		public readonly ReadOnlyMemory<long> LastEventNumbers;

		public CommitIndexed(Guid correlationId, long logPosition, long transactionPosition,
			ReadOnlyMemory<long> firstEventNumbers, ReadOnlyMemory<long> lastEventNumbers) {
			Ensure.NotEmptyGuid(correlationId, "correlationId");
			Ensure.Nonnegative(logPosition, "logPosition");
			Ensure.Nonnegative(transactionPosition, "transactionPosition");
			Ensure.Equal(firstEventNumbers.Length, lastEventNumbers.Length, nameof(lastEventNumbers));

			for (var i = 0; i < firstEventNumbers.Length; i++) {
				var firstEventNumber = firstEventNumbers.Span[i];
				var lastEventNumber = lastEventNumbers.Span[i];

				if (firstEventNumber < -1)
					throw new ArgumentOutOfRangeException(nameof(firstEventNumbers),
						$"FirstEventNumber: {firstEventNumber}");

				if (lastEventNumber - firstEventNumber + 1 < 0)
					throw new ArgumentOutOfRangeException(nameof(lastEventNumbers),
						$"LastEventNumber {lastEventNumber}, FirstEventNumber {firstEventNumber}.");
			}

			CorrelationId = correlationId;
			LogPosition = logPosition;
			TransactionPosition = transactionPosition;
			FirstEventNumbers = firstEventNumbers;
			LastEventNumbers = lastEventNumbers;
		}

		// used in tests only
		public static CommitIndexed ForSingleStream(Guid correlationId, long logPosition, long transactionPosition, long firstEventNumber, long lastEventNumber) {
			return new CommitIndexed(
				correlationId,
				logPosition,
				transactionPosition,
				firstEventNumbers: new[] { firstEventNumber },
				lastEventNumbers: new[] { lastEventNumber });
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class EventCommitted : Message {
		public readonly long CommitPosition;
		public readonly EventRecord Event;
		public readonly bool TfEof;

		public EventCommitted(long commitPosition, EventRecord @event, bool isTfEof) {
			CommitPosition = commitPosition;
			Event = @event;
			TfEof = isTfEof;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class InMemoryEventCommitted : Message {
		public readonly long CommitPosition;
		public readonly EventRecord Event;

		public InMemoryEventCommitted(long commitPosition, EventRecord @event) {
			CommitPosition = commitPosition;
			Event = @event;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class TfEofAtNonCommitRecord : Message {
		public TfEofAtNonCommitRecord() {
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class AlreadyCommitted : Message {
		public readonly Guid CorrelationId;

		public readonly ReadOnlyMemory<string> EventStreamIds;
		public readonly ReadOnlyMemory<long> FirstEventNumbers;
		public readonly ReadOnlyMemory<long> LastEventNumbers;
		public readonly long LogPosition;

		public AlreadyCommitted(Guid correlationId,
			ReadOnlyMemory<string> eventStreamIds,
			ReadOnlyMemory<long> firstEventNumbers,
			ReadOnlyMemory<long> lastEventNumbers,
			long logPosition) {
			Ensure.NotEmptyGuid(correlationId, "correlationId");
			Ensure.Positive(eventStreamIds.Length, nameof(eventStreamIds));
			Ensure.Equal(firstEventNumbers.Length, eventStreamIds.Length, nameof(firstEventNumbers));
			Ensure.Equal(lastEventNumbers.Length, eventStreamIds.Length, nameof(lastEventNumbers));

			CorrelationId = correlationId;
			EventStreamIds = eventStreamIds;
			FirstEventNumbers = firstEventNumbers;
			LastEventNumbers = lastEventNumbers;
			LogPosition = logPosition;
		}

		// used in tests only
		public static AlreadyCommitted ForSingleStream(Guid correlationId, string eventStreamId, long firstEventNumber, long lastEventNumber, long logPosition) {
			return new AlreadyCommitted(
				correlationId,
				eventStreamIds: new[] { eventStreamId },
				firstEventNumbers: new[] { firstEventNumber },
				lastEventNumbers: new[] { lastEventNumber },
				logPosition);
		}

		public override string ToString() {
			return
				$"EventStreamIds: {string.Join(", ", EventStreamIds.ToArray())}," +
				$"CorrelationId: {CorrelationId}," +
				$"FirstEventNumbers: {string.Join(", ", FirstEventNumbers.ToArray())}," +
				$"LastEventNumbers: {string.Join(", ", LastEventNumbers.ToArray())}";
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class InvalidTransaction : Message {
		public readonly Guid CorrelationId;

		public InvalidTransaction(Guid correlationId) {
			CorrelationId = correlationId;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class WrongExpectedVersion : Message {
		public readonly Guid CorrelationId;
		public readonly ReadOnlyMemory<int> StreamIndexes;
		public readonly ReadOnlyMemory<long> CurrentVersions;

		public WrongExpectedVersion(Guid correlationId, ReadOnlyMemory<int> streamIndexes, ReadOnlyMemory<long> currentVersions) {
			Ensure.NotEmptyGuid(correlationId, "correlationId");
			Ensure.Equal(streamIndexes.Length, currentVersions.Length, nameof(streamIndexes));
			CorrelationId = correlationId;
			StreamIndexes = streamIndexes;
			CurrentVersions = currentVersions;
		}

		public static WrongExpectedVersion ForSingleStream(Guid correlationId, long currentVersion) {
			return new WrongExpectedVersion(
				correlationId,
				streamIndexes: new[] { 0 },
				currentVersions: new[] { currentVersion });
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class StreamDeleted : Message {
		public readonly Guid CorrelationId;

		public StreamDeleted(Guid correlationId) {
			Ensure.NotEmptyGuid(correlationId, "correlationId");
			CorrelationId = correlationId;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class RequestCompleted : Message {
		public readonly Guid CorrelationId;
		public readonly bool Success;
		public readonly ReadOnlyMemory<long> CurrentVersions;

		public RequestCompleted(Guid correlationId, bool success, ReadOnlyMemory<long> currentVersions = default) {
			Ensure.NotEmptyGuid(correlationId, "correlationId");
			CorrelationId = correlationId;
			Success = success;
			CurrentVersions = currentVersions;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class RequestManagerTimerTick : Message {
		public DateTime UtcNow {
			get { return _now ?? DateTime.UtcNow; }
		}

		private readonly DateTime? _now;

		public RequestManagerTimerTick() {
		}

		public RequestManagerTimerTick(DateTime now) {
			_now = now;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class BatchLogExpiredMessages : Message, IQueueAffineMessage {
		public readonly Guid CorrelationId;
		public int QueueId { get; }

		public BatchLogExpiredMessages(Guid correlationId, int queueId) {
			Ensure.NotEmptyGuid(correlationId, "correlationId");
			Ensure.Nonnegative(queueId, "queueId");
			CorrelationId = correlationId;
			QueueId = queueId;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class EffectiveStreamAclRequest : Message {
		public readonly string StreamId;
		public readonly IEnvelope Envelope;

		public EffectiveStreamAclRequest(string streamId, IEnvelope envelope, CancellationToken cancellationToken)
			: base(cancellationToken) {
			StreamId = streamId;
			Envelope = envelope;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class EffectiveStreamAclResponse : Message {
		public readonly EffectiveAcl Acl;

		public EffectiveStreamAclResponse(EffectiveAcl acl) {
			Acl = acl;
		}
	}

	public class EffectiveAcl {
		public readonly StreamAcl Stream;
		public readonly StreamAcl System;
		public readonly StreamAcl Default;

		public EffectiveAcl(StreamAcl stream, StreamAcl system, StreamAcl @default) {
			Stream = stream;
			System = system;
			Default = @default;
		}

		public static Task<EffectiveAcl> LoadAsync(IPublisher publisher, string streamId, CancellationToken cancellationToken) {
			var envelope = new RequestEffectiveAclEnvelope();
			publisher.Publish(new EffectiveStreamAclRequest(streamId, envelope, cancellationToken));
			return envelope.Task;
		}

		class RequestEffectiveAclEnvelope : IEnvelope {
			private readonly TaskCompletionSource<EffectiveAcl> _tcs;

			public RequestEffectiveAclEnvelope() {
				_tcs = new TaskCompletionSource<EffectiveAcl>(TaskCreationOptions.RunContinuationsAsynchronously);
			}
			public void ReplyWith<T>(T message) where T : Message {
				if (message == null)
					throw new ArgumentNullException(nameof(message));
				if (message is EffectiveStreamAclResponse response) {
					_tcs.TrySetResult(response.Acl);
					return;
				} else {
					if (message is OperationCancelledMessage cancelled) {
						_tcs.TrySetCanceled(cancelled.CancellationToken);
					}
				}
				throw new ArgumentException($"Unexpected message type {typeof(T)}");
			}

			public Task<EffectiveAcl> Task => _tcs.Task;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class OperationCancelledMessage : Message {
		public OperationCancelledMessage(CancellationToken cancellationToken) : base(cancellationToken) {
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class StreamIdFromTransactionIdRequest : Message {
		public readonly long TransactionId;
		public readonly IEnvelope Envelope;

		public StreamIdFromTransactionIdRequest(in long transactionId, IEnvelope envelope, CancellationToken cancellationToken)
			: base(cancellationToken) {
			TransactionId = transactionId;
			Envelope = envelope;
		}
	}

	[DerivedMessage(CoreMessage.Storage)]
	public partial class StreamIdFromTransactionIdResponse : Message {
		public readonly string StreamId;

		public StreamIdFromTransactionIdResponse(string streamId) {
			StreamId = streamId;
		}
	}
}
