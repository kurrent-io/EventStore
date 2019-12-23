using System;
using System.Diagnostics;
using System.Security.Principal;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.Services.RequestManager.Managers {
	public abstract class TwoPhaseRequestManagerBase : IRequestManager,
		IHandle<StorageMessage.CheckStreamAccessCompleted>,
		IHandle<StorageMessage.AlreadyCommitted>,
		IHandle<StorageMessage.PrepareAck>,
		IHandle<StorageMessage.CommitAck>,
		IHandle<StorageMessage.WrongExpectedVersion>,
		IHandle<StorageMessage.StreamDeleted>,
		IHandle<StorageMessage.RequestManagerTimerTick> {
		internal static readonly TimeSpan TimeoutOffset = TimeSpan.FromMilliseconds(30);
		private static readonly ILogger Log = LogManager.GetLoggerFor<TwoPhaseRequestManagerBase>();

		protected readonly IPublisher Publisher;
		protected readonly IEnvelope PublishEnvelope;

		protected IEnvelope ResponseEnvelope {
			get { return _responseEnvelope; }
		}

		protected Guid ClientCorrId {
			get { return _clientCorrId; }
		}

		protected DateTime NextTimeoutTime {
			get { return _nextTimeoutTime; }
		}

		protected readonly TimeSpan PrepareTimeout;
		protected readonly TimeSpan CommitTimeout;
		private IEnvelope _responseEnvelope;
		private Guid _internalCorrId;
		private Guid _clientCorrId;
		private long _transactionId = -1;

		private int _awaitingPrepare;
		private DateTime _nextTimeoutTime;

		private bool _completed;
		private bool _initialized;
		private bool _betterOrdering;
		private long _transactionCommitPosition;
		private long _systemCommittedPosition;

		protected TwoPhaseRequestManagerBase(IPublisher publisher,
			int prepareCount,
			TimeSpan prepareTimeout,
			TimeSpan commitTimeout,
			bool betterOrdering) {
			Ensure.NotNull(publisher, "publisher");
			Ensure.Positive(prepareCount, "prepareCount");

			Publisher = publisher;
			PublishEnvelope = new PublishEnvelope(publisher);

			PrepareTimeout = prepareTimeout;
			CommitTimeout = commitTimeout;
			_betterOrdering = betterOrdering;

			_awaitingPrepare = prepareCount;
			_transactionCommitPosition = long.MaxValue;
			_systemCommittedPosition = long.MinValue;
		}

		protected abstract void OnSecurityAccessGranted(Guid internalCorrId);

		protected void InitNoPreparePhase(IEnvelope responseEnvelope, Guid internalCorrId, Guid clientCorrId,
			string eventStreamId, IPrincipal user, StreamAccessType accessType) {
			if (_initialized)
				throw new InvalidOperationException();

			_initialized = true;

			_responseEnvelope = responseEnvelope;
			_internalCorrId = internalCorrId;
			_clientCorrId = clientCorrId;

			_nextTimeoutTime = DateTime.UtcNow + CommitTimeout;
			_awaitingPrepare = 0;
			Publisher.Publish(new StorageMessage.CheckStreamAccess(
				PublishEnvelope, internalCorrId, eventStreamId, null, accessType, user, _betterOrdering));
		}

		protected void InitTwoPhase(IEnvelope responseEnvelope, Guid internalCorrId, Guid clientCorrId,
			long transactionId, IPrincipal user, StreamAccessType accessType) {
			if (_initialized)
				throw new InvalidOperationException();

			_initialized = true;

			_responseEnvelope = responseEnvelope;
			_internalCorrId = internalCorrId;
			_clientCorrId = clientCorrId;
			_transactionId = transactionId;

			_nextTimeoutTime = DateTime.UtcNow + PrepareTimeout;

			Publisher.Publish(new StorageMessage.CheckStreamAccess(
				PublishEnvelope, internalCorrId, null, transactionId, accessType, user));
		}

		public void Handle(StorageMessage.CheckStreamAccessCompleted message) {
			if (message.AccessResult.Granted)
				OnSecurityAccessGranted(_internalCorrId);
			else
				CompleteFailedRequest(OperationResult.AccessDenied, "Access denied.");
		}

		public void Handle(StorageMessage.WrongExpectedVersion message) {
			if (_completed)
				return;

			CompleteFailedRequest(OperationResult.WrongExpectedVersion, "Wrong expected version.",
				message.CurrentVersion);
		}

		public void Handle(StorageMessage.StreamDeleted message) {
			if (_completed)
				return;

			CompleteFailedRequest(OperationResult.StreamDeleted, "Stream is deleted.");
		}

		public void Handle(StorageMessage.RequestManagerTimerTick message) {
			if (_completed || message.UtcNow < _nextTimeoutTime)
				return;

			if (_awaitingPrepare != 0)
				CompleteFailedRequest(OperationResult.PrepareTimeout, "Prepare phase timeout.");
			else
				CompleteFailedRequest(OperationResult.CommitTimeout, "Commit phase timeout.");
		}

		public void Handle(StorageMessage.AlreadyCommitted message) {
			Log.Trace("IDEMPOTENT WRITE TO STREAM ClientCorrelationID {clientCorrelationId}, {message}.", _clientCorrId,
				message);
			_transactionCommitPosition = message.LogPosition;
			SuccessLocalCommitted(message.FirstEventNumber, message.LastEventNumber, -1, -1);
			if (_systemCommittedPosition >= _transactionCommitPosition) { SuccessClusterCommitted(); }
		}

		public void Handle(StorageMessage.PrepareAck message) {
			if (_completed)
				return;

			if (_transactionId == -1)
				throw new Exception("TransactionId was not set, transactionId = -1.");

			if (message.Flags.HasAnyOf(PrepareFlags.TransactionEnd)) {
				_awaitingPrepare -= 1;
				if (_awaitingPrepare == 0) {
					Publisher.Publish(new StorageMessage.WriteCommit(message.CorrelationId, PublishEnvelope,
						_transactionId));
					_nextTimeoutTime = DateTime.UtcNow + CommitTimeout;
				}
			}
		}

		public void Handle(StorageMessage.CommitAck message) {
			if (_completed) { return; }

			_transactionCommitPosition = message.LogPosition;
			SuccessLocalCommitted(message.FirstEventNumber, message.LastEventNumber, message.LogPosition,
				message.LogPosition);
			if (_systemCommittedPosition >= _transactionCommitPosition) { SuccessClusterCommitted(); }
		}

		public void Handle(CommitMessage.CommittedTo message) {
			if (_completed) { return; }
			_systemCommittedPosition = message.LogPosition;
			if (_systemCommittedPosition >= _transactionCommitPosition) { SuccessClusterCommitted(); }
		}

		protected virtual void SuccessLocalCommitted(long firstEventNumber, long lastEventNumber, long preparePosition,
			long commitPosition) {
		}
		protected virtual void SuccessClusterCommitted() {
			_completed = true;
			Publisher.Publish(new StorageMessage.RequestCompleted(_internalCorrId, true));
		}

		protected virtual void CompleteFailedRequest(OperationResult result, string error, long currentVersion = -1) {
			Debug.Assert(result != OperationResult.Success);
			_completed = true;
			Publisher.Publish(new StorageMessage.RequestCompleted(_internalCorrId, false, currentVersion));
		}
	}
}
