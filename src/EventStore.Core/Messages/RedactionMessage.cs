using System.Threading;
using EventStore.Common.Utils;
using EventStore.Core.Messaging;

namespace EventStore.Core.Messages {
	public static class RedactionMessage {

		public class SwitchChunkLock : Message {
			private static readonly int TypeId = Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			public IEnvelope Envelope { get; }

			public SwitchChunkLock(IEnvelope envelope) {
				Envelope = envelope;
			}
		}

		public class SwitchChunkLockSucceeded : Message {
			private static readonly int TypeId = Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}
		}

		public class SwitchChunkLockFailed : Message {
			private static readonly int TypeId = Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}
		}

		public class SwitchChunk : Message {
			private static readonly int TypeId = Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			public IEnvelope Envelope { get; }
			public string TargetChunkFile { get; }
			public string NewChunkFile { get; }

			public SwitchChunk(IEnvelope envelope, string targetChunkFile, string newChunkFile) {
				Ensure.NotNull(envelope, nameof(envelope));
				Ensure.NotNullOrEmpty(targetChunkFile, nameof(targetChunkFile));
				Ensure.NotNullOrEmpty(newChunkFile, nameof(newChunkFile));

				Envelope = envelope;
				TargetChunkFile = targetChunkFile;
				NewChunkFile = newChunkFile;
			}
		}

		public class SwitchChunkSucceeded : Message {
			private static readonly int TypeId = Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}
		}

		public class SwitchChunkFailed : Message {
			private static readonly int TypeId = Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			public string Reason { get; }

			public SwitchChunkFailed(string reason) {
				Ensure.NotNullOrEmpty(reason, nameof(reason));

				Reason = reason;
			}
		}

		public class SwitchChunkUnlock : Message {
			private static readonly int TypeId = Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			public IEnvelope Envelope { get; }

			public SwitchChunkUnlock(IEnvelope envelope) {
				Envelope = envelope;
			}
		}

		public class SwitchChunkUnlockSucceeded : Message {
			private static readonly int TypeId = Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}
		}

		public class SwitchChunkUnlockFailed : Message {
			private static readonly int TypeId = Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}
		}
	}
}
