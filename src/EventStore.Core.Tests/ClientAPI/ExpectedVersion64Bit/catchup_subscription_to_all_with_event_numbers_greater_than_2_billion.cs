extern alias GrpcClient;
extern alias GrpcClientStreams;
using System.Collections.Generic;
using System;
using NUnit.Framework;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Tests.ClientAPI.Helpers;
using GrpcClient::EventStore.Client;
using EventRecord = EventStore.Core.Data.EventRecord;
using ExpectedVersion = EventStore.Core.Tests.ClientAPI.Helpers.ExpectedVersion;
using StreamMetadata = GrpcClientStreams::EventStore.Client.StreamMetadata;

namespace EventStore.Core.Tests.ClientAPI.ExpectedVersion64Bit {
	[TestFixture(typeof(LogFormat.V2), typeof(string))]
	[TestFixture(typeof(LogFormat.V3), typeof(uint))]
	[Category("ClientAPI"), Category("LongRunning")]
	public class catchup_subscription_to_all_with_event_numbers_greater_than_2_billion<TLogFormat, TStreamId>
		: MiniNodeWithExistingRecords<TLogFormat, TStreamId> {
		private const long intMaxValue = (long)int.MaxValue;

		private string _streamId = "subscriptions-catchup-all";

		private EventRecord _r1, _r2;

		public override void WriteTestScenario() {
			_r1 = WriteSingleEvent(_streamId, intMaxValue + 1, new string('.', 3000));
			_r2 = WriteSingleEvent(_streamId, intMaxValue + 2, new string('.', 3000));
		}

		public override async Task Given() {
			_store = BuildConnection(Node);
			await _store.ConnectAsync();
			await _store.SetStreamMetadataAsync(_streamId, ExpectedVersion.Any,
				new StreamMetadata(truncateBefore: intMaxValue + 1));
		}

		[Test]
		public async Task should_be_able_to_subscribe_to_all_with_catchup_subscription() {
			var evnt = new EventData(Uuid.NewUuid(), "EventType", new byte[10], new byte[15]);
			List<ResolvedEvent> receivedEvents = new List<ResolvedEvent>();

			var countdown = new CountdownEvent(3);

			await _store.SubscribeToAllFrom(Position.Start, CatchUpSubscriptionSettings.Default, (s, e) => {
				if (e.Event.EventStreamId == _streamId) {
					receivedEvents.Add(e);
					countdown.Signal();
				}

				return Task.CompletedTask;
			}, userCredentials: DefaultData.AdminCredentials);

			await _store.AppendToStreamAsync(_streamId, intMaxValue + 2, evnt);

			Assert.That(countdown.Wait(TimeSpan.FromSeconds(10)), "Timed out waiting for events to appear");

			Assert.AreEqual(_r1.EventId, receivedEvents[0].Event.EventId.ToGuid());
			Assert.AreEqual(_r2.EventId, receivedEvents[1].Event.EventId.ToGuid());
			Assert.AreEqual(evnt.EventId, receivedEvents[2].Event.EventId);
		}
	}
}
