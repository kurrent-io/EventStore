// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Tests;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.ClientAPI.event_by_type_index;

[TestFixture(typeof(LogFormat.V2), typeof(string))]
[TestFixture(typeof(LogFormat.V3), typeof(uint))]
public class when_reverting_after_index_catches_up<TLogFormat, TStreamId> : specification_with_standard_projections_runnning<TLogFormat, TStreamId> {
	protected override bool GivenStandardProjectionsRunning() {
		return false;
	}

	protected override async Task Given() {
		await base.Given();
		await PostEvent("stream-1", "type1", "{}");
		await PostEvent("stream-1", "type2", "{}");
		await PostEvent("stream-2", "type1", "{}");
		await PostEvent("stream-2", "type2", "{}");
		WaitIdle();
		await PostProjection(@"
fromAll().foreachStream().when({
    $init: function(){return {a:0}},
    type1: function(s,e){s.a++},
    type2: function(s,e){s.a++},
    $deleted: function(s,e){s.deleted=1;},
}).outputState();
");
		await _manager.AbortAsync("test-projection", _admin);
		WaitIdle();

		await EnableStandardProjections();
		WaitIdle();
		await DisableStandardProjections();
		WaitIdle();
		await EnableStandardProjections();
		WaitIdle();
	}

	protected override async Task When() {
		await base.When();
		await _manager.EnableAsync("test-projection", _admin);
		WaitIdle();
	}

	[Test, Category("Network")]
	public async Task receives_deleted_notification() {
		await AssertStreamTail("$projections-test-projection-stream-1-result", "Result:{\"a\":1}", "Result:{\"a\":2}");
		await AssertStreamTail("$projections-test-projection-stream-2-result", "Result:{\"a\":1}", "Result:{\"a\":2}");
	}
}
