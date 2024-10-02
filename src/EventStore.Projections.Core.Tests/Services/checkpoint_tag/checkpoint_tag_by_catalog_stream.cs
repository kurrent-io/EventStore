// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Services.Processing.Checkpointing;
using NUnit.Framework;

#pragma warning disable 1718 // allow a == a comparison

namespace EventStore.Projections.Core.Tests.Services.checkpoint_tag {
	[TestFixture]
	public class checkpoint_tag_by_stream_position {
		private readonly CheckpointTag _a = CheckpointTag.FromStreamPosition(1, "stream", 9);
		private readonly CheckpointTag _b = CheckpointTag.FromStreamPosition(1, "stream", 15);
		private readonly CheckpointTag _c = CheckpointTag.FromStreamPosition(1, "stream", 29);

		[Test]
		public void equal_equals() {
			Assert.IsTrue(_a.Equals(_a));
		}

		[Test]
		public void equal_operator() {
			Assert.IsTrue(_b == _b);
		}

		[Test]
		public void less_operator() {
			Assert.IsTrue(_a < _b);
		}

		[Test]
		public void less_or_equal_operator() {
			Assert.IsTrue(_a <= _b);
			Assert.IsTrue(_c <= _c);
		}

		[Test]
		public void greater_operator() {
			Assert.IsTrue(_b > _a);
		}

		[Test]
		public void greater_or_equal_operator() {
			Assert.IsTrue(_b >= _a);
			Assert.IsTrue(_c >= _c);
		}
	}
#pragma warning restore 1718
}
