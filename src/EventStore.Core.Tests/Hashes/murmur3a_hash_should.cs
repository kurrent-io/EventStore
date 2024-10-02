// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using EventStore.Core.Index.Hashes;
using NUnit.Framework;

namespace EventStore.Core.Tests.Hashes {
	[TestFixture]
	public class murmur3a_hash_should {
		public static uint Murmur3AReferenceVerificationValue = 0xB0F57EE3; // taken from SMHasher

		[Test]
		public void pass_smhasher_verification_test() {
			Assert.IsTrue(SMHasher.VerificationTest(new Murmur3AUnsafe(), Murmur3AReferenceVerificationValue));
		}

		[Test, Category("LongRunning"), Explicit]
		public void pass_smhasher_sanity_test() {
			Assert.IsTrue(SMHasher.SanityTest(new Murmur3AUnsafe()));
		}
	}
}
