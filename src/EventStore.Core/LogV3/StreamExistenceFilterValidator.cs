﻿using System;
using EventStore.Core.LogAbstraction;
using EventStore.Core.Services;

namespace EventStore.Core.LogV3 {
	public class StreamExistenceFilterValidator : INameExistenceFilter {
		private readonly INameExistenceFilter _wrapped;

		public StreamExistenceFilterValidator(INameExistenceFilter wrapped) {
			_wrapped = wrapped;
		}

		public long CurrentCheckpoint {
			get => _wrapped.CurrentCheckpoint;
			set => _wrapped.CurrentCheckpoint = value;
		}

		public void Initialize(INameExistenceFilterInitializer source) => _wrapped.Initialize(source);

		public void Verify() => _wrapped.Verify();

		public void Add(string streamName) {
			ValidateStreamName(streamName);
			_wrapped.Add(streamName);
		}

		public void Add(ulong hash) => throw new NotSupportedException();

		public bool MightContain(string streamName) {
			ValidateStreamName(streamName);
			return _wrapped.MightContain(streamName);
		}

		private static void ValidateStreamName(string streamName) {
			if (string.IsNullOrEmpty(streamName))
				throw new ArgumentException($"{nameof(streamName)} must not be null or empty", nameof(streamName));

			if (SystemStreams.IsMetastream(streamName))
				throw new ArgumentException($"{nameof(streamName)} must not be a metastream", nameof(streamName));

			if (LogV3SystemStreams.TryGetVirtualStreamId(streamName, out _))
				throw new ArgumentException($"{nameof(streamName)} must not be a virtual stream", nameof(streamName));
		}

		public void Dispose() => _wrapped?.Dispose();
	}
}
