// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DotNext.IO;
using EventStore.Common.Utils;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Transport.Tcp.Framing;
using ILogger = Serilog.ILogger;

namespace EventStore.Core.Helpers;

public sealed class LengthPrefixSuffixFramer : IAsyncMessageFramer<ReadOnlySequence<byte>> {
	private static readonly ILogger Log = Serilog.Log.ForContext<LengthPrefixSuffixFramer>();

	private const int PrefixLength = sizeof(int);

	public bool HasData {
		get { return _memStream.Length > 0; }
	}

	private readonly int _maxPackageSize;
	private Func<ReadOnlySequence<byte>, CancellationToken, ValueTask> _packageHandler = static (_, _) => ValueTask.CompletedTask;

	private readonly MemoryStream _memStream;

	private int _prefixBytes;
	private int _packageLength;

	public LengthPrefixSuffixFramer(int maxPackageSize = TFConsts.MaxLogRecordSize) {
		Ensure.Positive(maxPackageSize, "maxPackageSize");

		_maxPackageSize = maxPackageSize;
		_memStream = new MemoryStream();
	}

	public void Reset() {
		_memStream.SetLength(0);
		_prefixBytes = 0;
		_packageLength = 0;
	}

	public async ValueTask UnFrameData(IEnumerable<ArraySegment<byte>> data, CancellationToken token) {
		Ensure.NotNull(data, nameof(data));

		foreach (ArraySegment<byte> buffer in data) {
			await Parse(buffer, token);
		}
	}

	public void RegisterMessageArrivedCallback(Func<ReadOnlySequence<byte>, CancellationToken, ValueTask> packageHandler) {
		Ensure.NotNull(packageHandler, nameof(packageHandler));
		_packageHandler = packageHandler;
	}

	public ValueTask UnFrameData(ArraySegment<byte> data, CancellationToken token) => Parse(data, token);

	// Parses a stream chunking based on length-prefixed-suffixed framing. Calls are re-entrant and hold state internally.
	private async ValueTask Parse(ArraySegment<byte> bytes, CancellationToken token) {
		byte[] data = bytes.Array;
		Debug.Assert(data is not null);

		for (int i = bytes.Offset; i < bytes.Offset + bytes.Count;) {
			if (_prefixBytes < PrefixLength) {
				_packageLength |= (data[i] << (_prefixBytes * 8)); // little-endian order
				_prefixBytes += 1;
				i += 1;
				if (_prefixBytes == PrefixLength) {
					if (_packageLength <= 0 || _packageLength > _maxPackageSize) {
						Log.Error("FRAMING ERROR! Data:\n{data}", Helper.FormatBinaryDump(bytes));
						throw new PackageFramingException(string.Format(
							"Package size is out of bounds: {0} (max: {1}).",
							_packageLength, _maxPackageSize));
					}

					_packageLength += PrefixLength; // we need to read suffix as well
				}
			} else {
				int copyCnt = Math.Min(bytes.Count + bytes.Offset - i, _packageLength - (int)_memStream.Length);
				_memStream.Write(data, i, copyCnt);
				i += copyCnt;

				if (_memStream.Length == _packageLength) {
#if DEBUG
					var buf = _memStream.GetBuffer();
					int suffixLength = (buf[_packageLength - 4] << 0)
					                   | (buf[_packageLength - 3] << 8)
					                   | (buf[_packageLength - 2] << 16)
					                   | (buf[_packageLength - 1] << 24);
					if (_packageLength - PrefixLength != suffixLength) {
						throw new Exception(string.Format("Prefix length: {0} is not equal to suffix length: {1}.",
							_packageLength - PrefixLength, suffixLength));
					}
#endif
					_memStream.SetLength(suffixLength); // remove suffix length
					_memStream.Position = 0;

					await _packageHandler(new(buf, 0, suffixLength), token);

					_memStream.SetLength(0);
					_prefixBytes = 0;
					_packageLength = 0;
				}
			}
		}
	}

	public IEnumerable<ArraySegment<byte>> FrameData(ArraySegment<byte> data) {
		var length = data.Count;

		var lengthArray = new ArraySegment<byte>(
			new[] {(byte)length, (byte)(length >> 8), (byte)(length >> 16), (byte)(length >> 24)});
		yield return lengthArray;
		yield return data;
		yield return lengthArray;
	}
}
