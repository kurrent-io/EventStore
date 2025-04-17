// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;
using DuckDB.NET.Data;
using DuckDB.NET.Native;

namespace KurrentDB.Duck;

/// <summary>
/// Represents reusable appender.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public readonly partial struct Appender : IDisposable {
	private readonly nint appender;

	public Appender(DuckDBNativeConnection connection, ReadOnlySpan<byte> tableNameUtf8) {
		if (Create(connection.DangerousGetHandle(), 0, tableNameUtf8, out appender) is DuckDBState.Error) {
			var ex = Interop.CreateException(GetErrorString(appender));
			Destroy(in appender);
			appender = 0;
			throw ex;
		}
	}

	public Appender(DuckDBConnection connection, ReadOnlySpan<byte> tableNameUtf8)
		: this(connection.NativeConnection, tableNameUtf8) {
	}

	/// <summary>
	/// Flushes the appender.
	/// </summary>
	public void Flush() => VerifyState(Flush(appender) is DuckDBState.Error);

	[StackTraceHidden]
	private void VerifyState([DoesNotReturnIf(true)] bool isError)
		=> VerifyState(isError, appender);

	[StackTraceHidden]
	private static void VerifyState([DoesNotReturnIf(true)] bool isError, nint appender) {
		if (isError)
			throw Interop.CreateException(GetErrorString(appender));
	}

	public void Dispose() {
		if (appender is not 0 && Destroy(in appender) is DuckDBState.Error)
			throw Interop.CreateException("Unable to destroy appender.");
	}
}
