// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using Serilog;

namespace EventStore.ClusterNode.Services;

public sealed class MonitoringService : IDisposable {
	static readonly ILogger Log = Serilog.Log.ForContext<MonitoringService>();

	readonly MeterListener _meterListener = new();

	public MonitoringService() {
		_meterListener.InstrumentPublished = (instrument, listener) => {
			Log.Information("Instrument published: {instrument}", instrument.Name);
			if (instrument.Name.StartsWith("http")
			    || instrument.Name.StartsWith("kestrel")) {
			    // || instrument.Name.StartsWith("eventstore")) {
				listener.EnableMeasurementEvents(instrument);
			}
		};
		_meterListener.SetMeasurementEventCallback<int>(OnMeasurementRecorded);
		_meterListener.SetMeasurementEventCallback<long>(OnMeasurementRecorded);
		_meterListener.SetMeasurementEventCallback<double>(OnMeasurementRecorded);
		_meterListener.Start();
	}

	void OnMeasurementRecorded<T>(
		Instrument instrument,
		T measurement,
		ReadOnlySpan<KeyValuePair<string, object>> tags,
		object state) {
		if (instrument.Name != "kestrel.active_connections" || measurement is not long l) return;
		Interlocked.Add(ref _nrActiveConnections, l);
		DataUpdated?.Invoke(null, EventArgs.Empty);
	}

	public event EventHandler<EventArgs> DataUpdated;

	public long ActiveConnections => _nrActiveConnections;

	long _nrActiveConnections = 0;

	public void Dispose() {
		_meterListener.Dispose();
	}

	DateTime _lastTime = DateTime.MinValue;
	readonly Process _process = Process.GetCurrentProcess();
	TimeSpan _lastTotalProcessorTime;
	DateTime _curTime;
	TimeSpan _curTotalProcessorTime;

	public double CalculateCpu() {
		if (_lastTime == DateTime.MinValue) {
			_lastTime = DateTime.Now;
			_lastTotalProcessorTime = _process.TotalProcessorTime;
			return 0;
		}

		_curTime = DateTime.Now;
		_curTotalProcessorTime = _process.TotalProcessorTime;

		double cpuUsage = (_curTotalProcessorTime.TotalMilliseconds - _lastTotalProcessorTime.TotalMilliseconds) / _curTime.Subtract(_lastTime).TotalMilliseconds;

		_lastTime = _curTime;
		_lastTotalProcessorTime = _curTotalProcessorTime;
		return cpuUsage;
	}

	public (double Total, double Used) CalculateRam() {
		var processRam = _process.WorkingSet64;
		var total = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
		var totalGb = (double)total / 1024 / 1024 / 1024;
		var processGb = (double)processRam / 1024 / 1024 / 1024;
		return (totalGb, processGb);
	}
}
