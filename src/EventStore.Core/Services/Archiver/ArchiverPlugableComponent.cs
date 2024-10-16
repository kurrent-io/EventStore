// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System;
using System.Collections.Generic;
using EventStore.Core.Services.Archiver.Storage;
using EventStore.Plugins;
using EventStore.Plugins.Licensing;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventStore.Core.Services.Archiver;

public class ArchiverPlugableComponent : IPlugableComponent {
	public string Name => "Archiver";

	public string DiagnosticsName => Name;

	public KeyValuePair<string, object>[] DiagnosticsTags => [];

	public string Version => "0.0.1";

	public bool Enabled => true;

	public string LicensePublicKey => LicenseConstants.LicensePublicKey;

	public void ConfigureApplication(IApplicationBuilder builder, IConfiguration configuration) {
		_ = builder.ApplicationServices.GetService<ArchiverService>();

		var licenseService = builder.ApplicationServices.GetRequiredService<ILicenseService>();

		_ = LicenseMonitor.MonitorAsync(
			featureName: Name,
			requiredEntitlements: ["ARCHIVE"],
			licenseService: licenseService,
			onLicenseException: licenseService.RejectLicense,
			logger: builder.ApplicationServices.GetRequiredService<ILoggerFactory>().CreateLogger(GetType()));
	}

	public void ConfigureServices(IServiceCollection services, IConfiguration configuration) {
		var options = configuration.GetSection("EventStore:Archive").Get<ArchiverOptions>();
		if (options is null || options.StorageType is StorageType.None)
			return;

		var storage = CreateStorage(options);
		services.AddSingleton(storage);
		services.AddSingleton<ArchiverService>();
	}

	static IArchiveStorage CreateStorage(ArchiverOptions options) {
		return options.StorageType switch {
			StorageType.FileSystem => new FileSystemArchiveStorage(options.FileSystem),
			StorageType.S3 => new S3ArchiveStorage(options.S3),
			_ => throw new ArgumentOutOfRangeException(nameof(options.StorageType))
		};
	}
}
