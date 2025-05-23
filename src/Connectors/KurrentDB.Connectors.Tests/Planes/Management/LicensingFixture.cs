// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using EventStore.Plugins.Licensing;
using KurrentDB.Connectors.Planes.Management;
using KurrentDB.Surge.Testing.Fixtures;
using Microsoft.Extensions.Logging;

namespace KurrentDB.Connectors.Tests.Planes.Management;

[PublicAPI]
public class LicensingFixture : FastFixture {
    public ILogger<ConnectorsLicenseService> LicensingLogger => LoggerFactory.CreateLogger<ConnectorsLicenseService>();

    public IObservable<License> NewLicenseObservable(License license) => new SimpleObservable([license]);

    public IObservable<License> NewEmptyLicenseObservable() => new SimpleObservable([]);

    public License NewLicense(string[] entitlements) {
        return License.Create(
            entitlements.ToDictionary(x => x, _ => (object)true));
    }
}

class SimpleObservable(IEnumerable<License> licenses) : IObservable<License> {
    IEnumerable<License> Licenses { get; } = licenses;

    public SimpleObservable() : this([]) {}

    public IDisposable Subscribe(IObserver<License> observer) {
        foreach (var license in Licenses)
            observer.OnNext(license);

        return null!;
    }
}
