// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using EventStore.Plugins.Authorization;
using KurrentDB.Common.Utils;
using KurrentDB.Core.Bus;

namespace EventStore.Core.Services.Transport.Grpc;

internal partial class Operations : Client.Operations.Operations.OperationsBase {
	private readonly IPublisher _publisher;
	private readonly IAuthorizationProvider _authorizationProvider;

	public Operations(IPublisher publisher, IAuthorizationProvider authorizationProvider) {
		_publisher = Ensure.NotNull(publisher);
		_authorizationProvider = Ensure.NotNull(authorizationProvider);
	}
}
