// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using EventStore.Plugins.Authorization;
using KurrentDB.Core.Authorization.AuthorizationPolicies;

namespace KurrentDB.Core.Authorization;

public class InternalAuthorizationProviderFactory : IAuthorizationProviderFactory {
	private readonly IAuthorizationPolicyRegistry _registry;

	public InternalAuthorizationProviderFactory(IAuthorizationPolicyRegistry registry) {
		_registry = registry;
	}

	public IAuthorizationProvider Build() {
		return new PolicyAuthorizationProvider(
		new MultiPolicyEvaluator(_registry), logAuthorization: true, logSuccesses: false);
	}
}
