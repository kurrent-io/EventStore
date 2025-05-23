// Copyright (c) Kurrent, Inc and/or licensed to Kurrent, Inc under one or more agreements.
// Kurrent, Inc licenses this file to you under the Kurrent License v1 (see LICENSE.md).

using KurrentDB.Connectors.Management.Contracts.Commands;
using FluentValidation;

namespace KurrentDB.Connectors.Planes.Management;

[UsedImplicitly]
public class CreateConnectorValidator : RequestValidator<CreateConnector> {
    public CreateConnectorValidator() : base(x => x.ConnectorId) {
        RuleFor(x => x.Name)
            .Must((cmd, name) => {
                if (!string.IsNullOrWhiteSpace(name?.Trim()))
                    return true;

                cmd.Name = cmd.ConnectorId;
                return true;
            })
            .DependentRules(() => AddNameValidationRules(x => x.Name))
            .When(x => x.Name != x.ConnectorId);
    }
}

[UsedImplicitly]
public class DeleteConnectorValidator() : RequestValidator<DeleteConnector>(x => x.ConnectorId);

[UsedImplicitly]
public class ReconfigureConnectorValidator() : RequestValidator<ReconfigureConnector>(x => x.ConnectorId);

[UsedImplicitly]
public class StartConnectorValidator() : RequestValidator<StartConnector>(x => x.ConnectorId);

[UsedImplicitly]
public class ResetConnectorValidator() : RequestValidator<ResetConnector>(x => x.ConnectorId);

[UsedImplicitly]
public class StopConnectorValidator() : RequestValidator<StopConnector>(x => x.ConnectorId);

[UsedImplicitly]
public class RenameConnectorValidator : RequestValidator<RenameConnector> {
    public RenameConnectorValidator() : base(x => x.ConnectorId) => AddNameValidationRules(x => x.Name);
}
