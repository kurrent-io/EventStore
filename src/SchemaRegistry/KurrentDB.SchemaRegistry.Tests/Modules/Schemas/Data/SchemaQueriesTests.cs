using DuckDB.NET.Data;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Kurrent.Surge.Projectors;
using Kurrent.Surge.Schema.Validation;
using KurrentDB.Surge.Testing.Messages.Telemetry;
using KurrentDB.SchemaRegistry.Tests.Fixtures;
using KurrentDB.Protocol.Registry.V2;
using KurrentDB.SchemaRegistry.Data;
using KurrentDB.SchemaRegistry.Protocol.Schemas.Events;

namespace KurrentDB.SchemaRegistry.Tests.Schemas.Data;

public class SchemaQueriesTests : SchemaRegistryServerTestFixture {
	[Test, Timeout(10_000)]
	public async Task get_schema_version_with_version_number_returns_version(CancellationToken cancellationToken) {
		// Arrange
		var schemaName = $"{nameof(PowerConsumption)}-{Identifiers.GenerateShortId()}";

		var schemaDefinition = Faker.Lorem.Sentences(10, Environment.NewLine);

		var createSchemaCommand = new CreateSchemaRequest {
			SchemaName = schemaName,
			SchemaDefinition = ByteString.CopyFromUtf8(schemaDefinition),
			Details = new SchemaDetails {
				Description = Faker.Lorem.Text(),
				DataFormat = SchemaDataFormat.Json,
				Compatibility = Faker.Random.Enum(CompatibilityMode.Unspecified),
				Tags = {
					new Dictionary<string, string> {
						[Faker.Lorem.Word()] = Faker.Lorem.Word(),
						[Faker.Lorem.Word()] = Faker.Lorem.Word(),
						[Faker.Lorem.Word()] = Faker.Lorem.Word()
					}
				}
			}
		};

		var createSchemaResponse = await Client.CreateSchemaAsync(createSchemaCommand, cancellationToken: cancellationToken);

		var expectedResponse = new GetSchemaVersionResponse {
			Version = new SchemaVersion {
				SchemaVersionId = createSchemaResponse.SchemaVersionId,
				SchemaDefinition = createSchemaCommand.SchemaDefinition,
				DataFormat = createSchemaCommand.Details.DataFormat,
				VersionNumber = createSchemaResponse.VersionNumber,
				RegisteredAt = Timestamp.FromDateTime(TimeProvider.GetUtcNow().UtcDateTime)
			}
		};

		// Act
		// await Wait.UntilAsserted(
		//     async () => {
		//         var response = await Client.GetSchemaVersionAsync(
		//             new GetSchemaVersionRequest {
		//                 SchemaName    = schemaName,
		//                 VersionNumber = createSchemaResponse.VersionNumber
		//             },
		//             cancellationToken: cancellationToken
		//         );
		//
		//         response.Should().BeEquivalentTo(expectedResponse);
		//     },
		//     cancellationToken: cancellationToken
		// );

		// await Tasks.SafeDelay(1_000, cancellationToken);

		var response = await Client.GetSchemaVersionAsync(
			new GetSchemaVersionRequest {
				SchemaName = schemaName,
				VersionNumber = createSchemaResponse.VersionNumber
			},
			cancellationToken: cancellationToken
		);

		// Assert
		// WARNING!!! BECAUSE for some reason, FLUENT ASSERTIONS it is not using the options!!!
		expectedResponse.Version.RegisteredAt = response.Version.RegisteredAt;

		response.Should().BeEquivalentTo(
			expectedResponse, options => options
				.Using<DateTime>(ctx => ctx.Subject.Should().BeCloseTo(ctx.Expectation, 1.Seconds()))
				.WhenTypeIs<DateTime>()
		);
	}

	[Test]
	public async Task list_schemas_with_name_prefix(CancellationToken cancellationToken) {
		var fooSchemaName = NewSchemaName("foo");
		var barSchemaName = NewSchemaName("bar");

		var connection = DuckDBConnectionProvider.GetConnection();
		var projection = new SchemaProjections();
		await projection.Setup(connection, cancellationToken);

		await CreateSchema(projection, fooSchemaName, cancellationToken);
		await CreateSchema(projection, barSchemaName, cancellationToken);

		var queries = new SchemaQueries(DuckDBConnectionProvider, new NJsonSchemaCompatibilityManager());

		var response = await queries.ListSchemas(new ListSchemasRequest { SchemaNamePrefix = "foo" }, cancellationToken);
		response.Schemas.Count.Should().Be(1);

		var schema = response.Schemas.First();
		schema.SchemaName.Should().Be(fooSchemaName);
	}

	private async Task CreateSchema(SchemaProjections projections, string schemaName, CancellationToken cancellationToken) {
		var record = await CreateRecord(
			new SchemaCreated {
				SchemaName = schemaName,
				SchemaDefinition = ByteString.CopyFromUtf8(Faker.Lorem.Text()),
				Description = Faker.Lorem.Text(),
				DataFormat = SchemaDataFormat.Json,
				Compatibility = Faker.Random.Enum(CompatibilityMode.Unspecified),
				Tags = {
					new Dictionary<string, string> {
						[Faker.Lorem.Word()] = Faker.Lorem.Word(),
						[Faker.Lorem.Word()] = Faker.Lorem.Word(),
						[Faker.Lorem.Word()] = Faker.Lorem.Word()
					}
				},
				SchemaVersionId = Guid.NewGuid().ToString(),
				VersionNumber = 1,
				CreatedAt = Timestamp.FromDateTimeOffset(TimeProvider.GetUtcNow())
			}
		);

		await projections.ProjectRecord(new ProjectionContext<DuckDBConnection>(_ => ValueTask.FromResult(DuckDBConnectionProvider.GetConnection()), record,
			cancellationToken));
	}
}
