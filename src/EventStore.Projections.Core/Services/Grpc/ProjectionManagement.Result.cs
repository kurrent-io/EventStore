using System.Text.Json;
using System.Threading.Tasks;
using EventStore.Core.Messaging;
using EventStore.Grpc.Projections;
using EventStore.Projections.Core.Messages;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace EventStore.Projections.Core.Services.Grpc {
	public partial class ProjectionManagement {
		public override async Task<ResultResp> Result(ResultReq request, ServerCallContext context) {
			var resetSource = new TaskCompletionSource<Value>();

			var options = request.Options;

			var name = options.Name;
			var partition = options.Partition ?? string.Empty;

			var envelope = new CallbackEnvelope(OnMessage);

			_queue.Publish(new ProjectionManagementMessage.Command.GetResult(envelope, name, partition));

			return new ResultResp {
				Result = await resetSource.Task.ConfigureAwait(false)
			};

			void OnMessage(Message message) {
				if (!(message is ProjectionManagementMessage.ProjectionResult result)) {
					resetSource.TrySetException(UnknownMessage<ProjectionManagementMessage.ProjectionResult>(message));
					return;
				}
				//todo: identify the correct return for a non-running projection, but let's not blow up the test host in the interim
				var resultTxt = string.IsNullOrWhiteSpace(result.Result) ? "{}" : result.Result;

				var document = JsonDocument.Parse(resultTxt);

				resetSource.TrySetResult(GetProtoValue(document.RootElement));
			}
		}

		public override async Task<StateResp> State(StateReq request, ServerCallContext context) {
			var resetSource = new TaskCompletionSource<Value>();

			var options = request.Options;

			var name = options.Name;
			var partition = options.Partition ?? string.Empty;

			var envelope = new CallbackEnvelope(OnMessage);

			_queue.Publish(new ProjectionManagementMessage.Command.GetState(envelope, name, partition));

			return new StateResp {
				State = await resetSource.Task.ConfigureAwait(false)
			};

			void OnMessage(Message message) {
				if (!(message is ProjectionManagementMessage.ProjectionState result)) {
					resetSource.TrySetException(UnknownMessage<ProjectionManagementMessage.ProjectionState>(message));
					return;
				}
				//todo: identify the correct return for a non-running projection, but let's not blow up the test host in the interim
				var state = string.IsNullOrWhiteSpace(result.State) ? "{}" : result.State;
				var document = JsonDocument.Parse(state);

				resetSource.TrySetResult(GetProtoValue(document.RootElement));
			}
		}
	}
}
