// autogenerated
using System.Threading;

#pragma warning disable CS0108 // Member hides inherited member; missing new keyword
namespace KurrentDB.SourceGenerators.Tests.Messaging.Abstract
{
	abstract partial class B
	{
		private partial class A
		{
			public static string OriginalLabelStatic { get; } = "TestMessageGroup-Abstract-A";
			public static string LabelStatic { get; set; } = "TestMessageGroup-Abstract-A";
			public override string Label => LabelStatic;
		}
	}
}
