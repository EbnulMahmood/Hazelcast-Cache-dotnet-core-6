using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;

namespace Caching
{
    internal static class OptionsExtensions
    {
        public static T SafeValue<T>(this IOptions<T> options, [CallerArgumentExpression("options")] string? argumentName = null)
        where T : class
        {
            if (options == null) throw new ArgumentNullException(argumentName);
            return options.Value;
        }
    }
}
