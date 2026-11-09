#if NETSTANDARD2_0
using System;

namespace System.Diagnostics.CodeAnalysis
{
    /// <summary>
    /// Polyfill for the nullable flow-analysis attribute that netstandard2.0 does not ship.
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter, Inherited = false)]
    internal sealed class NotNullWhenAttribute : Attribute
    {
        public NotNullWhenAttribute(bool returnValue)
        {
            ReturnValue = returnValue;
        }

        public bool ReturnValue { get; }
    }
}
#endif
