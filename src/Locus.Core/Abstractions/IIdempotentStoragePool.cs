using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Optional storage capability for deduplicating retried writes by a stable operation ID.
    /// </summary>
    public interface IIdempotentStoragePool
    {
        /// <summary>
        /// Writes a file once for the supplied operation ID, returning the original file key
        /// when the same operation is retried.
        /// </summary>
        Task<string> WriteFileIdempotentlyAsync(
            ITenantContext tenant,
            Stream content,
            string? originalFileName,
            string operationId,
            CancellationToken ct = default);
    }
}
