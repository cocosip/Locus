using System;
using System.IO;
using System.IO.Abstractions;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Locus.Storage
{
    internal interface ISourceFileRelocator
    {
        Task<string> RelocateAsync(string sourcePath, string destinationPath, CancellationToken ct);
    }

    /// <summary>
    /// Relocates watcher source files using copy, durable flush, verification, and source deletion.
    /// </summary>
    internal sealed class SourceFileRelocator : ISourceFileRelocator
    {
        private const int BufferSize = 81920;
        private readonly IFileSystem _fileSystem;
        private readonly Func<string, FileMode, FileAccess, FileShare, Stream> _openStream;

        internal SourceFileRelocator(
            IFileSystem fileSystem,
            Func<string, FileMode, FileAccess, FileShare, Stream>? openStream = null)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _openStream = openStream ?? ((path, mode, access, share) =>
                _fileSystem.File.Open(path, mode, access, share));
        }

        public async Task<string> RelocateAsync(
            string sourcePath,
            string destinationPath,
            CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(sourcePath))
                throw new ArgumentException("Source path cannot be empty.", nameof(sourcePath));
            if (string.IsNullOrWhiteSpace(destinationPath))
                throw new ArgumentException("Destination path cannot be empty.", nameof(destinationPath));

            var normalizedSource = _fileSystem.Path.GetFullPath(sourcePath);
            var normalizedDestination = _fileSystem.Path.GetFullPath(destinationPath);
            if (string.Equals(normalizedSource, normalizedDestination, StringComparison.OrdinalIgnoreCase))
                throw new IOException("Source and destination paths must be different.");

            ct.ThrowIfCancellationRequested();
            var sourceFingerprint = await ReadFingerprintAsync(sourcePath, ct).ConfigureAwait(false);
            var candidatePath = destinationPath;
            var alternateIndex = 0;

            while (true)
            {
                ct.ThrowIfCancellationRequested();
                if (_fileSystem.File.Exists(candidatePath))
                {
                    var destinationFingerprint = await ReadFingerprintAsync(candidatePath, ct).ConfigureAwait(false);
                    if (sourceFingerprint.ContentEquals(destinationFingerprint))
                    {
                        await VerifySourceAndDeleteAsync(sourcePath, sourceFingerprint, ct).ConfigureAwait(false);
                        return candidatePath;
                    }

                    candidatePath = BuildAlternatePath(destinationPath, sourceFingerprint.Hash, alternateIndex++);
                    continue;
                }

                var directory = _fileSystem.Path.GetDirectoryName(candidatePath);
                if (!string.IsNullOrWhiteSpace(directory))
                    _fileSystem.Directory.CreateDirectory(directory!);

                var createdDestination = false;
                try
                {
                    using (var source = _openStream(sourcePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
                    using (var destination = _openStream(candidatePath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                    {
                        createdDestination = true;
                        await CopyAsync(source, destination, ct).ConfigureAwait(false);
                        await destination.FlushAsync(ct).ConfigureAwait(false);
                        FlushToDisk(destination);
                    }

                    var copiedFingerprint = await ReadFingerprintAsync(candidatePath, ct).ConfigureAwait(false);
                    if (!sourceFingerprint.ContentEquals(copiedFingerprint))
                        throw new IOException($"Copied destination '{candidatePath}' failed content verification.");

                    await VerifySourceAndDeleteAsync(sourcePath, sourceFingerprint, ct).ConfigureAwait(false);
                    return candidatePath;
                }
                catch (IOException) when (!createdDestination && _fileSystem.File.Exists(candidatePath))
                {
                    candidatePath = BuildAlternatePath(destinationPath, sourceFingerprint.Hash, alternateIndex++);
                }
                catch
                {
                    if (createdDestination)
                        TryDeletePartialDestination(candidatePath);
                    throw;
                }
            }
        }

        private async Task VerifySourceAndDeleteAsync(
            string sourcePath,
            FileFingerprint expected,
            CancellationToken ct)
        {
            var current = await ReadFingerprintAsync(sourcePath, ct).ConfigureAwait(false);
            if (!expected.Equals(current))
                throw new IOException($"Source file '{sourcePath}' changed during relocation.");

            ct.ThrowIfCancellationRequested();
            _fileSystem.File.Delete(sourcePath);
        }

        private async Task<FileFingerprint> ReadFingerprintAsync(string path, CancellationToken ct)
        {
            var before = _fileSystem.FileInfo.New(path);
            var length = before.Length;
            var lastWriteTimeUtc = before.LastWriteTimeUtc;
            var creationTimeUtc = before.CreationTimeUtc;
            byte[] hash;

            using (var stream = _openStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
            using (var sha256 = SHA256.Create())
            {
                var buffer = new byte[BufferSize];
                while (true)
                {
                    var read = await stream.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false);
                    if (read == 0)
                        break;
                    sha256.TransformBlock(buffer, 0, read, null, 0);
                }
                sha256.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                hash = sha256.Hash!;
            }

            var after = _fileSystem.FileInfo.New(path);
            if (length != after.Length
                || lastWriteTimeUtc != after.LastWriteTimeUtc
                || creationTimeUtc != after.CreationTimeUtc)
            {
                throw new IOException($"File '{path}' changed while its fingerprint was being read.");
            }

            return new FileFingerprint(length, creationTimeUtc, lastWriteTimeUtc, hash);
        }

        private static async Task CopyAsync(Stream source, Stream destination, CancellationToken ct)
        {
            var buffer = new byte[BufferSize];
            while (true)
            {
                var read = await source.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false);
                if (read == 0)
                    return;
                await destination.WriteAsync(buffer, 0, read, ct).ConfigureAwait(false);
            }
        }

        private static void FlushToDisk(Stream destination)
        {
            if (destination is FileSystemStream fileSystemStream)
                fileSystemStream.Flush(flushToDisk: true);
            else
                destination.Flush();
        }

        private string BuildAlternatePath(string destinationPath, byte[] sourceHash, int suffix)
        {
            var directory = _fileSystem.Path.GetDirectoryName(destinationPath) ?? string.Empty;
            var name = _fileSystem.Path.GetFileNameWithoutExtension(destinationPath);
            var extension = _fileSystem.Path.GetExtension(destinationPath);
            var hashText = ToLowerHex(sourceHash, 8);
            var suffixText = suffix > 0 ? "-" + (suffix + 1).ToString() : string.Empty;
            return _fileSystem.Path.Combine(directory, $"{name}.{hashText}{suffixText}{extension}");
        }

        private static string ToLowerHex(byte[] bytes, int count)
        {
            var builder = new StringBuilder(count * 2);
            for (var i = 0; i < Math.Min(count, bytes.Length); i++)
                builder.Append(bytes[i].ToString("x2"));
            return builder.ToString();
        }

        private void TryDeletePartialDestination(string path)
        {
            try
            {
                if (_fileSystem.File.Exists(path))
                    _fileSystem.File.Delete(path);
            }
            catch
            {
                // Preserve the original relocation exception. A later retry will never overwrite this path.
            }
        }

        private sealed class FileFingerprint
        {
            internal FileFingerprint(long length, DateTime creationTimeUtc, DateTime lastWriteTimeUtc, byte[] hash)
            {
                Length = length;
                CreationTimeUtc = creationTimeUtc;
                LastWriteTimeUtc = lastWriteTimeUtc;
                Hash = hash;
            }

            internal long Length { get; }
            internal DateTime CreationTimeUtc { get; }
            internal DateTime LastWriteTimeUtc { get; }
            internal byte[] Hash { get; }

            internal bool ContentEquals(FileFingerprint other)
            {
                return Length == other.Length && FixedTimeEquals(Hash, other.Hash);
            }

            internal bool Equals(FileFingerprint other)
            {
                return Length == other.Length
                    && CreationTimeUtc == other.CreationTimeUtc
                    && LastWriteTimeUtc == other.LastWriteTimeUtc
                    && FixedTimeEquals(Hash, other.Hash);
            }

            private static bool FixedTimeEquals(byte[] left, byte[] right)
            {
                if (left.Length != right.Length)
                    return false;

                var difference = 0;
                for (var i = 0; i < left.Length; i++)
                    difference |= left[i] ^ right[i];
                return difference == 0;
            }
        }
    }
}
