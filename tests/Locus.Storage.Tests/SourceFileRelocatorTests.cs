using System;
using System.IO;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Locus.Storage.Tests
{
    public sealed class SourceFileRelocatorTests
    {
        [Fact]
        public async Task RelocateAsync_CopiesFlushesAndDeletesSource()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "incoming", "one.dcm");
                var destinationPath = Path.Combine(directory, "archive", "one.dcm");
                Directory.CreateDirectory(Path.GetDirectoryName(sourcePath)!);
                await File.WriteAllTextAsync(sourcePath, "content-one");

                var relocator = new SourceFileRelocator(new System.IO.Abstractions.FileSystem());
                var actualPath = await relocator.RelocateAsync(sourcePath, destinationPath, CancellationToken.None);

                Assert.Equal(destinationPath, actualPath);
                Assert.False(File.Exists(sourcePath));
                Assert.Equal("content-one", await File.ReadAllTextAsync(destinationPath));
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task RelocateAsync_WhenDestinationHasSameContent_DeletesOnlySource()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "incoming.dcm");
                var destinationPath = Path.Combine(directory, "archive.dcm");
                await File.WriteAllTextAsync(sourcePath, "same-content");
                await File.WriteAllTextAsync(destinationPath, "same-content");

                var relocator = new SourceFileRelocator(new System.IO.Abstractions.FileSystem());
                var actualPath = await relocator.RelocateAsync(sourcePath, destinationPath, CancellationToken.None);

                Assert.Equal(destinationPath, actualPath);
                Assert.False(File.Exists(sourcePath));
                Assert.Equal("same-content", await File.ReadAllTextAsync(destinationPath));
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task RelocateAsync_WhenDestinationHasDifferentContent_UsesDeterministicAlternateWithoutOverwrite()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "incoming.dcm");
                var destinationPath = Path.Combine(directory, "archive.dcm");
                await File.WriteAllTextAsync(sourcePath, "new-content");
                await File.WriteAllTextAsync(destinationPath, "existing-content");

                var relocator = new SourceFileRelocator(new System.IO.Abstractions.FileSystem());
                var firstActualPath = await relocator.RelocateAsync(sourcePath, destinationPath, CancellationToken.None);

                Assert.NotEqual(destinationPath, firstActualPath);
                Assert.Equal("existing-content", await File.ReadAllTextAsync(destinationPath));
                Assert.Equal("new-content", await File.ReadAllTextAsync(firstActualPath));

                await File.WriteAllTextAsync(sourcePath, "new-content");
                var secondActualPath = await relocator.RelocateAsync(sourcePath, destinationPath, CancellationToken.None);
                Assert.Equal(firstActualPath, secondActualPath);
                Assert.False(File.Exists(sourcePath));
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task RelocateAsync_WhenDestinationWriteFails_PreservesSourceAndRemovesPartialDestination()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "incoming.dcm");
                var destinationPath = Path.Combine(directory, "archive.dcm");
                await File.WriteAllTextAsync(sourcePath, "content-that-must-survive");
                var fileSystem = new System.IO.Abstractions.FileSystem();
                var relocator = new SourceFileRelocator(
                    fileSystem,
                    (path, mode, access, share) =>
                    {
                        var stream = fileSystem.File.Open(path, mode, access, share);
                        return access == FileAccess.Write
                            ? new FailAfterFirstWriteStream(stream)
                            : stream;
                    });

                await Assert.ThrowsAsync<IOException>(() =>
                    relocator.RelocateAsync(sourcePath, destinationPath, CancellationToken.None));

                Assert.True(File.Exists(sourcePath));
                Assert.Equal("content-that-must-survive", await File.ReadAllTextAsync(sourcePath));
                Assert.False(File.Exists(destinationPath));
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task RelocateAsync_WhenCancelled_PreservesSourceAndRemovesPartialDestination()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "incoming.dcm");
                var destinationPath = Path.Combine(directory, "archive.dcm");
                await File.WriteAllBytesAsync(sourcePath, new byte[256 * 1024]);
                var cancellation = new CancellationTokenSource();
                var fileSystem = new System.IO.Abstractions.FileSystem();
                var relocator = new SourceFileRelocator(
                    fileSystem,
                    (path, mode, access, share) =>
                    {
                        var stream = fileSystem.File.Open(path, mode, access, share);
                        return access == FileAccess.Write
                            ? new CancelAfterFirstWriteStream(stream, cancellation)
                            : stream;
                    });

                await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                    relocator.RelocateAsync(sourcePath, destinationPath, cancellation.Token));

                Assert.True(File.Exists(sourcePath));
                Assert.False(File.Exists(destinationPath));
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task RelocateAsync_WhenSourceChangesDuringCopy_PreservesSourceAndRemovesDestination()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "incoming.dcm");
                var destinationPath = Path.Combine(directory, "archive.dcm");
                await File.WriteAllBytesAsync(sourcePath, new byte[256 * 1024]);
                var fileSystem = new System.IO.Abstractions.FileSystem();
                var sourceMutated = false;
                var relocator = new SourceFileRelocator(
                    fileSystem,
                    (path, mode, access, share) =>
                    {
                        var stream = fileSystem.File.Open(path, mode, access, share);
                        if (access != FileAccess.Write || sourceMutated)
                            return stream;

                        sourceMutated = true;
                        return new MutateSourceAfterFirstWriteStream(stream, sourcePath);
                    });

                await Assert.ThrowsAsync<IOException>(() =>
                    relocator.RelocateAsync(sourcePath, destinationPath, CancellationToken.None));

                Assert.True(File.Exists(sourcePath));
                Assert.False(File.Exists(destinationPath));
                Assert.Equal((byte)1, (await File.ReadAllBytesAsync(sourcePath))[0]);
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        private static string CreateDirectory()
        {
            var path = Path.Combine(Path.GetTempPath(), "locus-source-relocator-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(path);
            return path;
        }

        private static void DeleteDirectory(string path)
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }

        private sealed class FailAfterFirstWriteStream : DelegatingStream
        {
            private bool _hasWritten;

            public FailAfterFirstWriteStream(Stream inner)
                : base(inner)
            {
            }

            public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                if (_hasWritten)
                    throw new IOException("Injected destination write failure.");

                _hasWritten = true;
                await base.WriteAsync(buffer, offset, Math.Min(count, 16), cancellationToken);
                throw new IOException("Injected destination write failure.");
            }
        }

        private sealed class CancelAfterFirstWriteStream : DelegatingStream
        {
            private readonly CancellationTokenSource _cancellation;

            public CancelAfterFirstWriteStream(Stream inner, CancellationTokenSource cancellation)
                : base(inner)
            {
                _cancellation = cancellation;
            }

            public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                await base.WriteAsync(buffer, offset, Math.Min(count, 16), cancellationToken);
                _cancellation.Cancel();
                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        private sealed class MutateSourceAfterFirstWriteStream : DelegatingStream
        {
            private readonly string _sourcePath;
            private bool _mutated;

            public MutateSourceAfterFirstWriteStream(Stream inner, string sourcePath)
                : base(inner)
            {
                _sourcePath = sourcePath;
            }

            public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                await base.WriteAsync(buffer, offset, count, cancellationToken);
                if (_mutated)
                    return;

                _mutated = true;
                using (var source = File.Open(_sourcePath, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete))
                    source.WriteByte(1);
            }
        }

        private abstract class DelegatingStream : Stream
        {
            private readonly Stream _inner;

            protected DelegatingStream(Stream inner)
            {
                _inner = inner;
            }

            public override bool CanRead => _inner.CanRead;
            public override bool CanSeek => _inner.CanSeek;
            public override bool CanWrite => _inner.CanWrite;
            public override long Length => _inner.Length;
            public override long Position { get => _inner.Position; set => _inner.Position = value; }
            public override void Flush() => _inner.Flush();
            public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);
            public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
                _inner.ReadAsync(buffer, offset, count, cancellationToken);
            public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
            public override void SetLength(long value) => _inner.SetLength(value);
            public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
                _inner.WriteAsync(buffer, offset, count, cancellationToken);

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                    _inner.Dispose();
                base.Dispose(disposing);
            }
        }
    }
}
