using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class StoragePoolExtensionsTests
    {
        [Fact]
        public async Task WriteFileAsync_ByteArray_DefaultTenant_UsesVisibleMemoryStream()
        {
            var storagePool = new Mock<IStoragePool>();
            var payload = new byte[] { 1, 2, 3, 4 };

            storagePool
                .Setup(p => p.WriteFileAsync(
                    It.IsAny<ITenantContext>(),
                    It.IsAny<Stream>(),
                    "scan.dcm",
                    "/incoming",
                    It.IsAny<CancellationToken>()))
                .Returns<ITenantContext, Stream, string, string, CancellationToken>((tenant, stream, _, _, _) =>
                {
                    Assert.Equal("default", tenant.TenantId);
                    Assert.Equal(TenantStatus.Enabled, tenant.Status);

                    var memoryStream = Assert.IsType<MemoryStream>(stream);
                    Assert.True(memoryStream.TryGetBuffer(out var segment));
                    Assert.Same(payload, segment.Array);
                    Assert.Equal(0, segment.Offset);
                    Assert.Equal(payload.Length, segment.Count);
                    Assert.Equal(payload.Length, memoryStream.Length);
                    Assert.Equal(0, memoryStream.Position);

                    return Task.FromResult("file-key");
                });

            var fileKey = await storagePool.Object.WriteFileAsync(payload, "scan.dcm", "/incoming", CancellationToken.None);

            Assert.Equal("file-key", fileKey);
        }

        [Fact]
        public async Task WriteFileAsync_ArraySegment_ExplicitTenant_PreservesSliceAsVisibleMemoryStream()
        {
            var storagePool = new Mock<IStoragePool>();
            var tenant = new Mock<ITenantContext>();
            tenant.SetupGet(t => t.TenantId).Returns("tenant-001");
            tenant.SetupGet(t => t.Status).Returns(TenantStatus.Enabled);

            var buffer = new byte[] { 9, 8, 7, 6, 5, 4 };
            var segment = new ArraySegment<byte>(buffer, 2, 3);

            storagePool
                .Setup(p => p.WriteFileAsync(
                    tenant.Object,
                    It.IsAny<Stream>(),
                    "part.dcm",
                    "/dicom",
                    It.IsAny<CancellationToken>()))
                .Returns<ITenantContext, Stream, string, string, CancellationToken>((_, stream, _, _, _) =>
                {
                    var memoryStream = Assert.IsType<MemoryStream>(stream);
                    Assert.True(memoryStream.TryGetBuffer(out var visibleSegment));
                    Assert.Same(buffer, visibleSegment.Array);
                    Assert.Equal(segment.Offset, visibleSegment.Offset);
                    Assert.Equal(segment.Count, visibleSegment.Count);
                    Assert.Equal(segment.Count, memoryStream.Length);
                    Assert.Equal(0, memoryStream.Position);

                    var bytes = memoryStream.ToArray();
                    Assert.Equal(new byte[] { 7, 6, 5 }, bytes);

                    return Task.FromResult("segment-key");
                });

            var fileKey = await storagePool.Object.WriteFileAsync(
                tenant.Object,
                segment,
                "part.dcm",
                "/dicom",
                CancellationToken.None);

            Assert.Equal("segment-key", fileKey);
        }
    }
}
