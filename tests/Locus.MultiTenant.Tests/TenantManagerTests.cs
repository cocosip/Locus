using System;
using System.IO.Abstractions.TestingHelpers;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Exceptions;
using Locus.Core.Models;
using Locus.MultiTenant;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Locus.MultiTenant.Tests
{
    public class TenantManagerTests
    {
        private readonly MockFileSystem _fileSystem;
        private readonly TenantManager _tenantManager;

        public TenantManagerTests()
        {
            _fileSystem = new MockFileSystem();
            _tenantManager = new TenantManager(
                _fileSystem,
                NullLogger<TenantManager>.Instance,
                ".locus/tenants"
            );
        }

        [Fact]
        public async Task CreateTenantAsync_CreatesNewTenant_Successfully()
        {
            // Arrange
            var tenantId = "tenant-001";

            // Act
            await _tenantManager.CreateTenantAsync(tenantId, CancellationToken.None);

            // Assert
            var tenant = await _tenantManager.GetTenantAsync(tenantId, CancellationToken.None);
            Assert.NotNull(tenant);
            Assert.Equal(tenantId, tenant.TenantId);
            Assert.Equal(TenantStatus.Enabled, tenant.Status);

            // Verify metadata file exists
            Assert.True(_fileSystem.File.Exists($".locus/tenants/{tenantId}.json"));

            // Verify storage directory exists
            Assert.True(_fileSystem.Directory.Exists($"storage/{tenantId}"));
        }

        [Fact]
        public async Task CreateTenantAsync_ThrowsException_WhenTenantAlreadyExists()
        {
            // Arrange
            var tenantId = "tenant-001";
            await _tenantManager.CreateTenantAsync(tenantId, CancellationToken.None);

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => _tenantManager.CreateTenantAsync(tenantId, CancellationToken.None)
            );
        }

        [Fact]
        public async Task GetTenantAsync_ThrowsException_WhenTenantNotFound()
        {
            // Arrange
            var tenantId = "nonexistent-tenant";

            // Act & Assert
            await Assert.ThrowsAsync<TenantNotFoundException>(
                () => _tenantManager.GetTenantAsync(tenantId, CancellationToken.None)
            );
        }

        [Fact]
        public async Task IsTenantEnabledAsync_ReturnsTrue_WhenTenantIsEnabled()
        {
            // Arrange
            var tenantId = "tenant-001";
            await _tenantManager.CreateTenantAsync(tenantId, CancellationToken.None);

            // Act
            var isEnabled = await _tenantManager.IsTenantEnabledAsync(tenantId, CancellationToken.None);

            // Assert
            Assert.True(isEnabled);
        }

        [Fact]
        public async Task IsTenantEnabledAsync_ReturnsFalse_WhenTenantIsDisabled()
        {
            // Arrange
            var tenantId = "tenant-001";
            await _tenantManager.CreateTenantAsync(tenantId, CancellationToken.None);
            await _tenantManager.DisableTenantAsync(tenantId, CancellationToken.None);

            // Act
            var isEnabled = await _tenantManager.IsTenantEnabledAsync(tenantId, CancellationToken.None);

            // Assert
            Assert.False(isEnabled);
        }

        [Fact]
        public async Task IsTenantEnabledAsync_ReturnsFalse_WhenTenantNotFound()
        {
            // Arrange
            var tenantId = "nonexistent-tenant";

            // Act
            var isEnabled = await _tenantManager.IsTenantEnabledAsync(tenantId, CancellationToken.None);

            // Assert
            Assert.False(isEnabled);
        }

        [Fact]
        public async Task DisableTenantAsync_UpdatesStatus_Successfully()
        {
            // Arrange
            var tenantId = "tenant-001";
            await _tenantManager.CreateTenantAsync(tenantId, CancellationToken.None);

            // Act
            await _tenantManager.DisableTenantAsync(tenantId, CancellationToken.None);

            // Assert
            var tenant = await _tenantManager.GetTenantAsync(tenantId, CancellationToken.None);
            Assert.Equal(TenantStatus.Disabled, tenant.Status);
        }

        [Fact]
        public async Task EnableTenantAsync_UpdatesStatus_Successfully()
        {
            // Arrange
            var tenantId = "tenant-001";
            await _tenantManager.CreateTenantAsync(tenantId, CancellationToken.None);
            await _tenantManager.DisableTenantAsync(tenantId, CancellationToken.None);

            // Act
            await _tenantManager.EnableTenantAsync(tenantId, CancellationToken.None);

            // Assert
            var tenant = await _tenantManager.GetTenantAsync(tenantId, CancellationToken.None);
            Assert.Equal(TenantStatus.Enabled, tenant.Status);
        }

        [Fact]
        public async Task GetAllTenantsAsync_ReturnsAllTenants_Successfully()
        {
            // Arrange
            await _tenantManager.CreateTenantAsync("tenant-001", CancellationToken.None);
            await _tenantManager.CreateTenantAsync("tenant-002", CancellationToken.None);
            await _tenantManager.CreateTenantAsync("tenant-003", CancellationToken.None);

            // Act
            var tenants = await _tenantManager.GetAllTenantsAsync(CancellationToken.None);

            // Assert
            Assert.Equal(3, System.Linq.Enumerable.Count(tenants));
        }

        [Fact]
        public async Task GetTenantAsync_UsesCaching_ForRepeatedCalls()
        {
            // Arrange
            var tenantId = "tenant-001";
            await _tenantManager.CreateTenantAsync(tenantId, CancellationToken.None);

            // Act - First call loads from file
            var tenant1 = await _tenantManager.GetTenantAsync(tenantId, CancellationToken.None);

            // Delete the file to verify cache is used
            _fileSystem.File.Delete($".locus/tenants/{tenantId}.json");

            // Act - Second call should use cache
            var tenant2 = await _tenantManager.GetTenantAsync(tenantId, CancellationToken.None);

            // Assert
            Assert.Equal(tenant1.TenantId, tenant2.TenantId);
            Assert.Equal(tenant1.Status, tenant2.Status);
        }

        [Fact]
        public async Task DisableTenantAsync_InvalidatesCache()
        {
            // Arrange
            var tenantId = "tenant-001";
            await _tenantManager.CreateTenantAsync(tenantId, CancellationToken.None);

            // Load into cache
            var tenant1 = await _tenantManager.GetTenantAsync(tenantId, CancellationToken.None);
            Assert.Equal(TenantStatus.Enabled, tenant1.Status);

            // Act - Disable should invalidate cache
            await _tenantManager.DisableTenantAsync(tenantId, CancellationToken.None);

            // Assert - Next call should get updated status
            var tenant2 = await _tenantManager.GetTenantAsync(tenantId, CancellationToken.None);
            Assert.Equal(TenantStatus.Disabled, tenant2.Status);
        }
    }
}
