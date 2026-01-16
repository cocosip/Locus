using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus
{
    /// <summary>
    /// Background service that initializes pre-configured tenants on application startup.
    /// </summary>
    internal class TenantInitializationService : IHostedService
    {
        private readonly ITenantManager _tenantManager;
        private readonly ITenantQuotaManager _tenantQuotaManager;
        private readonly ILogger<TenantInitializationService> _logger;
        private readonly LocusOptions _options;

        public TenantInitializationService(
            ITenantManager tenantManager,
            ITenantQuotaManager tenantQuotaManager,
            ILogger<TenantInitializationService> logger,
            LocusOptions options)
        {
            _tenantManager = tenantManager ?? throw new ArgumentNullException(nameof(tenantManager));
            _tenantQuotaManager = tenantQuotaManager ?? throw new ArgumentNullException(nameof(tenantQuotaManager));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Initializing Locus tenant configuration...");

            try
            {
                // 1. Set global default quota
                if (_options.DefaultTenantQuota >= 0)
                {
                    await _tenantQuotaManager.SetGlobalLimitAsync(_options.DefaultTenantQuota, cancellationToken);
                    _logger.LogInformation("Set global tenant quota to {Quota} files (0 = unlimited)", _options.DefaultTenantQuota);
                }

                // 2. Initialize pre-configured tenants
                foreach (var tenantConfig in _options.Tenants)
                {
                    try
                    {
                        // Create tenant if it doesn't exist
                        var existingTenant = await _tenantManager.GetTenantAsync(tenantConfig.TenantId, cancellationToken);
                        if (existingTenant == null)
                        {
                            await _tenantManager.CreateTenantAsync(tenantConfig.TenantId, cancellationToken);
                            _logger.LogInformation("Created tenant: {TenantId}", tenantConfig.TenantId);
                        }

                        // Set tenant status
                        if (tenantConfig.Enabled)
                        {
                            await _tenantManager.EnableTenantAsync(tenantConfig.TenantId, cancellationToken);
                        }
                        else
                        {
                            await _tenantManager.DisableTenantAsync(tenantConfig.TenantId, cancellationToken);
                        }

                        // Set tenant-specific quota if specified
                        if (tenantConfig.Quota.HasValue)
                        {
                            await _tenantQuotaManager.SetTenantLimitAsync(tenantConfig.TenantId, tenantConfig.Quota.Value, cancellationToken);
                            _logger.LogInformation("Set quota for tenant {TenantId} to {Quota} files",
                                tenantConfig.TenantId, tenantConfig.Quota.Value);
                        }

                        _logger.LogInformation("Initialized tenant: {TenantId} (Enabled: {Enabled})",
                            tenantConfig.TenantId, tenantConfig.Enabled);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to initialize tenant: {TenantId}", tenantConfig.TenantId);
                        throw;
                    }
                }

                _logger.LogInformation("Tenant initialization completed. AutoCreateTenants: {AutoCreate}", _options.AutoCreateTenants);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize tenants");
                throw;
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
