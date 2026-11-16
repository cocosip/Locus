using Locus.Core.Validation;

namespace Locus.Storage.Tests;

public sealed class TenantIdValidatorTests
{
    [Theory]
    [InlineData("tenant-001")]
    [InlineData("t")]
    [InlineData("tenant_1.2")]
    [InlineData("ueber-tenant")]
    public void Validate_AcceptsPortableIds(string tenantId)
    {
        TenantIdValidator.Validate(tenantId, nameof(tenantId));
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    [InlineData("..")]
    [InlineData("../escape")]
    [InlineData("a/b")]
    [InlineData("a\\b")]
    [InlineData("a..b")]
    public void Validate_RejectsPathCharacters(string tenantId)
    {
        Assert.Throws<ArgumentException>(() => TenantIdValidator.Validate(tenantId, nameof(tenantId)));
    }

    [Fact]
    public void Validate_RejectsNull()
    {
        Assert.Throws<ArgumentException>(() => TenantIdValidator.Validate(null, "tenantId"));
    }

    [Fact]
    public void Validate_RejectsRootedAndDriveRelativePaths()
    {
        if (Path.DirectorySeparatorChar != '\\')
            return; // The remaining cases are Windows-only path syntax.

        Assert.Throws<ArgumentException>(() => TenantIdValidator.Validate(@"C:\escape", "tenantId"));
        Assert.Throws<ArgumentException>(() => TenantIdValidator.Validate("C:escape", "tenantId"));
    }

    [Fact]
    public void Validate_RejectsPlatformInvalidFileNameCharacters()
    {
        if (Path.DirectorySeparatorChar != '\\')
            return; // ':' is a legal file name character on Unix.

        Assert.Throws<ArgumentException>(() => TenantIdValidator.Validate("a:b", "tenantId"));
    }
}
