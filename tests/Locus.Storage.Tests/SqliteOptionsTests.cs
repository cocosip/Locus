using Locus.Core.Models;
using Xunit;

namespace Locus.Storage.Tests
{
    public class SqliteOptionsTests
    {
        [Fact]
        public void BuildConnectionString_DisablesPoolingForTenantScopedMaintenance()
        {
            var options = new SqliteOptions();

            var connectionString = options.BuildConnectionString("data/tenant-a/metadata.db");

            Assert.Contains("Mode=ReadWriteCreate", connectionString);
            Assert.Contains("Pooling=False", connectionString);
        }
    }
}
