using System;
using System.IO;
using System.IO.Abstractions;
using Locus.FileSystem;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Locus.IntegrationTests
{
    /// <summary>
    /// 验证 LocalFileSystemVolume 会自动创建挂载路径的测试
    /// </summary>
    public class AutoCreateDirectoryTest : IDisposable
    {
        private readonly string _testRoot;

        public AutoCreateDirectoryTest()
        {
            _testRoot = Path.Combine(Path.GetTempPath(), "locus-auto-create-test", Guid.NewGuid().ToString());
        }

        [Fact]
        public void LocalFileSystemVolume_AutoCreates_MountPath()
        {
            // Arrange
            var mountPath = Path.Combine(_testRoot, "deep", "nested", "path", "volume-1");

            // 确保目录不存在
            Assert.False(Directory.Exists(mountPath), "目录不应该提前存在");

            var fileSystem = new System.IO.Abstractions.FileSystem();
            var logger = LoggerFactory.Create(builder => builder.AddConsole())
                .CreateLogger<LocalFileSystemVolume>();

            // Act - 创建 LocalFileSystemVolume，应该自动创建目录
            var volume = new LocalFileSystemVolume(
                fileSystem,
                logger,
                "test-volume",
                mountPath,
                shardingDepth: 2);

            // Assert
            Assert.True(Directory.Exists(mountPath), "目录应该被自动创建");
            Assert.Equal(mountPath, volume.MountPath);
            Assert.True(volume.IsHealthy, "卷应该是健康的");
        }

        [Fact]
        public void LocalFileSystemVolume_AutoCreates_NestedPath_With_Five_Levels()
        {
            // Arrange - 5层深度的路径
            var mountPath = Path.Combine(_testRoot, "a", "b", "c", "d", "e", "volume-1");

            // 确保所有父目录都不存在
            Assert.False(Directory.Exists(_testRoot));
            Assert.False(Directory.Exists(Path.Combine(_testRoot, "a")));
            Assert.False(Directory.Exists(Path.Combine(_testRoot, "a", "b")));
            Assert.False(Directory.Exists(mountPath));

            var fileSystem = new System.IO.Abstractions.FileSystem();
            var logger = LoggerFactory.Create(builder => builder.AddConsole())
                .CreateLogger<LocalFileSystemVolume>();

            // Act
            var volume = new LocalFileSystemVolume(
                fileSystem,
                logger,
                "test-volume",
                mountPath,
                shardingDepth: 2);

            // Assert - 所有目录都应该被创建
            Assert.True(Directory.Exists(_testRoot));
            Assert.True(Directory.Exists(Path.Combine(_testRoot, "a")));
            Assert.True(Directory.Exists(Path.Combine(_testRoot, "a", "b")));
            Assert.True(Directory.Exists(Path.Combine(_testRoot, "a", "b", "c")));
            Assert.True(Directory.Exists(mountPath));
        }

        [Fact]
        public async System.Threading.Tasks.Task LocalFileSystemVolume_Can_Write_File_After_AutoCreate()
        {
            // Arrange
            var mountPath = Path.Combine(_testRoot, "auto-created", "volume");

            var fileSystem = new System.IO.Abstractions.FileSystem();
            var logger = LoggerFactory.Create(builder => builder.AddConsole())
                .CreateLogger<LocalFileSystemVolume>();

            // Act
            var volume = new LocalFileSystemVolume(fileSystem, logger, "test-vol", mountPath, 2);

            // 尝试写入文件
            var testFilePath = Path.Combine(mountPath, "test.txt");
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("test content"));
            await volume.WriteAsync(testFilePath, stream, default);

            // Assert
            Assert.True(File.Exists(testFilePath), "文件应该被成功写入");
            var content = File.ReadAllText(testFilePath);
            Assert.Equal("test content", content);
        }

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(_testRoot))
                {
                    Directory.Delete(_testRoot, recursive: true);
                }
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}
