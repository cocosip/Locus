using System;
using System.IO;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Locus.IntegrationTests
{
    /// <summary>
    /// 测试 DriveInfo 在不同操作系统上的行为
    /// </summary>
    public class LinuxDriveInfoTest
    {
        private readonly ITestOutputHelper _output;

        public LinuxDriveInfoTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void ShowDriveInfoBehavior()
        {
            // Arrange
            var testPath = "/data/TMEasy/locus/storage/volume-1";
            var root = Path.GetPathRoot(testPath);

            _output.WriteLine($"Operating System: {Environment.OSVersion}");
            _output.WriteLine($"Test Path: {testPath}");
            _output.WriteLine($"Path Root: '{root}'");
            _output.WriteLine("");

            // Act
            var drives = DriveInfo.GetDrives();

            _output.WriteLine($"Found {drives.Length} drives:");
            foreach (var drive in drives)
            {
                _output.WriteLine($"  - Name: '{drive.Name}', IsReady: {drive.IsReady}");
                if (drive.IsReady)
                {
                    _output.WriteLine($"    RootDirectory: '{drive.RootDirectory.FullName}'");
                    _output.WriteLine($"    Total: {drive.TotalSize / 1024 / 1024 / 1024} GB");
                    _output.WriteLine($"    Available: {drive.AvailableFreeSpace / 1024 / 1024 / 1024} GB");
                }
            }

            _output.WriteLine("");

            // 使用当前的匹配逻辑
            var matchedDrive = drives.FirstOrDefault(d =>
                d.Name.Equals(root, StringComparison.OrdinalIgnoreCase) && d.IsReady);

            _output.WriteLine($"Using Name.Equals('{root}'):");
            _output.WriteLine($"  Matched Drive: {matchedDrive?.Name ?? "NULL"}");

            // 改进的匹配逻辑 - 使用 RootDirectory.FullName
            var matchedDriveFixed = drives.FirstOrDefault(d =>
                d.IsReady && d.RootDirectory.FullName.Equals(root, StringComparison.OrdinalIgnoreCase));

            _output.WriteLine($"Using RootDirectory.FullName.Equals('{root}'):");
            _output.WriteLine($"  Matched Drive: {matchedDriveFixed?.Name ?? "NULL"}");
        }
    }
}
