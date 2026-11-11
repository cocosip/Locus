using System;
using System.Collections.Generic;
using System.IO.Abstractions;
using Locus.FileSystem;
using Moq;
using Xunit;

namespace Locus.FileSystem.Tests
{
    public class MountPointSelectionTests
    {
        [Fact]
        public void SelectDriveForVolume_ChoosesLongestMatchingMountPoint()
        {
            var root = CreateDrive("/", isReady: true);
            var data = CreateDrive("/mnt/data", isReady: true);

            var selected = LocalFileSystemVolume.SelectDriveForVolume(
                new List<IDriveInfo> { root, data },
                "/mnt/data/tenant-1",
                StringComparison.Ordinal,
                '/');

            Assert.Same(data, selected);
        }

        [Fact]
        public void SelectDriveForVolume_FallsBackToFileSystemRoot()
        {
            var root = CreateDrive("/", isReady: true);

            var selected = LocalFileSystemVolume.SelectDriveForVolume(
                new List<IDriveInfo> { root },
                "/var/locus",
                StringComparison.Ordinal,
                '/');

            Assert.Same(root, selected);
        }

        [Fact]
        public void SelectDriveForVolume_SkipsNotReadyDrivesAndSiblingPrefixes()
        {
            var notReady = CreateDrive("/mnt/data", isReady: false);
            var sibling = CreateDrive("/mnt/data-archive", isReady: true);
            var root = CreateDrive("/", isReady: true);

            var selected = LocalFileSystemVolume.SelectDriveForVolume(
                new List<IDriveInfo> { notReady, sibling, root },
                "/mnt/data/tenant-1",
                StringComparison.Ordinal,
                '/');

            Assert.Same(root, selected);
        }

        private static IDriveInfo CreateDrive(string rootPath, bool isReady)
        {
            var rootDirectory = new Mock<IDirectoryInfo>();
            rootDirectory.SetupGet(directory => directory.FullName).Returns(rootPath);
            var drive = new Mock<IDriveInfo>();
            drive.SetupGet(info => info.IsReady).Returns(isReady);
            drive.SetupGet(info => info.RootDirectory).Returns(rootDirectory.Object);
            return drive.Object;
        }
    }
}
