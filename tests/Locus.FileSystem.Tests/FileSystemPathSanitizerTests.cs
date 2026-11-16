using System;
using Xunit;

namespace Locus.FileSystem.Tests;

public class FileSystemPathSanitizerTests
{
    [Fact]
    public void IsPathWithinBase_ReturnsTrueForPathWithinBase()
    {
        // Arrange
        var basePath = "/base/path";
        var fullPath = "/base/path/subdir/file.txt";

        // Act
        var result = FileSystemPathSanitizer.IsPathWithinBase(basePath, fullPath);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void IsPathWithinBase_ReturnsFalseForPathOutsideBase()
    {
        // Arrange
        var basePath = "/base/path";
        var fullPath = "/outside/file.txt";

        // Act
        var result = FileSystemPathSanitizer.IsPathWithinBase(basePath, fullPath);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void IsPathWithinBase_ThrowsWhenBasePathIsEmpty()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            FileSystemPathSanitizer.IsPathWithinBase("", "/some/path"));
    }

    [Fact]
    public void IsPathWithinBase_ThrowsWhenFullPathIsEmpty()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() =>
            FileSystemPathSanitizer.IsPathWithinBase("/base/path", ""));
    }
}
