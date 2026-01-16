using System;
using Locus.FileSystem;
using Xunit;

namespace Locus.FileSystem.Tests
{
    public class FileSystemPathSanitizerTests
    {
        [Fact]
        public void IsPathSafe_ReturnsTrueForSafePath()
        {
            // Arrange
            var basePath = "/base/path";
            var relativePath = "subdir/file.txt";

            // Act
            var result = FileSystemPathSanitizer.IsPathSafe(basePath, relativePath);

            // Assert
            Assert.True(result);
        }

        [Theory]
        [InlineData("/base/path", "../escape.txt")]
        [InlineData("/base/path", "../../escape.txt")]
        [InlineData("/base/path", "subdir/../../escape.txt")]
        public void IsPathSafe_ReturnsFalseForTraversalAttempts(string basePath, string relativePath)
        {
            // Act
            var result = FileSystemPathSanitizer.IsPathSafe(basePath, relativePath);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void IsPathSafe_ThrowsWhenBasePathIsEmpty()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                FileSystemPathSanitizer.IsPathSafe("", "file.txt"));
        }

        [Fact]
        public void IsPathSafe_ThrowsWhenRelativePathIsEmpty()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                FileSystemPathSanitizer.IsPathSafe("/base/path", ""));
        }

        [Fact]
        public void SanitizeFileName_RemovesInvalidCharacters()
        {
            // Arrange
            var fileName = "test<>file|name?.txt";

            // Act
            var result = FileSystemPathSanitizer.SanitizeFileName(fileName);

            // Assert
            Assert.Equal("testfilename.txt", result);
        }

        [Fact]
        public void SanitizeFileName_RemovesPathSeparators()
        {
            // Arrange
            var fileName = "test/file\\name.txt";

            // Act
            var result = FileSystemPathSanitizer.SanitizeFileName(fileName);

            // Assert
            Assert.Equal("testfilename.txt", result);
        }

        [Fact]
        public void SanitizeFileName_TrimsDotsAndSpaces()
        {
            // Arrange
            var fileName = "  ..test-file..  ";

            // Act
            var result = FileSystemPathSanitizer.SanitizeFileName(fileName);

            // Assert
            Assert.Equal("test-file", result);
        }

        [Fact]
        public void SanitizeFileName_ThrowsWhenEmpty()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                FileSystemPathSanitizer.SanitizeFileName(""));
        }

        [Fact]
        public void SanitizeFileName_ThrowsWhenOnlyInvalidCharacters()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                FileSystemPathSanitizer.SanitizeFileName("<>|?*"));
        }

        [Fact]
        public void SanitizeRelativePath_RemovesTraversalSegments()
        {
            // Arrange
            var relativePath = "subdir/../file.txt";

            // Act
            var result = FileSystemPathSanitizer.SanitizeRelativePath(relativePath);

            // Assert
            Assert.Equal("subdir/file.txt", result);
        }

        [Fact]
        public void SanitizeRelativePath_NormalizesPathSeparators()
        {
            // Arrange
            var relativePath = "subdir\\file.txt";

            // Act
            var result = FileSystemPathSanitizer.SanitizeRelativePath(relativePath);

            // Assert
            Assert.Equal("subdir/file.txt", result);
        }

        [Fact]
        public void SanitizeRelativePath_RemovesDotSegments()
        {
            // Arrange
            var relativePath = "./subdir/./file.txt";

            // Act
            var result = FileSystemPathSanitizer.SanitizeRelativePath(relativePath);

            // Assert
            Assert.Equal("subdir/file.txt", result);
        }

        [Fact]
        public void SanitizeRelativePath_ThrowsWhenEmpty()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                FileSystemPathSanitizer.SanitizeRelativePath(""));
        }

        [Fact]
        public void CombinePathSafely_CombinesPathsSuccessfully()
        {
            // Arrange
            var basePath = "/base/path";
            var relativePath = "subdir/file.txt";

            // Act
            var result = FileSystemPathSanitizer.CombinePathSafely(basePath, relativePath);

            // Assert
            Assert.Contains("base", result);
            Assert.Contains("path", result);
            Assert.Contains("subdir", result);
            Assert.Contains("file.txt", result);
        }

        [Theory]
        [InlineData("/base/path", "../escape.txt")]
        [InlineData("/base/path", "../../escape.txt")]
        public void CombinePathSafely_ThrowsForTraversalAttempts(string basePath, string relativePath)
        {
            // Act & Assert
            Assert.Throws<InvalidOperationException>(() =>
                FileSystemPathSanitizer.CombinePathSafely(basePath, relativePath));
        }

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

        [Fact]
        public void GenerateSafeFilePath_GeneratesValidPath()
        {
            // Arrange
            var basePath = "/base/path";
            var fileKey = "file-001";

            // Act
            var result = FileSystemPathSanitizer.GenerateSafeFilePath(basePath, fileKey);

            // Assert
            Assert.Contains("base", result);
            Assert.Contains("path", result);
            Assert.Contains("file-001", result);
        }

        [Fact]
        public void GenerateSafeFilePath_SanitizesFileKey()
        {
            // Arrange
            var basePath = "/base/path";
            var fileKey = "file<>|001.txt";

            // Act
            var result = FileSystemPathSanitizer.GenerateSafeFilePath(basePath, fileKey);

            // Assert
            Assert.Contains("file001.txt", result);
            Assert.DoesNotContain("<", result);
            Assert.DoesNotContain(">", result);
            Assert.DoesNotContain("|", result);
        }

        [Fact]
        public void GenerateSafeFilePath_ThrowsWhenBasePathIsEmpty()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                FileSystemPathSanitizer.GenerateSafeFilePath("", "file-001"));
        }

        [Fact]
        public void GenerateSafeFilePath_ThrowsWhenFileKeyIsEmpty()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                FileSystemPathSanitizer.GenerateSafeFilePath("/base/path", ""));
        }
    }
}
