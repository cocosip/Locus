# CI/CD 快速配置指南

本文档帮助你快速配置 Locus 项目的 CI/CD 流程。

## 前置条件

- GitHub 账号和仓库
- NuGet.org 账号 (用于发布包)
- Git 已安装并配置

## 配置步骤

### 1. 更新项目元数据

编辑 `src/Directory.Build.props` 文件，替换以下占位符：

```xml
<Authors>Your Name or Organization</Authors>          <!-- 替换为你的名字或组织名 -->
<Company>Your Company</Company>                        <!-- 替换为你的公司名 -->
<PackageProjectUrl>https://github.com/yourusername/Locus</PackageProjectUrl>  <!-- 替换为你的仓库 URL -->
<RepositoryUrl>https://github.com/yourusername/Locus</RepositoryUrl>          <!-- 替换为你的仓库 URL -->
```

### 2. 更新 README 徽章

编辑 `README.md` 文件顶部的徽章 URL：

```markdown
[![CI/CD](https://github.com/yourusername/Locus/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/yourusername/Locus/actions/workflows/ci-cd.yml)
```

将 `yourusername` 替换为你的 GitHub 用户名或组织名。

### 3. 配置 NuGet API Key

#### 3.1 获取 NuGet API Key

1. 访问 https://www.nuget.org/
2. 登录你的账号
3. 点击右上角用户名 → **API Keys**
4. 点击 **Create**
5. 配置:
   - Key Name: `Locus GitHub Actions`
   - Select Scopes: 选择 **Push new packages and package versions**
   - Select Packages: 选择 **Glob Pattern**，输入 `Locus.*`
   - Expiration: 设置合理的过期时间 (建议 1 年)
6. 点击 **Create**
7. **立即复制生成的 API Key** (之后无法再查看)

#### 3.2 添加 GitHub Secret

1. 打开你的 GitHub 仓库
2. 进入 **Settings** → **Secrets and variables** → **Actions**
3. 点击 **New repository secret**
4. 配置:
   - Name: `NUGET_API_KEY`
   - Secret: 粘贴你复制的 NuGet API Key
5. 点击 **Add secret**

### 4. 推送代码到 GitHub

```bash
# 添加所有文件
git add .

# 提交更改
git commit -m "feat: Add CI/CD configuration"

# 推送到 GitHub (假设远程名为 origin)
git push origin main
# 或者
git push origin master
```

### 5. 验证 CI 构建

1. 打开 GitHub 仓库
2. 点击 **Actions** 标签
3. 你应该看到 "CI/CD Pipeline" workflow 正在运行
4. 点击查看详细日志
5. 确保 "build-and-test" job 成功完成

## 发布第一个版本

### 步骤 1: 确保代码稳定

```bash
# 本地运行测试
dotnet test

# 确保所有测试通过
```

### 步骤 2: 创建版本标签

```bash
# 创建 v1.0.0 标签
git tag v1.0.0

# 推送标签到远程
git push origin v1.0.0
```

### 步骤 3: 监控发布流程

1. 打开 **Actions** 标签
2. 你会看到两个 job:
   - **build-and-test**: 构建和测试
   - **pack-and-publish**: 打包和发布 (仅在 tag 推送时)
3. 等待两个 job 都完成 (大约 5-10 分钟)

### 步骤 4: 验证发布结果

#### 检查 GitHub Release
1. 打开 **Releases** 标签
2. 你应该看到 "Release v1.0.0"
3. 包含:
   - 完整的 Changelog
   - 所有 NuGet 包文件

#### 检查 NuGet 包
1. 访问 https://www.nuget.org/profiles/[你的用户名]
2. 确认 **Locus** 包已发布 (包含所有依赖组件)

## 后续版本发布

### 选择版本号

使用语义化版本 (Semantic Versioning):

- **Patch (修订)**: `v1.0.1`, `v1.0.2`, etc.
  - Bug 修复
  - 性能改进
  - 文档更新

- **Minor (次版本)**: `v1.1.0`, `v1.2.0`, etc.
  - 新功能添加
  - 向后兼容的 API 更改

- **Major (主版本)**: `v2.0.0`, `v3.0.0`, etc.
  - 破坏性更改
  - 不向后兼容的 API 更改

### 发布流程

```bash
# 1. 确保在最新的 main/master 分支
git checkout main
git pull

# 2. 确保所有测试通过
dotnet test

# 3. 创建并推送新标签
git tag v1.1.0
git push origin v1.1.0

# 4. 等待 CI/CD 自动完成
```

## 常见问题

### Q: CI 构建失败了怎么办?

**A**: 检查错误日志:
1. 进入 Actions 标签
2. 点击失败的 workflow
3. 展开失败的步骤查看详细日志
4. 根据错误信息修复问题
5. 推送修复代码，CI 会自动重新运行

### Q: NuGet 发布失败?

**A**: 检查以下几点:
- NUGET_API_KEY secret 是否正确配置
- API Key 是否有 Push 权限
- 包名是否与现有包冲突
- 版本号是否已存在

### Q: 如何跳过 NuGet 发布只创建 Release?

**A**: 两种方法:
1. 删除 NUGET_API_KEY secret (workflow 会跳过发布步骤)
2. 修改 workflow 文件，注释掉发布步骤

### Q: 如何修改已发布的版本?

**A**: NuGet 包一旦发布无法修改，只能:
1. 取消列出 (unlist) 旧版本
2. 发布新版本 (增加版本号)

### Q: Changelog 不准确怎么办?

**A**: Changelog 从 Git 提交历史自动生成:
1. 确保提交信息清晰明确
2. 使用约定的格式 (feat:, fix:, docs:, etc.)
3. 如需自定义，可编辑 Release 描述

## 高级配置

### 配置分支保护规则

保护 main/master 分支，防止直接推送:

1. 进入 **Settings** → **Branches**
2. 点击 **Add rule**
3. 配置:
   - Branch name pattern: `main` (或 `master`)
   - ✅ Require status checks to pass before merging
   - ✅ Require branches to be up to date before merging
   - 选择 **build-and-test** 作为必需检查
   - ✅ Require pull request reviews before merging (推荐)
4. 点击 **Create**

### 自动生成更详细的 Changelog

可以使用第三方工具如 `conventional-changelog` 或 `release-drafter`:

```yaml
# .github/workflows/release-drafter.yml
name: Release Drafter

on:
  push:
    branches:
      - main
      - master

jobs:
  update_release_draft:
    runs-on: ubuntu-latest
    steps:
      - uses: release-drafter/release-drafter@v5
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

### 添加代码覆盖率报告

在 workflow 中添加:

```yaml
- name: Generate coverage report
  run: dotnet test --collect:"XPlat Code Coverage"

- name: Upload coverage to Codecov
  uses: codecov/codecov-action@v3
```

## 总结

完成以上配置后，你的 CI/CD 流程已经就绪:

✅ 每次推送到 main/master 都会自动构建和测试
✅ 每个 Pull Request 都会自动运行测试
✅ 推送 tag 会自动打包并发布到 NuGet
✅ 自动创建 GitHub Release 包含完整 Changelog

开始享受自动化带来的便利吧！🚀
