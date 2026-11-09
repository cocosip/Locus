# GitHub Actions CI/CD 配置说明

## 概述

本项目使用 GitHub Actions 自动化构建、测试和发布流程。

## Workflow 说明

### CI/CD Pipeline (`ci-cd.yml`)

该 workflow 包含三个主要 job：

#### 1. build-and-test (构建和测试)

**触发条件**:
- Push 到 `main`、`master` 或 `dev` 分支
- Pull Request 到 `main`、`master` 或 `dev` 分支
- Tag 推送 (格式: `v*.*.*`)

**执行步骤**:
1. 检出代码
2. 设置 .NET 8.0 / 9.0 / 10.0 环境
3. 恢复 NuGet 依赖
4. 构建解决方案
5. 运行 `net8.0` / `net9.0` / `net10.0` 全部测试
6. 上传测试结果

#### 2. pack-nuget (打包和发布)

**触发条件**:
- 仅在 tag 推送时触发 (格式: `v*.*.*`)
- 依赖于 build-and-test job 成功完成

**执行步骤**:
1. 从 tag 提取版本号
2. 构建解决方案
3. 打包 Locus NuGet 包 (包含 `netstandard2.0`、`net8.0`、`net9.0`、`net10.0` 四套资产与依赖组):
   - Locus.Core
   - Locus.FileSystem
   - Locus.Storage
   - Locus.MultiTenant
4. 校验包内四个 TFM 的依赖组和 `Locus.dll`
5. 上传 NuGet 包到 Artifacts
6. 通过 NuGet Trusted Publishing (OIDC) 发布到 NuGet.org
7. 生成 Changelog (从上一个 tag 到当前 tag 的提交记录)
8. 创建 GitHub Release，包含:
   - 版本信息
   - NuGet 包信息
   - 完整的 Changelog
   - NuGet 包文件附件

#### 3. pack-debug-symbols (符号包)

**触发条件**:
- 仅在 tag 推送且 `pack-nuget` 成功后触发

**执行步骤**:
1. 使用 .NET 8/9/10 构建 `Locus.slnx`
2. 生成并上传 `.snupkg` 符号包

## 配置步骤

### 1. 配置 NuGet Trusted Publishing

发布使用 NuGet 的 Trusted Publishing，不再需要长期 API Key。先到 nuget.org 配置信任策略：

1. 登录 [NuGet.org](https://www.nuget.org/)
2. 点击用户名 → **Trusted Publishing** → **Add new policy**
3. 填写：
   - **Repository owner**: `cocosip`
   - **Repository**: `Locus`
   - **Workflow file**: `ci-cd.yml`（只填文件名，不带 `.github/workflows/`）
   - **Environment**: 留空（当前 workflow 未使用 GitHub Environment）
4. 工作流中的 `NuGet/login@v1` 使用 `user: virtual`（nuget.org 的 profile 名，不是邮箱）。如果账号的 profile 名不同，修改 [ci-cd.yml](ci-cd.yml) 中的 `user`。
5. 无需在 GitHub 配置 `NUGET_API_KEY` secret；发布 job 已声明 `id-token: write`。

参考：[NuGet Trusted Publishing - GitHub Actions](https://learn.microsoft.com/zh-cn/nuget/nuget-org/trusted-publishing#github-actions)

### 2. 更新项目元数据

编辑根目录的 `common.props` 文件，更新以下信息：

```xml
<Authors>Your Name or Organization</Authors>
<Company>Your Company</Company>
<PackageProjectUrl>https://github.com/yourusername/Locus</PackageProjectUrl>
<RepositoryUrl>https://github.com/yourusername/Locus</RepositoryUrl>
```

### 3. 配置分支保护 (可选但推荐)

在 GitHub 仓库设置中配置分支保护规则：

1. 进入: Settings → Branches
2. 添加规则到 `main` 或 `master` 分支
3. 启用:
   - Require status checks to pass before merging
   - Require branches to be up to date before merging
   - 选择 "build-and-test" 作为必需的检查

## 发布新版本

### 步骤：

1. **确保代码在 main/master 分支且所有测试通过**

2. **创建并推送 tag**:
   ```bash
   # 创建 tag (使用语义化版本)
   git tag v1.0.0

   # 推送 tag 到远程
   git push origin v1.0.0
   ```

3. **自动化流程**:
   - GitHub Actions 自动检测到 tag 推送
   - 运行构建和测试
   - 打包 NuGet 包
   - 发布到 NuGet.org
   - 创建 GitHub Release，包含 Changelog

4. **验证发布**:
   - 检查 Actions 标签页查看 workflow 执行状态
   - 查看 Releases 标签页确认 Release 已创建
   - 访问 NuGet.org 确认包已发布

## 版本号规范

使用语义化版本 (Semantic Versioning):

- **MAJOR** (主版本): 不兼容的 API 更改
- **MINOR** (次版本): 向后兼容的功能新增
- **PATCH** (修订版本): 向后兼容的问题修复

示例:
- `v1.0.0` - 首次正式发布
- `v1.1.0` - 添加新功能
- `v1.1.1` - 修复 bug
- `v2.0.0` - 重大更改，不向后兼容

## Changelog 生成

Changelog 自动从 Git 提交历史生成，包含：

- 从上一个 tag 到当前 tag 的所有提交
- 提交格式: `- 提交信息 (commit hash)`
- 按时间顺序排列

**建议的提交信息格式**:
```
type: subject

例如:
feat: Add directory quota management
fix: Resolve concurrent file allocation issue
docs: Update README with usage examples
refactor: Simplify StoragePool initialization
test: Add integration tests for FileScheduler
```

## 故障排查

### 测试失败
- 检查 Actions 日志查看失败的测试
- 在本地运行 `dotnet test Locus.slnx` 重现问题
- 修复后推送到分支，CI 会自动重新运行

### 打包失败
- 确保所有项目文件包含正确的 PackageId 和 Description
- 检查版本号格式是否正确 (v1.0.0)
- 查看 Actions 日志获取详细错误信息

### 发布到 NuGet 失败
- 确认 nuget.org 的 Trusted Publishing 策略为 `cocosip` / `Locus` / `ci-cd.yml`
- 确认发布 job 有 `id-token: write` 权限
- 确认 `NuGet/login@v1` 的 `user` 是 nuget.org profile 名（不是邮箱）
- 如果包版本已存在，`--skip-duplicate` 会跳过；需要新版本时更新 tag
- 查看 Actions 日志中 `NuGet login` 步骤的输出

### GitHub Release 创建失败
- 确认 GITHUB_TOKEN 有足够权限 (默认已配置)
- 检查 tag 格式是否正确
- 查看是否有同名 Release 已存在

## 最佳实践

1. **在 main/master 分支保持稳定**
   - 所有更改通过 Pull Request 合并
   - 确保 CI 通过后才合并

2. **合理的提交信息**
   - 清晰描述更改内容
   - 使用约定的格式 (feat/fix/docs/etc)

3. **语义化版本**
   - 严格遵循语义化版本规范
   - 记录 BREAKING CHANGES

4. **发布前检查**
   - 确保所有测试通过
   - 更新 CHANGELOG.md (如果手动维护)
   - 检查文档是否同步

## 相关链接

- [GitHub Actions 文档](https://docs.github.com/en/actions)
- [NuGet 打包文档](https://docs.microsoft.com/en-us/nuget/create-packages/)
- [语义化版本规范](https://semver.org/)
