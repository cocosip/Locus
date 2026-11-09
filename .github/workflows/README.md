# GitHub Actions CI/CD 配置说明

## 概述

本项目使用 GitHub Actions 自动化构建、测试和发布流程。整个流水线只编译一次：

```
restore → build（一次） → test（--no-build） → pack（复用同一份输出，Tag 时） → publish（只推送产物，Tag 时）
```

## 触发范围

| 事件 | 分支 / 引用 | 执行内容 |
| --- | --- | --- |
| `push` | `master` | 编译 + 单元测试 |
| `pull_request` | 目标分支 `master` | 编译 + 单元测试 |
| `push` | Tag `v*.*.*` | 编译 + 单元测试 + 打包 + 发布 NuGet |
| `workflow_dispatch` | 手动 | 编译 + 单元测试（非 Tag，不发布） |

`dev` 及其他分支**不触发任何任务**；`dev` 上的改动通过 PR 合并到 `master` 后才有 CI 反馈。

## Workflow 说明

### CI/CD Pipeline (`ci-cd.yml`)

该 workflow 包含两个 job：

#### 1. build-and-test (构建和测试)

**执行步骤**:
1. 检出代码（`fetch-depth: 0`，便于生成本地版本信息）
2. 设置 .NET 8.0 / 9.0 / 10.0 环境
3. 恢复 NuGet 依赖（带包缓存）
4. 解析版本号（Tag → 去 `v` 的版本；其他 → `0.0.0-dev.<时间戳>`，且标记为非发布）
5. 编译 `Locus.slnx`（Release，**全流程唯一一次编译**）
6. 运行 `net8.0` / `net9.0` / `net10.0` 全部测试（`--no-build`，直接复用上一步产物），收集覆盖率
7. 上传测试结果与覆盖率报告
8. 仅 Tag：打包 `Locus` NuGet 包，**一次 pack 同时产出 `.nupkg` 与 `.snupkg`**（`common.props` 已启用 `IncludeSymbols` / `SymbolPackageFormat=snupkg`）
9. 仅 Tag：校验包内四个 TFM 的依赖组与 `Locus.dll`、确认符号包存在、确认包内不含测试/覆盖率产物
10. 仅 Tag：上传 `nuget-packages` 产物（`.nupkg` + `.snupkg`）

> `dotnet pack` 使用增量编译（约数秒且不会重新编译）。在存在 `ProjectReference` 的情况下，SDK 会因 `NETSDK1085` 拒绝 `--no-build`，因此这里保留增量行为，而不是再建一个编译 job。

#### 2. publish (发布)

**触发条件**:
- 仅在 Tag 推送且 `build-and-test` 成功后触发
- 使用 job 级 `permissions`：`contents: write`（创建 Release）、`id-token: write`（NuGet Trusted Publishing）

**执行步骤**:
1. 检出代码（仅用于生成 Changelog）
2. 下载 `nuget-packages` 产物（**不 restore、不编译**）
3. 通过 NuGet Trusted Publishing (OIDC) 获取短期 API Key
4. 推送 `.nupkg` 到 NuGet.org（同目录的 `.snupkg` 会自动一起推送）
5. 生成 Changelog（从上一个 Tag 到当前 Tag 的提交记录）
6. 创建 GitHub Release，附上 `.nupkg` 与 `.snupkg`

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
5. 无需在 GitHub 配置 `NUGET_API_KEY` secret。

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
2. 添加规则到 `master` 分支
3. 启用:
   - Require status checks to pass before merging
   - Require branches to be up to date before merging
   - 选择 "Build and Test" 作为必需的检查

## 发布新版本

### 步骤：

1. **确保代码在 `master` 分支且所有测试通过**

2. **创建并推送 tag**:
   ```bash
   # 创建 tag (使用语义化版本)
   git tag v1.0.0

   # 推送 tag 到远程
   git push origin v1.0.0
   ```

3. **自动化流程**:
   - GitHub Actions 检测到 tag 推送
   - 编译 + 运行测试（Tag 运行同样是发布门槛）
   - 打包 `.nupkg` 与 `.snupkg`
   - 发布到 NuGet.org
   - 创建 GitHub Release，包含 Changelog

4. **验证发布**:
   - 检查 Actions 标签页查看 workflow 执行状态
   - 查看 Releases 标签页确认 Release 已创建
   - 访问 NuGet.org 确认包与符号包已发布

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
- 修复后推送到 `master`（或 PR 到 `master`），CI 会自动重新运行

### 打包失败
- 确保所有项目文件包含正确的 PackageId 和 Description
- 检查 tag 格式是否正确 (v1.0.0)
- 查看 Actions 日志获取详细错误信息

### 发布到 NuGet 失败
- 确认 nuget.org 的 Trusted Publishing 策略为 `cocosip` / `Locus` / `ci-cd.yml`
- 确认 publish job 有 `id-token: write` 权限
- 确认 `NuGet/login@v1` 的 `user` 是 nuget.org profile 名（不是邮箱）
- 如果包版本已存在，`--skip-duplicate` 会跳过；需要新版本时更新 tag
- 查看 Actions 日志中 `NuGet login` 步骤的输出

### GitHub Release 创建失败
- 确认 publish job 有 `contents: write` 权限
- 检查 tag 格式是否正确
- 查看是否有同名 Release 已存在

## 最佳实践

1. **在 `master` 分支保持稳定**
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
